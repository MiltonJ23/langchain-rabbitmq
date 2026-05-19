"""Unit tests for AsyncRabbitMQClient.

Uses ``AsyncMock`` to simulate aio-pika connection/channel objects
without a live broker.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import (
    RabbitMQChannelError,
    RabbitMQConnectionError,
    RabbitMQMessageError,
)
from langchain_rabbitmq.utilities._models import (
    ConnectionInfo,
    ExchangeType,
    HealthStatus,
    QueueInfo,
)
from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient

_MODULE = "langchain_rabbitmq.utilities.async_rabbitmq"


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings() -> RabbitMQSettings:
    return RabbitMQSettings(host="localhost", port=5672, max_retries=1, retry_delay=0.0)


def _make_async_infra() -> tuple[AsyncMock, AsyncMock]:
    """Return (mock_connection, mock_channel) ready for inject."""
    mock_channel = AsyncMock()
    mock_channel.is_closed = False
    mock_channel.set_qos = AsyncMock()

    mock_connection = AsyncMock()
    mock_connection.is_closed = False
    mock_connection.channel = AsyncMock(return_value=mock_channel)
    return mock_connection, mock_channel


def _patch_connect(mock_connection: AsyncMock) -> Any:
    return patch(
        f"{_MODULE}.aio_pika.connect_robust",
        new_callable=AsyncMock,
        return_value=mock_connection,
    )


# ---------------------------------------------------------------------------
# Lifecycle
# ---------------------------------------------------------------------------


class TestAsyncClientLifecycle:
    @pytest.mark.asyncio
    async def test_connect_and_close(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        with _patch_connect(mock_conn):
            client = AsyncRabbitMQClient(settings)
            await client.connect()
            assert client._connection is mock_conn
            await client.close()
            assert client._connection is None
            assert client._channel is None

    @pytest.mark.asyncio
    async def test_context_manager(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                assert client._connection is mock_conn
            assert client._connection is None

    @pytest.mark.asyncio
    async def test_close_idempotent(self, settings: RabbitMQSettings) -> None:
        client = AsyncRabbitMQClient(settings)
        await client.close()  # nothing open — should not raise
        await client.close()

    @pytest.mark.asyncio
    async def test_close_suppresses_channel_error(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.close.side_effect = RuntimeError("channel gone")
        with _patch_connect(mock_conn):
            client = AsyncRabbitMQClient(settings)
            await client.connect()
            await client.close()  # must not raise
        assert client._channel is None

    @pytest.mark.asyncio
    async def test_connect_raises_connection_error_on_amqp(
        self, settings: RabbitMQSettings
    ) -> None:
        import aio_pika.exceptions as aio_exc  # type: ignore[import-untyped]

        with patch(
            f"{_MODULE}.aio_pika.connect_robust",
            new_callable=AsyncMock,
            side_effect=aio_exc.AMQPConnectionError("refused"),
        ):
            client = AsyncRabbitMQClient(settings)
            with pytest.raises(RabbitMQConnectionError, match="Cannot connect"):
                await client.connect()

    @pytest.mark.asyncio
    async def test_connect_raises_connection_error_on_generic(
        self, settings: RabbitMQSettings
    ) -> None:
        with patch(
            f"{_MODULE}.aio_pika.connect_robust",
            new_callable=AsyncMock,
            side_effect=OSError("refused"),
        ):
            client = AsyncRabbitMQClient(settings)
            with pytest.raises(RabbitMQConnectionError, match="Unexpected error"):
                await client.connect()


# ---------------------------------------------------------------------------
# Queue management
# ---------------------------------------------------------------------------


class TestAsyncClientQueueManagement:
    @pytest.mark.asyncio
    async def test_declare_queue(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.name = "orders"
        mock_queue.declaration_result = MagicMock(message_count=3, consumer_count=1)
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                info = await client.declare_queue("orders", durable=True)

        assert isinstance(info, QueueInfo)
        assert info.name == "orders"
        assert info.durable is True

    @pytest.mark.asyncio
    async def test_declare_queue_channel_error(
        self, settings: RabbitMQSettings
    ) -> None:
        import aio_pika.exceptions as aio_exc  # type: ignore[import-untyped]

        mock_conn, mock_ch = _make_async_infra()
        mock_ch.declare_queue = AsyncMock(
            side_effect=aio_exc.ChannelNotFoundEntity("no queue")
        )

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQChannelError, match="does not exist"):
                    await client.declare_queue("missing", passive=True)

    @pytest.mark.asyncio
    async def test_delete_queue(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_result = MagicMock()
        mock_result.message_count = 5
        mock_ch.queue_delete = AsyncMock(return_value=mock_result)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                deleted = await client.delete_queue("orders")

        assert deleted == 5

    @pytest.mark.asyncio
    async def test_delete_queue_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.queue_delete = AsyncMock(side_effect=RuntimeError("fail"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQChannelError, match="Failed to delete"):
                    await client.delete_queue("orders")

    @pytest.mark.asyncio
    async def test_purge_queue(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        purge_result = MagicMock()
        purge_result.message_count = 10
        mock_queue.purge = AsyncMock(return_value=purge_result)
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                count = await client.purge_queue("orders")

        assert count == 10

    @pytest.mark.asyncio
    async def test_purge_queue_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.purge = AsyncMock(side_effect=RuntimeError("fail"))
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQChannelError, match="Failed to purge"):
                    await client.purge_queue("orders")

    @pytest.mark.asyncio
    async def test_bind_queue(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.bind = AsyncMock()
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                await client.bind_queue("orders", "events", "order.created")

        mock_queue.bind.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bind_queue_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.bind = AsyncMock(side_effect=RuntimeError("fail"))
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQChannelError, match="Failed to bind queue"):
                    await client.bind_queue("orders", "events")

    @pytest.mark.asyncio
    async def test_unbind_queue(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.unbind = AsyncMock()
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                await client.unbind_queue("orders", "events", "order.created")

        mock_queue.unbind.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_unbind_queue_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.unbind = AsyncMock(side_effect=RuntimeError("fail"))
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(
                    RabbitMQChannelError, match="Failed to unbind queue"
                ):
                    await client.unbind_queue("orders", "events")

    @pytest.mark.asyncio
    async def test_get_queue_info_delegates_to_declare_passive(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.name = "orders"
        mock_queue.declaration_result = MagicMock(message_count=0, consumer_count=0)
        mock_ch.declare_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                info = await client.get_queue_info("orders")

        assert info.name == "orders"
        call_kwargs = mock_ch.declare_queue.call_args.kwargs
        assert call_kwargs.get("passive") is True


# ---------------------------------------------------------------------------
# Exchange management
# ---------------------------------------------------------------------------


class TestAsyncClientExchangeManagement:
    @pytest.mark.asyncio
    async def test_declare_exchange(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.declare_exchange = AsyncMock()

        with (
            _patch_connect(mock_conn),
            patch(f"{_MODULE}.aio_pika.ExchangeType", side_effect=lambda v: v),
        ):
            async with AsyncRabbitMQClient(settings) as client:
                info = await client.declare_exchange("events", ExchangeType.TOPIC)

        assert info.name == "events"
        assert info.exchange_type == ExchangeType.TOPIC

    @pytest.mark.asyncio
    async def test_declare_exchange_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.declare_exchange = AsyncMock(side_effect=RuntimeError("fail"))

        with (
            _patch_connect(mock_conn),
            patch(f"{_MODULE}.aio_pika.ExchangeType", side_effect=lambda v: v),
        ):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(
                    RabbitMQChannelError, match="Failed to declare exchange"
                ):
                    await client.declare_exchange("events")

    @pytest.mark.asyncio
    async def test_delete_exchange(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.exchange_delete = AsyncMock()

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                await client.delete_exchange("events")

        mock_ch.exchange_delete.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_delete_exchange_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.exchange_delete = AsyncMock(side_effect=RuntimeError("fail"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(
                    RabbitMQChannelError, match="Failed to delete exchange"
                ):
                    await client.delete_exchange("events")

    @pytest.mark.asyncio
    async def test_bind_exchange(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_dest_exc = AsyncMock()
        mock_dest_exc.bind = AsyncMock()
        mock_ch.declare_exchange = AsyncMock(return_value=mock_dest_exc)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                info = await client.bind_exchange(
                    destination="dest", source="src", routing_key="rk"
                )

        assert info.source == "src"
        assert info.destination == "dest"
        mock_dest_exc.bind.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_bind_exchange_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_dest_exc = AsyncMock()
        mock_dest_exc.bind = AsyncMock(side_effect=RuntimeError("fail"))
        mock_ch.declare_exchange = AsyncMock(return_value=mock_dest_exc)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQChannelError, match="Failed to bind"):
                    await client.bind_exchange("dest", "src")


# ---------------------------------------------------------------------------
# Message operations
# ---------------------------------------------------------------------------


class TestAsyncClientMessageOperations:
    @pytest.mark.asyncio
    async def test_publish_to_named_exchange(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_exch = AsyncMock()
        mock_ch.get_exchange = AsyncMock(return_value=mock_exch)

        with (
            _patch_connect(mock_conn),
            patch(f"{_MODULE}.aio_pika.Message", return_value=MagicMock()),
            patch(
                f"{_MODULE}.aio_pika.DeliveryMode",
                NOT_PERSISTENT=2,
                PERSISTENT=2,
                spec=True,
            ),
        ):
            async with AsyncRabbitMQClient(settings) as client:
                await client.publish_message("events", "order.created", b"payload")

        mock_exch.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publish_to_default_exchange(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.default_exchange = AsyncMock()

        with (
            _patch_connect(mock_conn),
            patch(f"{_MODULE}.aio_pika.Message", return_value=MagicMock()),
            patch(
                f"{_MODULE}.aio_pika.DeliveryMode",
                NOT_PERSISTENT=1,
                PERSISTENT=2,
                spec=True,
            ),
        ):
            async with AsyncRabbitMQClient(settings) as client:
                await client.publish_message("", "orders", b"hello")

        mock_ch.default_exchange.publish.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_publish_error_raises_message_error(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.get_exchange = AsyncMock(side_effect=RuntimeError("fail"))

        with (
            _patch_connect(mock_conn),
            patch(f"{_MODULE}.aio_pika.Message", return_value=MagicMock()),
            patch(
                f"{_MODULE}.aio_pika.DeliveryMode",
                NOT_PERSISTENT=1,
                PERSISTENT=2,
                spec=True,
            ),
        ):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQMessageError, match="Publish failed"):
                    await client.publish_message("events", "rk", b"body")

    @pytest.mark.asyncio
    async def test_consume_message_returns_result(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_msg = MagicMock()
        mock_msg.body = b"hello"
        mock_msg.delivery_tag = 1
        mock_msg.exchange = "events"
        mock_msg.routing_key = "order.created"
        mock_msg.redelivered = False
        mock_msg.headers = {}
        mock_msg.content_type = "text/plain"
        mock_msg.content_encoding = None

        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=mock_msg)
        mock_ch.get_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                result = await client.consume_message("orders")

        assert result is not None
        assert result.body == b"hello"
        assert result.delivery_tag == 1

    @pytest.mark.asyncio
    async def test_consume_message_returns_none_when_empty(
        self, settings: RabbitMQSettings
    ) -> None:
        import aio_pika.exceptions as aio_exc  # type: ignore[import-untyped]

        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(side_effect=aio_exc.QueueEmpty)
        mock_ch.get_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                result = await client.consume_message("empty_queue")

        assert result is None

    @pytest.mark.asyncio
    async def test_consume_message_returns_none_when_msg_is_none(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_queue = AsyncMock()
        mock_queue.get = AsyncMock(return_value=None)
        mock_ch.get_queue = AsyncMock(return_value=mock_queue)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                result = await client.consume_message("orders")

        assert result is None

    @pytest.mark.asyncio
    async def test_consume_message_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.get_queue = AsyncMock(side_effect=RuntimeError("fail"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQChannelError, match="Failed to get"):
                    await client.consume_message("orders")

    @pytest.mark.asyncio
    async def test_ack_message(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        mock_msg = AsyncMock()
        mock_msg.ack = AsyncMock()

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                client._pending_messages[42] = mock_msg
                await client.ack_message(42)

        mock_msg.ack.assert_awaited_once_with(multiple=False)

    @pytest.mark.asyncio
    async def test_ack_message_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        mock_msg = AsyncMock()
        mock_msg.ack = AsyncMock(side_effect=RuntimeError("fail"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                client._pending_messages[42] = mock_msg
                with pytest.raises(RabbitMQMessageError, match="Failed to ack"):
                    await client.ack_message(42)

    @pytest.mark.asyncio
    async def test_ack_message_no_pending(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                with pytest.raises(RabbitMQMessageError, match="No pending message"):
                    await client.ack_message(99)

    @pytest.mark.asyncio
    async def test_nack_message(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        mock_msg = AsyncMock()
        mock_msg.nack = AsyncMock()

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                client._pending_messages[42] = mock_msg
                await client.nack_message(42, multiple=True, requeue=False)

        mock_msg.nack.assert_awaited_once_with(multiple=True, requeue=False)

    @pytest.mark.asyncio
    async def test_nack_message_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        mock_msg = AsyncMock()
        mock_msg.nack = AsyncMock(side_effect=RuntimeError("fail"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                client._pending_messages[42] = mock_msg
                with pytest.raises(RabbitMQMessageError, match="Failed to nack"):
                    await client.nack_message(42)

    @pytest.mark.asyncio
    async def test_reject_message(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        mock_msg = AsyncMock()
        mock_msg.reject = AsyncMock()

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                client._pending_messages[42] = mock_msg
                await client.reject_message(42, requeue=False)

        mock_msg.reject.assert_awaited_once_with(requeue=False)

    @pytest.mark.asyncio
    async def test_reject_message_error(self, settings: RabbitMQSettings) -> None:
        mock_conn, _ = _make_async_infra()
        mock_msg = AsyncMock()
        mock_msg.reject = AsyncMock(side_effect=RuntimeError("fail"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                client._pending_messages[42] = mock_msg
                with pytest.raises(RabbitMQMessageError, match="Failed to reject"):
                    await client.reject_message(42)


# ---------------------------------------------------------------------------
# Health & connection info
# ---------------------------------------------------------------------------


class TestAsyncClientHealthAndInfo:
    @pytest.mark.asyncio
    async def test_check_health_ok(self, settings: RabbitMQSettings) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_probe = AsyncMock()
        mock_ch.declare_queue = AsyncMock(return_value=mock_probe)

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                health = await client.check_health()

        assert health.status == HealthStatus.OK

    @pytest.mark.asyncio
    async def test_check_health_down_on_connection_error(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.declare_queue = AsyncMock(
            side_effect=RabbitMQConnectionError("down", cause=None)
        )

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                health = await client.check_health()

        assert health.status == HealthStatus.DOWN

    @pytest.mark.asyncio
    async def test_check_health_degraded_on_generic_error(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, mock_ch = _make_async_infra()
        mock_ch.declare_queue = AsyncMock(side_effect=RuntimeError("partial"))

        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                health = await client.check_health()

        assert health.status == HealthStatus.DEGRADED

    @pytest.mark.asyncio
    async def test_get_connection_info_connected(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, _ = _make_async_infra()
        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                info = await client.get_connection_info()

        assert isinstance(info, ConnectionInfo)
        assert info.connected is True
        assert info.host == "localhost"

    @pytest.mark.asyncio
    async def test_get_connection_info_not_connected(
        self, settings: RabbitMQSettings
    ) -> None:
        client = AsyncRabbitMQClient(settings)
        info = await client.get_connection_info()
        assert info.connected is False

    @pytest.mark.asyncio
    async def test_get_connection_info_closed_connection(
        self, settings: RabbitMQSettings
    ) -> None:
        mock_conn, _ = _make_async_infra()
        mock_conn.is_closed = True
        with _patch_connect(mock_conn):
            async with AsyncRabbitMQClient(settings) as client:
                # Force closed flag after connection
                client._connection.is_closed = True  # type: ignore[union-attr]
                info = await client.get_connection_info()
        assert info.connected is False


# ---------------------------------------------------------------------------
# SSL context
# ---------------------------------------------------------------------------


class TestAsyncClientSSL:
    @pytest.mark.asyncio
    async def test_no_ssl_returns_none_context(
        self, settings: RabbitMQSettings
    ) -> None:
        client = AsyncRabbitMQClient(settings)
        ctx = client._build_ssl_context()
        assert ctx is None

    @pytest.mark.asyncio
    async def test_ssl_enabled_creates_context(self) -> None:
        ssl_settings = RabbitMQSettings(host="localhost", ssl_enabled=True)
        client = AsyncRabbitMQClient(ssl_settings)
        ctx = client._build_ssl_context()
        assert ctx is not None
