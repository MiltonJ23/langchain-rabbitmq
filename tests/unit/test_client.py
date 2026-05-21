"""Unit tests for RabbitMQClient (sync pika wrapper)."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from langchain_rabbitmq.exceptions import (
    RabbitMQChannelError,
    RabbitMQConnectionError,
    RabbitMQMessageError,
    RabbitMQValidationError,
)
from langchain_rabbitmq.utilities._models import (
    ExchangeType,
    HealthStatus,
)
from langchain_rabbitmq.utilities.rabbitmq import (
    RabbitMQClient,
    _validate_amqp_name,
    _validate_exchange_name,
)

pytestmark = pytest.mark.unit


if TYPE_CHECKING:
    from langchain_rabbitmq.config import RabbitMQSettings

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_declare_ok(name: str = "q", msg: int = 0, consumers: int = 0) -> MagicMock:
    m = MagicMock()
    m.method.queue = name
    m.method.message_count = msg
    m.method.consumer_count = consumers
    return m


def _make_delete_ok(msg: int = 0) -> MagicMock:
    m = MagicMock()
    m.method.message_count = msg
    return m


def _make_purge_ok(msg: int = 3) -> MagicMock:
    m = MagicMock()
    m.method.message_count = msg
    return m


def _basic_get_frames(
    body: bytes = b"data",
    tag: int = 1,
    exchange: str = "",
    routing_key: str = "rk",
    redelivered: bool = False,
) -> tuple[MagicMock, MagicMock, bytes]:
    method = MagicMock()
    method.delivery_tag = tag
    method.exchange = exchange
    method.routing_key = routing_key
    method.redelivered = redelivered
    props = MagicMock()
    props.headers = {}
    props.content_type = "text/plain"
    props.content_encoding = "utf-8"
    return method, props, body


# ---------------------------------------------------------------------------
# Validation helpers (pure functions — no broker needed)
# ---------------------------------------------------------------------------


class TestValidateAmqpName:
    def test_valid_name(self) -> None:
        _validate_amqp_name("orders", "queue")  # should not raise

    def test_empty_name_allowed(self) -> None:
        _validate_amqp_name("", "queue")  # empty = server-named, allowed by pika

    def test_too_long_raises(self) -> None:
        with pytest.raises(RabbitMQValidationError, match="255"):
            _validate_amqp_name("a" * 256, "queue")

    def test_control_char_raises(self) -> None:
        with pytest.raises(RabbitMQValidationError, match="control character"):
            _validate_amqp_name("bad\x00name", "queue")

    def test_del_char_raises(self) -> None:
        with pytest.raises(RabbitMQValidationError, match="control character"):
            _validate_amqp_name("name\x7f", "queue")

    def test_255_bytes_valid(self) -> None:
        _validate_amqp_name("a" * 255, "queue")  # exact boundary — valid


class TestValidateExchangeName:
    def test_empty_is_default_exchange(self) -> None:
        _validate_exchange_name("")  # always valid

    def test_reserved_amq_prefix_raises(self) -> None:
        with pytest.raises(RabbitMQValidationError, match="broker-reserved"):
            _validate_exchange_name("amq.direct")

    def test_user_exchange_valid(self) -> None:
        _validate_exchange_name("my-exchange")


# ---------------------------------------------------------------------------
# Connection lifecycle
# ---------------------------------------------------------------------------


class TestConnectClose:
    def test_connect_opens_connection_and_channel(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            return_value=mock_connection,
        ):
            client = RabbitMQClient(settings)
            client.connect()
            assert client._connection is mock_connection
            assert client._channel is mock_channel

    def test_close_idempotent(self, settings: RabbitMQSettings) -> None:
        client = RabbitMQClient(settings)
        client.close()  # no connection open — should not raise
        client.close()  # second call — idempotent

    def test_context_manager_calls_close(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
    ) -> None:
        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            return_value=mock_connection,
        ):
            with RabbitMQClient(settings) as client:
                assert client._connection is not None
            assert client._connection is None

    def test_connect_wraps_amqp_error(
        self,
        settings: RabbitMQSettings,
    ) -> None:
        import pika.exceptions

        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            side_effect=pika.exceptions.AMQPConnectionError("refused"),
        ):
            client = RabbitMQClient(settings)
            with pytest.raises(RabbitMQConnectionError, match="Cannot connect"):
                client.connect()


# ---------------------------------------------------------------------------
# Queue management
# ---------------------------------------------------------------------------


class TestDeclareQueue:
    def test_returns_queue_info(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.queue_declare.return_value = _make_declare_ok("orders", 5, 1)
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            info = client.declare_queue("orders", durable=True)
        assert info.name == "orders"
        assert info.message_count == 5
        assert info.consumer_count == 1
        assert info.durable is True

    def test_invalid_name_raises_validation_error(self, settings: RabbitMQSettings) -> None:
        client = RabbitMQClient(settings)
        with pytest.raises(RabbitMQValidationError):
            client.declare_queue("bad\x00name")

    def test_broker_refusal_raises_channel_error(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        import pika.exceptions

        mock_channel.queue_declare.side_effect = pika.exceptions.ChannelClosedByBroker(
            405, "resource locked"
        )
        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            return_value=mock_connection,
        ):
            client = RabbitMQClient(settings)
            client.connect()
            with pytest.raises(RabbitMQChannelError, match=r"queue\.declare"):
                client.declare_queue("orders")
            # channel reference should be cleared
            assert client._channel is None


class TestDeleteQueue:
    def test_returns_message_count(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.queue_delete.return_value = _make_delete_ok(7)
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            count = client.delete_queue("orders")
        assert count == 7


class TestPurgeQueue:
    def test_returns_purged_count(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.queue_purge.return_value = _make_purge_ok(12)
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            count = client.purge_queue("orders")
        assert count == 12


class TestBindQueue:
    def test_bind_calls_queue_bind(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.bind_queue("orders", "events", routing_key="order.*")
        mock_channel.queue_bind.assert_called_once()

    def test_invalid_exchange_raises(self, settings: RabbitMQSettings) -> None:
        client = RabbitMQClient(settings)
        with pytest.raises(RabbitMQValidationError, match="broker-reserved"):
            client.bind_queue("orders", "amq.direct")


class TestUnbindQueue:
    def test_unbind_calls_queue_unbind(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.unbind_queue("orders", "events")
        mock_channel.queue_unbind.assert_called_once()


# ---------------------------------------------------------------------------
# Exchange management
# ---------------------------------------------------------------------------


class TestDeclareExchange:
    def test_returns_exchange_info(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            info = client.declare_exchange("events", ExchangeType.TOPIC, durable=True)
        assert info.name == "events"
        assert info.exchange_type == ExchangeType.TOPIC
        assert info.durable is True

    def test_reserved_name_raises(self, settings: RabbitMQSettings) -> None:
        client = RabbitMQClient(settings)
        with pytest.raises(RabbitMQValidationError, match="broker-reserved"):
            client.declare_exchange("amq.direct")


class TestDeleteExchange:
    def test_calls_exchange_delete(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.delete_exchange("events")
        mock_channel.exchange_delete.assert_called_once()


class TestBindExchange:
    def test_returns_binding_info(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            info = client.bind_exchange("dest-ex", "src-ex", routing_key="rk")
        assert info.source == "src-ex"
        assert info.destination == "dest-ex"
        assert info.destination_type == "exchange"
        assert info.routing_key == "rk"


# ---------------------------------------------------------------------------
# Message operations
# ---------------------------------------------------------------------------


class TestPublishMessage:
    def test_calls_basic_publish(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.publish_message("", "orders", b"hello")
        mock_channel.basic_publish.assert_called_once()

    def test_reserved_exchange_raises(self, settings: RabbitMQSettings) -> None:
        client = RabbitMQClient(settings)
        with pytest.raises(RabbitMQValidationError):
            client.publish_message("amq.direct", "rk", b"x")

    def test_unroutable_error_raises_message_error(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        import pika.exceptions

        mock_channel.basic_publish.side_effect = pika.exceptions.UnroutableError(messages=[])
        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            return_value=mock_connection,
        ):
            client = RabbitMQClient(settings)
            client.connect()
            with pytest.raises(RabbitMQMessageError, match="unroutable"):
                client.publish_message("", "ghost", b"x", mandatory=True)


class TestConsumeMessage:
    def test_returns_message_result(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.basic_get.return_value = _basic_get_frames(
            body=b'{"id":1}', tag=5, routing_key="orders"
        )
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            msg = client.consume_message("orders")
        assert msg is not None
        assert msg.body == b'{"id":1}'
        assert msg.delivery_tag == 5

    def test_empty_queue_returns_none(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.basic_get.return_value = (None, None, None)
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            msg = client.consume_message("empty")
        assert msg is None


class TestAckNackReject:
    def test_ack_calls_basic_ack(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.ack_message(1)
        mock_channel.basic_ack.assert_called_once_with(delivery_tag=1, multiple=False)

    def test_nack_calls_basic_nack(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.nack_message(2, requeue=False)
        mock_channel.basic_nack.assert_called_once()

    def test_reject_calls_basic_reject(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            client.reject_message(3, requeue=True)
        mock_channel.basic_reject.assert_called_once()

    def test_ack_error_raises_message_error(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.basic_ack.side_effect = RuntimeError("channel closed")
        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            return_value=mock_connection,
        ):
            client = RabbitMQClient(settings)
            client.connect()
            with pytest.raises(RabbitMQMessageError, match="ack"):
                client.ack_message(1)


# ---------------------------------------------------------------------------
# Health and connection info
# ---------------------------------------------------------------------------


class TestCheckHealth:
    def test_ok_when_connected(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            health = client.check_health()
        assert health.status == HealthStatus.OK
        assert health.host == "localhost"

    def test_down_when_broker_unreachable(self, settings: RabbitMQSettings) -> None:
        import pika.exceptions

        with patch(
            "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
            side_effect=pika.exceptions.AMQPConnectionError("refused"),
        ):
            client = RabbitMQClient(settings)
            health = client.check_health()
        assert health.status == HealthStatus.DOWN

    def test_degraded_on_unexpected_error(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
        mock_channel: MagicMock,
    ) -> None:
        mock_channel.queue_declare.side_effect = RuntimeError("unexpected")
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            health = client.check_health()
        assert health.status == HealthStatus.DEGRADED


class TestGetConnectionInfo:
    def test_connected_returns_info(
        self,
        settings: RabbitMQSettings,
        mock_connection: MagicMock,
    ) -> None:
        with (
            patch(
                "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
                return_value=mock_connection,
            ),
            RabbitMQClient(settings) as client,
        ):
            info = client.get_connection_info()
        assert info.connected is True
        assert info.server_version == "3.12.4"
        assert info.server_platform == "Erlang/OTP 26.1"

    def test_not_connected_returns_disconnected_info(self, settings: RabbitMQSettings) -> None:
        client = RabbitMQClient(settings)
        info = client.get_connection_info()
        assert info.connected is False
        assert info.host == "localhost"
