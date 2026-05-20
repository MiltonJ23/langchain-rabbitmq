"""Async execution path tests for all LangChain RabbitMQ tools.

Covers the ``_aexecute`` / ``_arun`` code paths and the factory methods
``_make_client`` / ``_make_async_client`` that aren't exercised by the sync
tool tests (which patch the factories).
"""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import (
    RabbitMQChannelError,
    RabbitMQConnectionError,
)
from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.tools.admin import (
    CheckHealthTool,
    CloseConnectionTool,
    GetConnectionInfoTool,
    GetNodeStatsTool,
    ListBindingsTool,
    ListExchangesTool,
    ListQueuesTool,
)
from langchain_rabbitmq.tools.exchange import (
    BindExchangeTool,
    DeclareExchangeTool,
    DeleteExchangeTool,
)
from langchain_rabbitmq.tools.message import (
    AckMessageTool,
    ConsumeMessageTool,
    NackMessageTool,
    PublishMessageTool,
    RejectMessageTool,
)
from langchain_rabbitmq.tools.queue import (
    BindQueueTool,
    DeclareQueueTool,
    DeleteQueueTool,
    GetQueueInfoTool,
    PurgeQueueTool,
    UnbindQueueTool,
)
from langchain_rabbitmq.utilities._models import (
    BindingInfo,
    ConnectionInfo,
    ExchangeInfo,
    ExchangeType,
    HealthInfo,
    HealthStatus,
    MessageResult,
    QueueInfo,
)
from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

pytestmark = pytest.mark.unit


_ADMIN_MODULE = "langchain_rabbitmq.tools.admin"


@pytest.fixture()
def settings() -> RabbitMQSettings:
    return RabbitMQSettings(host="localhost", max_retries=1, retry_delay=0.0)


def _make_async_ctx_mock() -> AsyncMock:
    """Return an AsyncMock that supports the async context-manager protocol."""
    m = AsyncMock()
    m.__aenter__ = AsyncMock(return_value=m)
    m.__aexit__ = AsyncMock(return_value=None)
    return m


def _make_async_mgmt_mock() -> AsyncMock:
    """Patch target for AsyncManagementAPIClient — class mock."""
    m = _make_async_ctx_mock()
    return m


# ---------------------------------------------------------------------------
# _RabbitMQBaseTool factory methods  (lines 80 & 89 in _base.py)
# ---------------------------------------------------------------------------


class TestBaseToolFactories:
    def test_make_client_returns_rabbitmq_client(self, settings: RabbitMQSettings) -> None:
        tool = DeclareQueueTool(settings=settings)
        client = tool._make_client()
        assert isinstance(client, RabbitMQClient)

    def test_make_async_client_returns_async_client(self, settings: RabbitMQSettings) -> None:
        tool = DeclareQueueTool(settings=settings)
        client = tool._make_async_client()
        assert isinstance(client, AsyncRabbitMQClient)


# ---------------------------------------------------------------------------
# _RabbitMQBaseTool._arun error handling  (lines 171-179)
# ---------------------------------------------------------------------------


class TestBaseToolArunErrorHandling:
    @pytest.mark.asyncio
    async def test_arun_converts_tool_exception_to_string(self, settings: RabbitMQSettings) -> None:
        tool = DeclareQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.declare_queue = AsyncMock(
            side_effect=RabbitMQChannelError("channel lost", cause=None)
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="bad_queue")
        assert "[CHANNEL_ERROR]" in result or "channel lost" in result

    @pytest.mark.asyncio
    async def test_arun_converts_unexpected_exception_to_internal_error(
        self, settings: RabbitMQSettings
    ) -> None:
        tool = DeclareQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.declare_queue = AsyncMock(side_effect=RuntimeError("totally unexpected"))
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="some_queue")
        assert "[INTERNAL_ERROR]" in result

    @pytest.mark.asyncio
    async def test_default_aexecute_delegates_to_execute(self, settings: RabbitMQSettings) -> None:
        """The base _aexecute default (line 123) calls _execute synchronously."""

        from pydantic import BaseModel

        class _EmptySchema(BaseModel):
            pass

        class _NaiveTool(_RabbitMQBaseTool):
            name: str = "test_naive"
            description: str = "test"
            args_schema: type[BaseModel] = _EmptySchema  # type: ignore[assignment]

            def _execute(self, **kwargs: object) -> str:  # type: ignore[override]
                return "sync_result"

        tool = _NaiveTool(settings=settings)
        # _aexecute not overridden — should fall through to _execute
        result = await tool._aexecute()
        assert result == "sync_result"


# ---------------------------------------------------------------------------
# Queue tools — async paths
# ---------------------------------------------------------------------------


class TestQueueToolsAsync:
    @pytest.mark.asyncio
    async def test_declare_queue_async(self, settings: RabbitMQSettings) -> None:
        tool = DeclareQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.declare_queue = AsyncMock(
            return_value=QueueInfo(
                name="orders",
                durable=True,
                exclusive=False,
                auto_delete=False,
                message_count=0,
                consumer_count=0,
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="orders", durable=True)
        assert "orders" in result
        assert "declared" in result.lower()

    @pytest.mark.asyncio
    async def test_delete_queue_async(self, settings: RabbitMQSettings) -> None:
        tool = DeleteQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.delete_queue = AsyncMock(return_value=3)
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="orders")
        assert "orders" in result
        assert "3" in result

    @pytest.mark.asyncio
    async def test_purge_queue_async(self, settings: RabbitMQSettings) -> None:
        tool = PurgeQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.purge_queue = AsyncMock(return_value=7)
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="orders")
        assert "7" in result

    @pytest.mark.asyncio
    async def test_bind_queue_async(self, settings: RabbitMQSettings) -> None:
        tool = BindQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.bind_queue = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(
                queue="orders", exchange="events", routing_key="order.created"
            )
        assert "orders" in result

    @pytest.mark.asyncio
    async def test_unbind_queue_async(self, settings: RabbitMQSettings) -> None:
        tool = UnbindQueueTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.unbind_queue = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(
                queue="orders", exchange="events", routing_key="order.created"
            )
        assert "orders" in result

    @pytest.mark.asyncio
    async def test_get_queue_info_async(self, settings: RabbitMQSettings) -> None:
        tool = GetQueueInfoTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.get_queue_info = AsyncMock(
            return_value=QueueInfo(
                name="orders",
                durable=False,
                exclusive=False,
                auto_delete=False,
                message_count=5,
                consumer_count=1,
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="orders")
        assert "orders" in result
        assert "5" in result


# ---------------------------------------------------------------------------
# Exchange tools — async paths
# ---------------------------------------------------------------------------


class TestExchangeToolsAsync:
    @pytest.mark.asyncio
    async def test_declare_exchange_async(self, settings: RabbitMQSettings) -> None:
        tool = DeclareExchangeTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.declare_exchange = AsyncMock(
            return_value=ExchangeInfo(
                name="events",
                exchange_type=ExchangeType.TOPIC,
                durable=False,
                auto_delete=False,
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="events", exchange_type="topic")
        assert "events" in result

    @pytest.mark.asyncio
    async def test_delete_exchange_async(self, settings: RabbitMQSettings) -> None:
        tool = DeleteExchangeTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.delete_exchange = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(name="events")
        assert "events" in result

    @pytest.mark.asyncio
    async def test_bind_exchange_async(self, settings: RabbitMQSettings) -> None:
        tool = BindExchangeTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.bind_exchange = AsyncMock(
            return_value=BindingInfo(
                source="src",
                destination="dst",
                destination_type="exchange",
                routing_key="rk",
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(destination="dst", source="src", routing_key="rk")
        assert "dst" in result


# ---------------------------------------------------------------------------
# Message tools — async paths
# ---------------------------------------------------------------------------


class TestMessageToolsAsync:
    @pytest.mark.asyncio
    async def test_publish_message_async(self, settings: RabbitMQSettings) -> None:
        tool = PublishMessageTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.publish_message = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(
                exchange="events",
                routing_key="order.created",
                body='{"id": 1}',
            )
        assert "published" in result.lower()

    @pytest.mark.asyncio
    async def test_consume_message_async_with_result(self, settings: RabbitMQSettings) -> None:
        tool = ConsumeMessageTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.consume_message = AsyncMock(
            return_value=MessageResult(
                body=b'{"id": 1}',
                delivery_tag=42,
                exchange="events",
                routing_key="order.created",
                redelivered=False,
                headers={},
                content_type="application/json",
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(queue="orders")
        assert "42" in result
        assert "orders" in result

    @pytest.mark.asyncio
    async def test_consume_message_async_empty_queue(self, settings: RabbitMQSettings) -> None:
        tool = ConsumeMessageTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.consume_message = AsyncMock(return_value=None)
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(queue="orders")
        assert "No messages" in result

    @pytest.mark.asyncio
    async def test_ack_message_async(self, settings: RabbitMQSettings) -> None:
        tool = AckMessageTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.ack_message = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(delivery_tag=42)
        assert "42" in result

    @pytest.mark.asyncio
    async def test_nack_message_async(self, settings: RabbitMQSettings) -> None:
        tool = NackMessageTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.nack_message = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(delivery_tag=42, requeue=False)
        assert "42" in result

    @pytest.mark.asyncio
    async def test_reject_message_async(self, settings: RabbitMQSettings) -> None:
        tool = RejectMessageTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.reject_message = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun(delivery_tag=42, requeue=False)
        assert "42" in result


# ---------------------------------------------------------------------------
# Admin tools — async paths (use AsyncManagementAPIClient directly)
# ---------------------------------------------------------------------------


class TestAdminToolsAsync:
    @pytest.mark.asyncio
    async def test_list_queues_async(self, settings: RabbitMQSettings) -> None:
        tool = ListQueuesTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.list_queues = AsyncMock(
            return_value=[{"name": "orders", "messages": 5, "consumers": 1, "durable": True}]
        )
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(vhost="")
        assert "orders" in result

    @pytest.mark.asyncio
    async def test_list_queues_async_empty(self, settings: RabbitMQSettings) -> None:
        tool = ListQueuesTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.list_queues = AsyncMock(return_value=[])
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(vhost="")
        assert "No queues" in result

    @pytest.mark.asyncio
    async def test_list_exchanges_async(self, settings: RabbitMQSettings) -> None:
        tool = ListExchangesTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.list_exchanges = AsyncMock(
            return_value=[{"name": "events", "type": "topic", "durable": True}]
        )
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(vhost="", include_defaults=False)
        assert "events" in result

    @pytest.mark.asyncio
    async def test_list_exchanges_async_empty(self, settings: RabbitMQSettings) -> None:
        tool = ListExchangesTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.list_exchanges = AsyncMock(return_value=[])
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(vhost="", include_defaults=False)
        assert "No user-defined" in result

    @pytest.mark.asyncio
    async def test_list_bindings_async(self, settings: RabbitMQSettings) -> None:
        tool = ListBindingsTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.list_bindings = AsyncMock(
            return_value=[
                {
                    "source": "events",
                    "destination": "orders",
                    "destination_type": "queue",
                    "routing_key": "order.*",
                }
            ]
        )
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(vhost="")
        assert "events" in result

    @pytest.mark.asyncio
    async def test_list_bindings_async_empty(self, settings: RabbitMQSettings) -> None:
        tool = ListBindingsTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.list_bindings = AsyncMock(return_value=[])
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(vhost="")
        assert "No bindings" in result

    @pytest.mark.asyncio
    async def test_get_node_stats_async(self, settings: RabbitMQSettings) -> None:
        tool = GetNodeStatsTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.get_node_stats = AsyncMock(
            return_value=[{"name": "rabbit@node1", "running": True}]
        )
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(fields=None)
        assert "node1" in result

    @pytest.mark.asyncio
    async def test_get_node_stats_async_empty(self, settings: RabbitMQSettings) -> None:
        tool = GetNodeStatsTool(settings=settings)
        mock_mgmt = _make_async_mgmt_mock()
        mock_mgmt.get_node_stats = AsyncMock(return_value=[])
        with patch(f"{_ADMIN_MODULE}.AsyncManagementAPIClient", return_value=mock_mgmt):
            result = await tool._arun(fields=None)
        assert "No node statistics" in result

    @pytest.mark.asyncio
    async def test_check_health_async(self, settings: RabbitMQSettings) -> None:
        tool = CheckHealthTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.check_health = AsyncMock(
            return_value=HealthInfo(
                status=HealthStatus.OK,
                host="localhost",
                port=5672,
                message="Healthy",
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun()
        assert "OK" in result

    @pytest.mark.asyncio
    async def test_get_connection_info_async_connected(self, settings: RabbitMQSettings) -> None:
        tool = GetConnectionInfoTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.get_connection_info = AsyncMock(
            return_value=ConnectionInfo(
                host="localhost",
                port=5672,
                virtual_host="/",
                connected=True,
                server_version="3.12.4",
                server_platform="Erlang/OTP",
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun()
        assert "localhost" in result
        assert "3.12.4" in result

    @pytest.mark.asyncio
    async def test_get_connection_info_async_not_connected(
        self, settings: RabbitMQSettings
    ) -> None:
        tool = GetConnectionInfoTool(settings=settings)
        mock_client = _make_async_ctx_mock()
        mock_client.get_connection_info = AsyncMock(
            return_value=ConnectionInfo(
                host="localhost",
                port=5672,
                virtual_host="/",
                connected=False,
            )
        )
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun()
        assert "Not connected" in result

    @pytest.mark.asyncio
    async def test_close_connection_async(self, settings: RabbitMQSettings) -> None:
        tool = CloseConnectionTool(settings=settings)
        mock_client = AsyncMock()
        mock_client.connect = AsyncMock()
        mock_client.close = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun()
        assert "closed" in result.lower()
        mock_client.connect.assert_awaited_once()
        mock_client.close.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_close_connection_async_connect_error_still_closes(
        self, settings: RabbitMQSettings
    ) -> None:
        tool = CloseConnectionTool(settings=settings)
        mock_client = AsyncMock()
        mock_client.connect = AsyncMock(side_effect=RabbitMQConnectionError("refused", cause=None))
        mock_client.close = AsyncMock()
        with patch.object(tool, "_make_async_client", return_value=mock_client):
            result = await tool._arun()
        # _arun catches RabbitMQToolException and returns message
        assert "[CONNECTION_ERROR]" in result or "refused" in result
        mock_client.close.assert_awaited_once()
