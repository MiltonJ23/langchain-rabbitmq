"""Unit tests for admin and monitoring LangChain tools."""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import RabbitMQAdminError, RabbitMQConnectionError
from langchain_rabbitmq.tools.admin import (
    ADMIN_TOOLS,
    CheckHealthTool,
    CloseConnectionTool,
    GetConnectionInfoTool,
    GetNodeStatsTool,
    ListBindingsTool,
    ListExchangesTool,
    ListQueuesTool,
)
from langchain_rabbitmq.utilities._models import (
    ConnectionInfo,
    HealthInfo,
    HealthStatus,
)


# ---------------------------------------------------------------------------
# Mock factories
# ---------------------------------------------------------------------------


def _mock_mgmt_client(
    queues: list[dict[str, Any]] | None = None,
    exchanges: list[dict[str, Any]] | None = None,
    bindings: list[dict[str, Any]] | None = None,
    nodes: list[dict[str, Any]] | None = None,
) -> MagicMock:
    """Sync management client mock (context manager)."""
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    client.list_queues.return_value = queues or []
    client.list_exchanges.return_value = exchanges or []
    client.list_bindings.return_value = bindings or []
    client.get_node_stats.return_value = nodes or []
    return client


def _mock_amqp_client(
    health: HealthInfo | None = None,
    conn_info: ConnectionInfo | None = None,
) -> MagicMock:
    """Sync AMQP client mock (context manager + explicit lifecycle)."""
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)
    client.connect.return_value = None
    client.close.return_value = None
    client.check_health.return_value = health or HealthInfo(
        status=HealthStatus.OK,
        host="localhost",
        port=5672,
        message="Broker connection healthy",
    )
    client.get_connection_info.return_value = conn_info or ConnectionInfo(
        host="localhost",
        port=5672,
        virtual_host="/",
        server_version="3.12.4",
        connected=True,
    )
    return client


# ---------------------------------------------------------------------------
# ListQueuesTool
# ---------------------------------------------------------------------------


class TestListQueuesTool:
    def test_returns_queue_list(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(
            queues=[
                {"name": "orders", "messages": 5, "consumers": 2, "durable": True},
                {"name": "dlq", "messages": 0, "consumers": 0, "durable": False},
            ]
        )
        tool = ListQueuesTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({"vhost": "/"})
        assert "orders" in result
        assert "2" in result  # found count or consumer count

    def test_empty_returns_no_queues_message(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(queues=[])
        tool = ListQueuesTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({})
        assert "No queues" in result

    def test_tool_metadata(self) -> None:
        t = ListQueuesTool()
        assert t.name == "rabbitmq_list_queues"
        assert t.args_schema is not None


# ---------------------------------------------------------------------------
# ListExchangesTool
# ---------------------------------------------------------------------------


class TestListExchangesTool:
    def test_returns_exchange_list(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(
            exchanges=[
                {"name": "events", "type": "topic", "durable": True},
                {"name": "fanout-ex", "type": "fanout", "durable": False},
            ]
        )
        tool = ListExchangesTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({"include_defaults": True})
        assert "events" in result
        assert "topic" in result

    def test_default_exchange_filtered_out(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(
            exchanges=[
                {"name": "", "type": "direct", "durable": True},
                {"name": "amq.direct", "type": "direct", "durable": True},
                {"name": "user-ex", "type": "direct", "durable": False},
            ]
        )
        tool = ListExchangesTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({"include_defaults": False})
        # Only user-ex should remain
        assert "user-ex" in result
        assert "amq.direct" not in result

    def test_empty_no_user_exchanges_message(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(exchanges=[])
        tool = ListExchangesTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({})
        assert "No user-defined exchanges" in result


# ---------------------------------------------------------------------------
# ListBindingsTool
# ---------------------------------------------------------------------------


class TestListBindingsTool:
    def test_returns_binding_list(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(
            bindings=[
                {
                    "source": "events",
                    "destination": "orders",
                    "destination_type": "queue",
                    "routing_key": "order.*",
                }
            ]
        )
        tool = ListBindingsTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({})
        assert "events" in result
        assert "orders" in result

    def test_empty_returns_no_bindings(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(bindings=[])
        tool = ListBindingsTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({})
        assert "No bindings" in result


# ---------------------------------------------------------------------------
# GetNodeStatsTool
# ---------------------------------------------------------------------------


class TestGetNodeStatsTool:
    def test_returns_node_info(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(
            nodes=[
                {
                    "name": "rabbit@hostname",
                    "type": "disc",
                    "running": True,
                    "mem_used": 104857600,
                    "mem_limit": 838860800,
                    "fd_used": 34,
                    "fd_total": 1024,
                    "uptime": 86400000,
                }
            ]
        )
        tool = GetNodeStatsTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({})
        assert "rabbit@hostname" in result

    def test_empty_returns_no_stats_message(self, settings: RabbitMQSettings) -> None:
        mgmt = _mock_mgmt_client(nodes=[])
        tool = GetNodeStatsTool(settings=settings)
        with patch(
            "langchain_rabbitmq.tools.admin.ManagementAPIClient", return_value=mgmt
        ):
            result = tool.invoke({})
        assert "No node statistics" in result


# ---------------------------------------------------------------------------
# CheckHealthTool
# ---------------------------------------------------------------------------


class TestCheckHealthTool:
    def test_ok_health_reported(self, settings: RabbitMQSettings) -> None:
        amqp = _mock_amqp_client(
            health=HealthInfo(
                status=HealthStatus.OK,
                host="localhost",
                port=5672,
                message="Broker connection healthy",
            )
        )
        tool = CheckHealthTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=amqp):
            result = tool.invoke({})
        assert "OK" in result
        assert "localhost" in result

    def test_down_health_reported(self, settings: RabbitMQSettings) -> None:
        amqp = _mock_amqp_client(
            health=HealthInfo(
                status=HealthStatus.DOWN,
                host="localhost",
                port=5672,
                message="Connection refused",
            )
        )
        tool = CheckHealthTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=amqp):
            result = tool.invoke({})
        assert "DOWN" in result


# ---------------------------------------------------------------------------
# GetConnectionInfoTool
# ---------------------------------------------------------------------------


class TestGetConnectionInfoTool:
    def test_connected_info_reported(self, settings: RabbitMQSettings) -> None:
        amqp = _mock_amqp_client(
            conn_info=ConnectionInfo(
                host="localhost",
                port=5672,
                virtual_host="/",
                server_version="3.12.4",
                server_platform="Erlang/OTP 26.1",
                connected=True,
            )
        )
        tool = GetConnectionInfoTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=amqp):
            result = tool.invoke({})
        assert "3.12.4" in result
        assert "localhost" in result

    def test_disconnected_reported(self, settings: RabbitMQSettings) -> None:
        amqp = _mock_amqp_client(
            conn_info=ConnectionInfo(
                host="localhost",
                port=5672,
                virtual_host="/",
                connected=False,
            )
        )
        tool = GetConnectionInfoTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=amqp):
            result = tool.invoke({})
        assert "Not connected" in result


# ---------------------------------------------------------------------------
# CloseConnectionTool
# ---------------------------------------------------------------------------


class TestCloseConnectionTool:
    def test_graceful_close_message(self, settings: RabbitMQSettings) -> None:
        amqp = _mock_amqp_client()
        tool = CloseConnectionTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=amqp):
            result = tool.invoke({})
        assert "closed gracefully" in result
        amqp.connect.assert_called_once()
        amqp.close.assert_called_once()

    def test_connection_error_returned_as_string(
        self, settings: RabbitMQSettings
    ) -> None:
        amqp = _mock_amqp_client()
        amqp.connect.side_effect = RabbitMQConnectionError("refused")
        tool = CloseConnectionTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=amqp):
            result = tool.invoke({})
        assert "[CONNECTION_ERROR]" in result
        # close() must still be called (try/finally)
        amqp.close.assert_called_once()


# ---------------------------------------------------------------------------
# ADMIN_TOOLS list
# ---------------------------------------------------------------------------


class TestAdminToolsList:
    def test_contains_seven_tools(self) -> None:
        assert len(ADMIN_TOOLS) == 7

    def test_all_instantiable(self, settings: RabbitMQSettings) -> None:
        for cls in ADMIN_TOOLS:
            inst = cls(settings=settings)
            assert inst.name.startswith("rabbitmq_")
