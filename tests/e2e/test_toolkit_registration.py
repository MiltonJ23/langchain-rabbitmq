"""Tests for RabbitMQToolkit structure, tool registration, and direct invocation.

Validates that:

* :class:`~langchain_rabbitmq.tools.toolkit.RabbitMQToolkit` is a valid
  :class:`~langchain_core.tools.BaseToolkit` exposing exactly 21 tools.
* Every tool has a valid ``name`` (``rabbitmq_`` prefix), non-empty
  ``description``, and a :class:`~pydantic.BaseModel` ``args_schema``.
* Tools are directly invocable via ``.invoke()`` and ``.ainvoke()`` when
  the broker client is mocked — no real broker needed.
* Tool errors are returned as agent-readable strings, not raised as exceptions.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from langchain_core.tools import BaseTool, BaseToolkit
from pydantic import BaseModel

import langchain_rabbitmq.tools.admin as admin_module
from langchain_rabbitmq.exceptions import RabbitMQConnectionError
from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.tools.admin import CheckHealthTool, ListQueuesTool
from langchain_rabbitmq.tools.message import PublishMessageTool
from langchain_rabbitmq.tools.queue import DeclareQueueTool
from langchain_rabbitmq.tools.toolkit import ALL_TOOLS, RabbitMQToolkit
from langchain_rabbitmq.utilities._models import QueueInfo


# ---------------------------------------------------------------------------
# Toolkit structure
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestToolkitStructure:
    """Verify the toolkit is a valid LangChain BaseToolkit with 21 tools."""

    def test_toolkit_is_base_toolkit(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        assert isinstance(toolkit, BaseToolkit)

    def test_get_tools_returns_21_tools(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        assert len(toolkit.get_tools()) == 21

    def test_all_tools_count_matches_all_tools_list(self) -> None:
        assert len(ALL_TOOLS) == 21

    def test_all_tools_are_base_tool_instances(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        for tool in toolkit.get_tools():
            assert isinstance(tool, BaseTool), f"{tool!r} is not a BaseTool"

    def test_all_tool_names_prefixed_rabbitmq(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        for tool in toolkit.get_tools():
            assert tool.name.startswith("rabbitmq_"), (
                f"Tool {tool.name!r} does not start with 'rabbitmq_'"
            )

    def test_all_tools_have_nonempty_description(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        for tool in toolkit.get_tools():
            assert len(tool.description) > 20, (
                f"Tool {tool.name!r} description too short: {tool.description!r}"
            )

    def test_all_tools_have_valid_args_schema(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        for tool in toolkit.get_tools():
            assert tool.args_schema is not None, (
                f"Tool {tool.name!r} has no args_schema"
            )
            assert issubclass(tool.args_schema, BaseModel), (
                f"Tool {tool.name!r} args_schema is not a BaseModel subclass"
            )

    def test_tool_names_are_unique(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        names = [t.name for t in toolkit.get_tools()]
        assert len(names) == len(set(names)), "Duplicate tool names detected"

    def test_from_settings_factory(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit.from_settings(mock_settings)
        assert isinstance(toolkit, RabbitMQToolkit)
        assert len(toolkit.get_tools()) == 21

    def test_default_settings_creates_toolkit(self) -> None:
        toolkit = RabbitMQToolkit()
        assert len(toolkit.get_tools()) == 21

    def test_all_tools_share_same_settings(self, mock_settings: Any) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        for tool in toolkit.get_tools():
            assert tool.settings.host == mock_settings.host
            assert tool.settings.port == mock_settings.port


# ---------------------------------------------------------------------------
# Direct tool invocation (no agent, no broker)
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestDirectToolInvocation:
    """Verify individual tools are invocable with a mocked AMQP client."""

    def test_declare_queue_invoke(
        self, mock_settings: Any, mock_sync_client: MagicMock
    ) -> None:
        tool = DeclareQueueTool(settings=mock_settings)
        with patch.object(tool, "_make_client", return_value=mock_sync_client):
            result = tool.invoke({"name": "orders", "durable": True})
        assert isinstance(result, str)
        assert "orders" in result
        assert "declared" in result.lower()

    def test_publish_message_invoke(
        self, mock_settings: Any, mock_sync_client: MagicMock
    ) -> None:
        tool = PublishMessageTool(settings=mock_settings)
        with patch.object(tool, "_make_client", return_value=mock_sync_client):
            result = tool.invoke(
                {
                    "exchange": "",
                    "routing_key": "orders",
                    "body": '{"id": 1}',
                    "persistent": True,
                    "content_type": "application/json",
                }
            )
        assert isinstance(result, str)
        assert "orders" in result
        assert "published" in result.lower()

    def test_check_health_invoke(
        self, mock_settings: Any, mock_sync_client: MagicMock
    ) -> None:
        tool = CheckHealthTool(settings=mock_settings)
        with patch.object(tool, "_make_client", return_value=mock_sync_client):
            result = tool.invoke({})
        assert isinstance(result, str)
        assert "OK" in result

    def test_list_queues_invoke(self, mock_settings: Any) -> None:
        mock_mgmt = MagicMock()
        mock_mgmt.__enter__ = MagicMock(return_value=mock_mgmt)
        mock_mgmt.__exit__ = MagicMock(return_value=False)
        mock_mgmt.list_queues.return_value = [
            {"name": "orders", "messages": 5, "consumers": 1, "durable": True},
            {"name": "events", "messages": 0, "consumers": 0, "durable": False},
        ]

        tool = ListQueuesTool(settings=mock_settings)
        with patch.object(admin_module, "ManagementAPIClient", return_value=mock_mgmt):
            result = tool.invoke({"vhost": ""})

        assert "Found 2 queue(s)" in result
        assert "orders" in result
        assert "events" in result

    async def test_declare_queue_ainvoke(self, mock_settings: Any) -> None:
        """Async invocation path uses ``_make_async_client``."""
        mock_async = AsyncMock()
        mock_async.__aenter__ = AsyncMock(return_value=mock_async)
        mock_async.__aexit__ = AsyncMock(return_value=False)
        mock_async.declare_queue = AsyncMock(
            return_value=QueueInfo(
                name="orders",
                durable=True,
                exclusive=False,
                auto_delete=False,
            )
        )

        tool = DeclareQueueTool(settings=mock_settings)
        with patch.object(tool, "_make_async_client", return_value=mock_async):
            result = await tool.ainvoke({"name": "orders", "durable": True})

        assert isinstance(result, str)
        assert "orders" in result
        assert "declared" in result.lower()

    def test_tool_error_returns_string_not_exception(
        self, mock_settings: Any
    ) -> None:
        """Tools must return agent-readable error strings, not raise exceptions."""
        error_client = MagicMock()
        error_client.__enter__ = MagicMock(return_value=error_client)
        error_client.__exit__ = MagicMock(return_value=False)
        error_client.declare_queue.side_effect = RabbitMQConnectionError(
            "broker unreachable"
        )

        tool = DeclareQueueTool(settings=mock_settings)
        with patch.object(tool, "_make_client", return_value=error_client):
            result = tool.invoke({"name": "orders"})

        assert isinstance(result, str)
        assert "CONNECTION_ERROR" in result
        assert "unreachable" in result

    def test_all_21_tools_invocable_with_mock(
        self, mock_settings: Any, mock_sync_client: MagicMock
    ) -> None:
        """Every tool in the toolkit can be invoked without raising."""
        toolkit = RabbitMQToolkit(settings=mock_settings)

        mock_mgmt = MagicMock()
        mock_mgmt.__enter__ = MagicMock(return_value=mock_mgmt)
        mock_mgmt.__exit__ = MagicMock(return_value=False)
        mock_mgmt.list_queues.return_value = []
        mock_mgmt.list_exchanges.return_value = []
        mock_mgmt.list_bindings.return_value = []
        mock_mgmt.get_node_stats.return_value = []

        # Minimal valid inputs per tool
        min_inputs: dict[str, dict[str, Any]] = {
            "rabbitmq_declare_queue": {"name": "q"},
            "rabbitmq_delete_queue": {"name": "q"},
            "rabbitmq_purge_queue": {"name": "q"},
            "rabbitmq_bind_queue": {
                "queue": "q",
                "exchange": "ex",
                "routing_key": "rk",
            },
            "rabbitmq_unbind_queue": {
                "queue": "q",
                "exchange": "ex",
                "routing_key": "rk",
            },
            "rabbitmq_get_queue_info": {"name": "q"},
            "rabbitmq_declare_exchange": {"name": "ex", "exchange_type": "direct"},
            "rabbitmq_delete_exchange": {"name": "ex"},
            "rabbitmq_bind_exchange": {
                "destination": "d",
                "source": "s",
                "routing_key": "rk",
            },
            "rabbitmq_publish_message": {
                "exchange": "",
                "routing_key": "q",
                "body": "hello",
            },
            "rabbitmq_consume_message": {"queue": "q"},
            "rabbitmq_ack_message": {"delivery_tag": 1},
            "rabbitmq_nack_message": {"delivery_tag": 1},
            "rabbitmq_reject_message": {"delivery_tag": 1},
            "rabbitmq_list_queues": {},
            "rabbitmq_list_exchanges": {},
            "rabbitmq_list_bindings": {},
            "rabbitmq_get_node_stats": {},
            "rabbitmq_check_health": {},
            "rabbitmq_get_connection_info": {},
            "rabbitmq_close_connection": {},
        }

        with (
            patch.object(
                _RabbitMQBaseTool, "_make_client", return_value=mock_sync_client
            ),
            patch.object(
                admin_module, "ManagementAPIClient", return_value=mock_mgmt
            ),
        ):
            for tool in toolkit.get_tools():
                inputs = min_inputs.get(tool.name, {})
                result = tool.invoke(inputs)
                assert isinstance(result, str), (
                    f"Tool {tool.name!r} did not return a string"
                )
