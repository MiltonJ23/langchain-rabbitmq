"""End-to-end agent dispatch scenarios using a mocked LLM.

Validates that a real LangChain agent loop (``create_agent`` from
``langchain.agents``) can:

* Dispatch RabbitMQ tool calls embedded in ``AIMessage.tool_calls``.
* Return tool results back to the agent as ``ToolMessage`` entries.
* Complete multi-step workflows (declare → publish) without exceptions.
* Surface tool error strings to the agent instead of crashing.

No real RabbitMQ broker is required — AMQP clients are replaced with
:class:`unittest.mock.MagicMock` instances at the ``_RabbitMQBaseTool``
class level.
"""

from __future__ import annotations

from typing import Any
from unittest.mock import MagicMock, patch

import pytest
from langchain.agents import create_agent
from langchain_core.messages import AIMessage

import langchain_rabbitmq.tools.admin as admin_module
from langchain_rabbitmq.exceptions import RabbitMQConnectionError
from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.tools.toolkit import RabbitMQToolkit


# ---------------------------------------------------------------------------
# Scenario 1 — declare queue then publish message
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestAgentDeclareAndPublish:
    """Full agent loop: LLM calls declare_queue, then publish_message."""

    def test_agent_declare_then_publish(
        self,
        mock_settings: Any,
        mock_sync_client: MagicMock,
        sequence_llm_factory: Any,
    ) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_declare_queue",
                            "args": {"name": "orders", "durable": True},
                            "id": "call_declare_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_publish_message",
                            "args": {
                                "exchange": "",
                                "routing_key": "orders",
                                "body": '{"order_id": 42}',
                                "persistent": True,
                                "content_type": "application/json",
                            },
                            "id": "call_publish_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(
                    content="Declared queue 'orders' and published the JSON message."
                ),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            _RabbitMQBaseTool, "_make_client", return_value=mock_sync_client
        ):
            result = agent.invoke(
                {
                    "messages": [
                        ("user", "Declare queue 'orders' and publish a JSON message.")
                    ]
                }
            )

        messages = result["messages"]
        assert messages[-1].content == (
            "Declared queue 'orders' and published the JSON message."
        )
        mock_sync_client.declare_queue.assert_called_once()
        mock_sync_client.publish_message.assert_called_once()

    def test_agent_receives_tool_result_in_message_history(
        self,
        mock_settings: Any,
        mock_sync_client: MagicMock,
        sequence_llm_factory: Any,
    ) -> None:
        """Tool result string appears in the message history passed to the LLM."""
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_declare_queue",
                            "args": {"name": "test-q"},
                            "id": "call_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="Queue declared."),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            _RabbitMQBaseTool, "_make_client", return_value=mock_sync_client
        ):
            result = agent.invoke(
                {"messages": [("user", "Declare a test queue.")]}
            )

        # Message sequence: HumanMessage → AIMessage (tool_call) → ToolMessage → AIMessage
        assert len(result["messages"]) == 4
        tool_result_msg = result["messages"][2]
        content = tool_result_msg.content
        assert "declared" in content.lower() or "orders" in content

    def test_declare_queue_tool_args_forwarded_correctly(
        self,
        mock_settings: Any,
        mock_sync_client: MagicMock,
        sequence_llm_factory: Any,
    ) -> None:
        """Args from the LLM tool_call are forwarded verbatim to the client."""
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_declare_queue",
                            "args": {
                                "name": "invoices",
                                "durable": True,
                                "auto_delete": False,
                            },
                            "id": "call_a",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="Done."),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            _RabbitMQBaseTool, "_make_client", return_value=mock_sync_client
        ):
            agent.invoke({"messages": [("user", "Declare invoices queue.")]})

        mock_sync_client.declare_queue.assert_called_once()
        positional_arg = mock_sync_client.declare_queue.call_args[0][0]
        assert positional_arg == "invoices"


# ---------------------------------------------------------------------------
# Scenario 2 — list queues via Management API
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestAgentListQueues:
    """Full agent loop: LLM calls rabbitmq_list_queues."""

    def _make_mgmt_mock(self) -> MagicMock:
        mock_mgmt = MagicMock()
        mock_mgmt.__enter__ = MagicMock(return_value=mock_mgmt)
        mock_mgmt.__exit__ = MagicMock(return_value=False)
        mock_mgmt.list_queues.return_value = [
            {"name": "orders", "messages": 10, "consumers": 2, "durable": True},
            {"name": "notifications", "messages": 0, "consumers": 0, "durable": False},
        ]
        return mock_mgmt

    def test_agent_list_queues(
        self, mock_settings: Any, sequence_llm_factory: Any
    ) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()
        mock_mgmt = self._make_mgmt_mock()

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_list_queues",
                            "args": {"vhost": ""},
                            "id": "call_lq_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(
                    content="There are 2 queues: orders and notifications."
                ),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            admin_module, "ManagementAPIClient", return_value=mock_mgmt
        ):
            result = agent.invoke({"messages": [("user", "List all queues.")]})

        assert (
            result["messages"][-1].content
            == "There are 2 queues: orders and notifications."
        )
        tool_msg = result["messages"][2]
        assert "orders" in tool_msg.content
        assert "notifications" in tool_msg.content

    def test_agent_list_queues_empty_result(
        self, mock_settings: Any, sequence_llm_factory: Any
    ) -> None:
        """Agent handles empty queue list without error."""
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        mock_mgmt = MagicMock()
        mock_mgmt.__enter__ = MagicMock(return_value=mock_mgmt)
        mock_mgmt.__exit__ = MagicMock(return_value=False)
        mock_mgmt.list_queues.return_value = []

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_list_queues",
                            "args": {},
                            "id": "call_lq_2",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="No queues found."),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            admin_module, "ManagementAPIClient", return_value=mock_mgmt
        ):
            result = agent.invoke({"messages": [("user", "Are there any queues?")]})

        tool_msg = result["messages"][2]
        assert "no queues" in tool_msg.content.lower()


# ---------------------------------------------------------------------------
# Scenario 3 — health check
# ---------------------------------------------------------------------------


@pytest.mark.e2e
class TestAgentHealthCheck:
    """Full agent loop: LLM calls rabbitmq_check_health."""

    def test_agent_health_check_ok(
        self,
        mock_settings: Any,
        mock_sync_client: MagicMock,
        sequence_llm_factory: Any,
    ) -> None:
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_check_health",
                            "args": {},
                            "id": "call_hc_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="The broker is healthy and responsive."),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            _RabbitMQBaseTool, "_make_client", return_value=mock_sync_client
        ):
            result = agent.invoke({"messages": [("user", "Check broker health.")]})

        assert (
            result["messages"][-1].content == "The broker is healthy and responsive."
        )
        tool_msg = result["messages"][2]
        assert "OK" in tool_msg.content

    def test_agent_handles_tool_error_gracefully(
        self, mock_settings: Any, sequence_llm_factory: Any
    ) -> None:
        """When a tool returns a connection error string, the agent still completes."""
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        error_client = MagicMock()
        error_client.__enter__ = MagicMock(return_value=error_client)
        error_client.__exit__ = MagicMock(return_value=False)
        error_client.check_health.side_effect = RabbitMQConnectionError(
            "broker unreachable"
        )

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_check_health",
                            "args": {},
                            "id": "call_err_1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="The broker appears to be down."),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            _RabbitMQBaseTool, "_make_client", return_value=error_client
        ):
            result = agent.invoke({"messages": [("user", "Check broker health.")]})

        # Agent must complete (4 messages) even when the tool returns an error
        assert len(result["messages"]) == 4
        tool_msg = result["messages"][2]
        assert isinstance(tool_msg.content, str)
        assert "CONNECTION_ERROR" in tool_msg.content

    def test_agent_completes_multi_step_without_real_broker(
        self,
        mock_settings: Any,
        mock_sync_client: MagicMock,
        sequence_llm_factory: Any,
    ) -> None:
        """Three-step agent loop (health → declare → final) completes cleanly."""
        toolkit = RabbitMQToolkit(settings=mock_settings)
        tools = toolkit.get_tools()

        llm = sequence_llm_factory(
            [
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_check_health",
                            "args": {},
                            "id": "step1",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(
                    content="",
                    tool_calls=[
                        {
                            "name": "rabbitmq_declare_queue",
                            "args": {"name": "verified-q"},
                            "id": "step2",
                            "type": "tool_call",
                        }
                    ],
                ),
                AIMessage(content="Broker healthy; queue 'verified-q' is ready."),
            ]
        )

        agent = create_agent(llm, tools)
        with patch.object(
            _RabbitMQBaseTool, "_make_client", return_value=mock_sync_client
        ):
            result = agent.invoke(
                {
                    "messages": [
                        ("user", "Check health, then declare queue 'verified-q'.")
                    ]
                }
            )

        # 6 messages: Human, AI(tool), Tool, AI(tool), Tool, AI(final)
        assert len(result["messages"]) == 6
        assert result["messages"][-1].content == (
            "Broker healthy; queue 'verified-q' is ready."
        )
        mock_sync_client.check_health.assert_called_once()
        mock_sync_client.declare_queue.assert_called_once()
