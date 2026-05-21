"""Shared fixtures and helpers for end-to-end agent tests.

Provides :class:`SequenceChatModel`, a deterministic mock LLM that emits
preset responses in sequence, plus pytest fixtures for mocked broker clients,
settings, and an LLM factory.
"""

from __future__ import annotations

from typing import TYPE_CHECKING, Any
from unittest.mock import MagicMock

import pytest
from langchain_core.language_models import BaseChatModel
from langchain_core.messages import AIMessage, BaseMessage
from langchain_core.outputs import ChatGeneration, ChatResult
from pydantic import PrivateAttr

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.utilities._models import (
    ConnectionInfo,
    HealthInfo,
    HealthStatus,
    QueueInfo,
)

pytestmark = pytest.mark.e2e

if TYPE_CHECKING:
    from collections.abc import Sequence


class SequenceChatModel(BaseChatModel):
    """Deterministic mock LLM that emits preset AIMessage responses in order.

    The last response is repeated if the sequence is exhausted.
    ``bind_tools`` is a no-op that returns ``self`` — the model ignores tool
    schemas and relies solely on the preset :attr:`responses`.

    Attributes:
        responses: Ordered list of :class:`~langchain_core.messages.AIMessage`
            instances returned on successive ``_generate`` calls.

    Example:
        Create an LLM that calls a tool then gives a final answer::

            llm = SequenceChatModel(responses=[
                AIMessage(
                    content="",
                    tool_calls=[{
                        "name": "rabbitmq_declare_queue",
                        "args": {"name": "orders"},
                        "id": "call_1",
                        "type": "tool_call",
                    }],
                ),
                AIMessage(content="Queue declared successfully."),
            ])
    """

    responses: list[AIMessage]
    _call_count: int = PrivateAttr(default=0)

    def _generate(
        self,
        messages: list[BaseMessage],
        stop: list[str] | None = None,
        run_manager: Any = None,
        **kwargs: Any,
    ) -> ChatResult:
        """Return the next preset response.

        Args:
            messages: Conversation history (ignored — responses are preset).
            stop: Stop sequences (ignored).
            run_manager: LangChain callback manager.
            **kwargs: Extra parameters (ignored).

        Returns:
            A :class:`~langchain_core.outputs.ChatResult` wrapping the next
            preset :class:`~langchain_core.messages.AIMessage`.
        """
        idx = min(self._call_count, len(self.responses) - 1)
        self._call_count += 1
        return ChatResult(generations=[ChatGeneration(message=self.responses[idx])])

    @property
    def _llm_type(self) -> str:
        return "sequence"

    def bind_tools(
        self,
        tools: Sequence[Any],
        **kwargs: Any,
    ) -> SequenceChatModel:
        """No-op — preset responses encode tool decisions directly.

        Args:
            tools: Tool schemas (ignored).
            **kwargs: Extra parameters (ignored).

        Returns:
            ``self`` unchanged.
        """
        return self


@pytest.fixture
def mock_settings() -> RabbitMQSettings:
    """Fake broker settings — no real broker required.

    Returns:
        A :class:`~langchain_rabbitmq.config.RabbitMQSettings` instance
        with default ``localhost:5672`` guest credentials.
    """
    return RabbitMQSettings()


@pytest.fixture
def mock_sync_client() -> MagicMock:
    """MagicMock for ``RabbitMQClient`` with realistic return values.

    Configured as a context manager: ``__enter__`` returns ``self``.

    Returns:
        A :class:`unittest.mock.MagicMock` pre-configured with expected return
        values for common AMQP operations (declare, publish, health, connection).
    """
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)

    client.declare_queue.return_value = QueueInfo(
        name="orders",
        durable=True,
        exclusive=False,
        auto_delete=False,
        message_count=0,
        consumer_count=0,
    )
    client.delete_queue.return_value = 0
    client.purge_queue.return_value = 0
    client.bind_queue.return_value = None
    client.unbind_queue.return_value = None
    client.get_queue_info.return_value = QueueInfo(
        name="orders",
        durable=True,
        exclusive=False,
        auto_delete=False,
        message_count=5,
        consumer_count=1,
    )
    client.publish_message.return_value = None
    client.consume_message.return_value = None
    client.check_health.return_value = HealthInfo(
        status=HealthStatus.OK,
        host="localhost",
        port=5672,
        message="Broker connection healthy",
    )
    client.get_connection_info.return_value = ConnectionInfo(
        host="localhost",
        port=5672,
        virtual_host="/",
        server_version="3.12.0",
        server_platform="Erlang/OTP 26",
        connected=True,
    )
    return client


@pytest.fixture
def sequence_llm_factory():
    """Factory fixture that creates fresh :class:`SequenceChatModel` instances.

    Yields:
        A callable that accepts a list of
        :class:`~langchain_core.messages.AIMessage` and returns a new
        :class:`SequenceChatModel` with a reset call counter.

    Example:
        ::

            def test_something(sequence_llm_factory):
                llm = sequence_llm_factory([
                    AIMessage(content="", tool_calls=[...]),
                    AIMessage(content="Done."),
                ])
    """

    def _factory(responses: list[AIMessage]) -> SequenceChatModel:
        return SequenceChatModel(responses=responses)

    return _factory
