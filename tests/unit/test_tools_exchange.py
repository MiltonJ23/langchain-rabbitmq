"""Unit tests for exchange management LangChain tools."""

from __future__ import annotations

from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest

from langchain_rabbitmq.exceptions import (
    RabbitMQChannelError,
    RabbitMQValidationError,
)
from langchain_rabbitmq.tools.exchange import (
    EXCHANGE_TOOLS,
    BindExchangeTool,
    DeclareExchangeTool,
    DeleteExchangeTool,
)
from langchain_rabbitmq.utilities._models import BindingInfo, ExchangeInfo, ExchangeType

pytestmark = pytest.mark.unit

if TYPE_CHECKING:
    from langchain_rabbitmq.config import RabbitMQSettings


def _mock_client(
    declare_return: ExchangeInfo | None = None,
    bind_return: BindingInfo | None = None,
) -> MagicMock:
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)

    if declare_return is None:
        declare_return = ExchangeInfo(
            name="events",
            exchange_type=ExchangeType.TOPIC,
            durable=False,
            auto_delete=False,
        )
    if bind_return is None:
        bind_return = BindingInfo(
            source="src",
            destination="dst",
            destination_type="exchange",
            routing_key="",
        )
    client.declare_exchange.return_value = declare_return
    client.delete_exchange.return_value = None
    client.bind_exchange.return_value = bind_return
    return client


class TestDeclareExchangeTool:
    def test_invoke_returns_success_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = DeclareExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "events", "exchange_type": "topic"})
        assert "events" in result
        assert "topic" in result

    def test_durable_reported(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(
            declare_return=ExchangeInfo(
                name="events",
                exchange_type=ExchangeType.DIRECT,
                durable=True,
                auto_delete=False,
            )
        )
        tool = DeclareExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "events", "durable": True})
        assert "durable=True" in result

    def test_validation_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.declare_exchange.side_effect = RabbitMQValidationError("reserved")
        tool = DeclareExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "amq.direct"})
        assert "[VALIDATION_ERROR]" in result

    def test_channel_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.declare_exchange.side_effect = RabbitMQChannelError("mismatch")
        tool = DeclareExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "events"})
        assert "[CHANNEL_ERROR]" in result

    def test_tool_metadata(self) -> None:
        t = DeclareExchangeTool()
        assert t.name == "rabbitmq_declare_exchange"
        assert t.args_schema is not None


class TestDeleteExchangeTool:
    def test_success_message(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = DeleteExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "events"})
        assert "events" in result
        assert "deleted" in result.lower()

    def test_channel_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.delete_exchange.side_effect = RabbitMQChannelError("not found")
        tool = DeleteExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "ghost"})
        assert "[CHANNEL_ERROR]" in result


class TestBindExchangeTool:
    def test_success_message(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(
            bind_return=BindingInfo(
                source="src-ex",
                destination="dst-ex",
                destination_type="exchange",
                routing_key="rk",
            )
        )
        tool = BindExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"destination": "dst-ex", "source": "src-ex", "routing_key": "rk"})
        assert "dst-ex" in result or "bound" in result.lower()

    def test_validation_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.bind_exchange.side_effect = RabbitMQValidationError("reserved")
        tool = BindExchangeTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"destination": "d", "source": "amq.direct"})
        assert "[VALIDATION_ERROR]" in result


class TestExchangeToolsList:
    def test_contains_three_tools(self) -> None:
        assert len(EXCHANGE_TOOLS) == 3

    def test_all_instantiable(self, settings: RabbitMQSettings) -> None:
        for cls in EXCHANGE_TOOLS:
            inst = cls(settings=settings)
            assert inst.name.startswith("rabbitmq_")
