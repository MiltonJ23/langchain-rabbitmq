"""Unit tests for message operation LangChain tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import (
    RabbitMQMessageError,
    RabbitMQValidationError,
)
from langchain_rabbitmq.tools.message import (
    AckMessageTool,
    ConsumeMessageTool,
    MESSAGE_TOOLS,
    NackMessageTool,
    PublishMessageTool,
    RejectMessageTool,
)
from langchain_rabbitmq.utilities._models import MessageResult


def _mock_client(
    consume_return: MessageResult | None = None,
    consume_empty: bool = False,
) -> MagicMock:
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)

    if consume_empty:
        client.consume_message.return_value = None
    elif consume_return is None:
        client.consume_message.return_value = MessageResult(
            body=b'{"id": 1}',
            delivery_tag=1,
            exchange="",
            routing_key="orders",
            redelivered=False,
            content_type="application/json",
        )
    else:
        client.consume_message.return_value = consume_return

    client.publish_message.return_value = None
    client.ack_message.return_value = None
    client.nack_message.return_value = None
    client.reject_message.return_value = None
    return client


class TestPublishMessageTool:
    def test_success_message_contains_exchange_and_key(
        self, settings: RabbitMQSettings
    ) -> None:
        mock = _mock_client()
        tool = PublishMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke(
                {"exchange": "", "routing_key": "orders", "body": '{"id": 1}'}
            )
        assert "orders" in result
        assert "published" in result.lower() or "sent" in result.lower()

    def test_persistent_flag_passed(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = PublishMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            tool.invoke(
                {
                    "exchange": "",
                    "routing_key": "orders",
                    "body": "hello",
                    "persistent": True,
                }
            )
        call_kwargs = mock.publish_message.call_args
        assert call_kwargs.kwargs.get("persistent") is True

    def test_message_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.publish_message.side_effect = RabbitMQMessageError("unroutable")
        tool = PublishMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"routing_key": "ghost", "body": "x"})
        assert "[MESSAGE_ERROR]" in result

    def test_validation_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.publish_message.side_effect = RabbitMQValidationError("bad exchange")
        tool = PublishMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"exchange": "amq.direct", "routing_key": "r", "body": "x"})
        assert "[VALIDATION_ERROR]" in result

    def test_tool_metadata(self) -> None:
        t = PublishMessageTool()
        assert t.name == "rabbitmq_publish_message"
        assert t.args_schema is not None


class TestConsumeMessageTool:
    def test_returns_message_body(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(
            consume_return=MessageResult(
                body=b'{"id": 99}',
                delivery_tag=7,
                exchange="",
                routing_key="orders",
                redelivered=False,
            )
        )
        tool = ConsumeMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"queue": "orders"})
        assert '{"id": 99}' in result or "orders" in result

    def test_empty_queue_returns_no_message(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(consume_empty=True)
        tool = ConsumeMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"queue": "empty"})
        assert "empty" in result.lower() or "no message" in result.lower()

    def test_message_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.consume_message.side_effect = RabbitMQMessageError("channel closed")
        tool = ConsumeMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"queue": "orders"})
        assert "[MESSAGE_ERROR]" in result


class TestAckMessageTool:
    def test_success_string_contains_delivery_tag(
        self, settings: RabbitMQSettings
    ) -> None:
        mock = _mock_client()
        tool = AckMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"delivery_tag": 42})
        assert "42" in result

    def test_message_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.ack_message.side_effect = RabbitMQMessageError("tag not found")
        tool = AckMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"delivery_tag": 1})
        assert "[MESSAGE_ERROR]" in result


class TestNackMessageTool:
    def test_success_string_contains_delivery_tag(
        self, settings: RabbitMQSettings
    ) -> None:
        mock = _mock_client()
        tool = NackMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"delivery_tag": 10, "requeue": False})
        assert "10" in result

    def test_requeue_false_reported(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = NackMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            tool.invoke({"delivery_tag": 1, "requeue": False})
        call_kwargs = mock.nack_message.call_args
        assert call_kwargs.kwargs.get("requeue") is False


class TestRejectMessageTool:
    def test_success_string_contains_delivery_tag(
        self, settings: RabbitMQSettings
    ) -> None:
        mock = _mock_client()
        tool = RejectMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"delivery_tag": 5})
        assert "5" in result

    def test_message_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.reject_message.side_effect = RabbitMQMessageError("closed")
        tool = RejectMessageTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"delivery_tag": 5})
        assert "[MESSAGE_ERROR]" in result


class TestMessageToolsList:
    def test_contains_five_tools(self) -> None:
        assert len(MESSAGE_TOOLS) == 5

    def test_all_instantiable(self, settings: RabbitMQSettings) -> None:
        for cls in MESSAGE_TOOLS:
            inst = cls(settings=settings)
            assert inst.name.startswith("rabbitmq_")
