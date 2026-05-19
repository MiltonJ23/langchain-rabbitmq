"""Unit tests for queue management LangChain tools."""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import (
    RabbitMQChannelError,
    RabbitMQConnectionError,
    RabbitMQValidationError,
)
from langchain_rabbitmq.tools.queue import (
    BindQueueTool,
    DeclareQueueTool,
    DeleteQueueTool,
    GetQueueInfoTool,
    PurgeQueueTool,
    QUEUE_TOOLS,
    UnbindQueueTool,
)
from langchain_rabbitmq.utilities._models import QueueInfo


def _mock_client(
    declare_return: QueueInfo | None = None,
    delete_return: int = 0,
    purge_return: int = 0,
) -> MagicMock:
    """Return a mock RabbitMQClient as a context manager."""
    client = MagicMock()
    client.__enter__ = MagicMock(return_value=client)
    client.__exit__ = MagicMock(return_value=False)

    if declare_return is None:
        declare_return = QueueInfo(
            name="orders",
            durable=False,
            exclusive=False,
            auto_delete=False,
        )
    client.declare_queue.return_value = declare_return
    client.get_queue_info.return_value = declare_return  # GetQueueInfoTool uses this method
    client.delete_queue.return_value = delete_return
    client.purge_queue.return_value = purge_return
    client.bind_queue.return_value = None
    client.unbind_queue.return_value = None
    return client


class TestDeclareQueueTool:
    def test_invoke_returns_success_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = DeclareQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "orders"})
        assert "orders" in result
        assert "declared" in result

    def test_durable_flag_reported(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(
            declare_return=QueueInfo(
                name="orders", durable=True, exclusive=False, auto_delete=False
            )
        )
        tool = DeclareQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "orders", "durable": True})
        assert "durable=True" in result

    def test_validation_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.declare_queue.side_effect = RabbitMQValidationError("bad name")
        tool = DeclareQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "bad\x00name"})
        assert "[VALIDATION_ERROR]" in result

    def test_connection_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.declare_queue.side_effect = RabbitMQConnectionError("broker down")
        tool = DeclareQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "orders"})
        assert "[CONNECTION_ERROR]" in result

    def test_name_description_args_schema(self) -> None:
        tool = DeclareQueueTool()
        assert tool.name == "rabbitmq_declare_queue"
        assert tool.args_schema is not None
        assert len(tool.description) > 10


class TestDeleteQueueTool:
    def test_returns_message_count(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(delete_return=5)
        tool = DeleteQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "orders"})
        assert "5" in result

    def test_channel_error_returned_as_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        mock.delete_queue.side_effect = RabbitMQChannelError("not found")
        tool = DeleteQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "missing"})
        assert "[CHANNEL_ERROR]" in result


class TestPurgeQueueTool:
    def test_returns_purged_count(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(purge_return=10)
        tool = PurgeQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "orders"})
        assert "10" in result


class TestBindQueueTool:
    def test_success_message(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = BindQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke(
                {"queue": "orders", "exchange": "events", "routing_key": "order.*"}
            )
        assert "bound" in result.lower() or "orders" in result


class TestUnbindQueueTool:
    def test_success_message(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client()
        tool = UnbindQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke(
                {"queue": "orders", "exchange": "events", "routing_key": "order.*"}
            )
        assert "unbound" in result.lower() or "orders" in result


class TestGetQueueInfoTool:
    def test_returns_queue_info_string(self, settings: RabbitMQSettings) -> None:
        mock = _mock_client(
            declare_return=QueueInfo(
                name="orders",
                durable=True,
                exclusive=False,
                auto_delete=False,
                message_count=7,
                consumer_count=2,
            )
        )
        tool = GetQueueInfoTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "orders"})
        assert "orders" in result
        assert "7" in result


class TestQueueToolsList:
    def test_contains_all_six_tools(self) -> None:
        assert len(QUEUE_TOOLS) == 6

    def test_all_instantiable(self, settings: RabbitMQSettings) -> None:
        for cls in QUEUE_TOOLS:
            inst = cls(settings=settings)
            assert inst.name.startswith("rabbitmq_")


class TestBaseToolErrorHandling:
    def test_unexpected_exception_returns_internal_error(
        self, settings: RabbitMQSettings
    ) -> None:
        mock = _mock_client()
        mock.declare_queue.side_effect = RuntimeError("totally unexpected")
        tool = DeclareQueueTool(settings=settings)
        with patch.object(tool, "_make_client", return_value=mock):
            result = tool.invoke({"name": "q"})
        assert "[INTERNAL_ERROR]" in result
