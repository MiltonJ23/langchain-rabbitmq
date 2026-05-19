"""Unit tests for the exception hierarchy."""

from __future__ import annotations

import pytest

from langchain_rabbitmq.exceptions import (
    ErrorCategory,
    RabbitMQAdminError,
    RabbitMQChannelError,
    RabbitMQConnectionError,
    RabbitMQMessageError,
    RabbitMQToolException,
    RabbitMQValidationError,
)


class TestErrorCategory:
    def test_values_are_lowercase_strings(self) -> None:
        assert ErrorCategory.CONNECTION.value == "connection"
        assert ErrorCategory.CHANNEL.value == "channel"
        assert ErrorCategory.MESSAGE.value == "message"
        assert ErrorCategory.ADMIN.value == "admin"
        assert ErrorCategory.VALIDATION.value == "validation"

    def test_is_str_enum(self) -> None:
        assert isinstance(ErrorCategory.CONNECTION, str)


class TestRabbitMQToolException:
    def test_message_stored(self) -> None:
        exc = RabbitMQToolException("something failed")
        assert exc.message == "something failed"

    def test_cause_none_by_default(self) -> None:
        exc = RabbitMQToolException("msg")
        assert exc.cause is None
        assert exc.__cause__ is None

    def test_cause_chained(self) -> None:
        underlying = OSError("network down")
        exc = RabbitMQToolException("outer", cause=underlying)
        assert exc.cause is underlying
        assert exc.__cause__ is underlying

    def test_to_agent_message_no_cause(self) -> None:
        exc = RabbitMQConnectionError("Broker unreachable")
        msg = exc.to_agent_message()
        assert msg == "[CONNECTION_ERROR] Broker unreachable"

    def test_to_agent_message_with_cause(self) -> None:
        cause = OSError("timed out")
        exc = RabbitMQConnectionError("Connection failed", cause=cause)
        msg = exc.to_agent_message()
        assert "[CONNECTION_ERROR] Connection failed" in msg
        assert "OSError" in msg
        assert "timed out" in msg

    def test_to_agent_message_uses_category(self) -> None:
        assert "[CHANNEL_ERROR]" in RabbitMQChannelError("ch").to_agent_message()
        assert "[MESSAGE_ERROR]" in RabbitMQMessageError("msg").to_agent_message()
        assert "[ADMIN_ERROR]" in RabbitMQAdminError("adm").to_agent_message()
        assert "[VALIDATION_ERROR]" in RabbitMQValidationError("val").to_agent_message()

    def test_repr_contains_class_name_and_message(self) -> None:
        exc = RabbitMQConnectionError("boom")
        r = repr(exc)
        assert "RabbitMQConnectionError" in r
        assert "boom" in r

    def test_is_exception(self) -> None:
        exc = RabbitMQToolException("x")
        assert isinstance(exc, Exception)


class TestSubclasses:
    @pytest.mark.parametrize(
        "cls, expected_category",
        [
            (RabbitMQConnectionError, ErrorCategory.CONNECTION),
            (RabbitMQChannelError, ErrorCategory.CHANNEL),
            (RabbitMQMessageError, ErrorCategory.MESSAGE),
            (RabbitMQAdminError, ErrorCategory.ADMIN),
            (RabbitMQValidationError, ErrorCategory.VALIDATION),
        ],
    )
    def test_category(
        self,
        cls: type[RabbitMQToolException],
        expected_category: ErrorCategory,
    ) -> None:
        exc = cls("test")
        assert exc.category == expected_category

    @pytest.mark.parametrize(
        "cls",
        [
            RabbitMQConnectionError,
            RabbitMQChannelError,
            RabbitMQMessageError,
            RabbitMQAdminError,
            RabbitMQValidationError,
        ],
    )
    def test_is_rabbitmq_tool_exception(self, cls: type[RabbitMQToolException]) -> None:
        assert issubclass(cls, RabbitMQToolException)
