"""Custom exception hierarchy for langchain-rabbitmq.

All exceptions extend :class:`RabbitMQToolException`, which provides a
:meth:`~RabbitMQToolException.to_agent_message` helper that returns a clean,
actionable string suitable for returning directly to a LangChain agent —
no raw stack traces, no internal detail leakage.

Example:
    Raising and catching a connection error::

        from langchain_rabbitmq.exceptions import RabbitMQConnectionError

        try:
            connect()
        except OSError as exc:
            raise RabbitMQConnectionError(
                "Unable to reach broker at localhost:5672", cause=exc
            ) from exc
"""

from __future__ import annotations

from enum import Enum
from typing import ClassVar, Optional


class ErrorCategory(str, Enum):
    """Broad category of a RabbitMQ error used for agent-facing classification.

    Values are lowercase strings so they embed naturally in log messages and
    structured error payloads.
    """

    CONNECTION = "connection"
    CHANNEL = "channel"
    MESSAGE = "message"
    ADMIN = "admin"
    VALIDATION = "validation"


class RabbitMQToolException(Exception):
    """Base exception for all langchain-rabbitmq errors.

    Args:
        message: Human-readable description of the error.
        cause: Optional underlying exception that triggered this error.

    Attributes:
        message: Human-readable description of the error.
        category: :class:`ErrorCategory` set per concrete subclass.
        cause: Underlying exception, if any.

    Example:
        >>> err = RabbitMQToolException("something went wrong")
        >>> err.to_agent_message()
        '[CONNECTION_ERROR] something went wrong'
    """

    category: ClassVar[ErrorCategory] = ErrorCategory.CONNECTION

    def __init__(
        self,
        message: str,
        cause: Optional[BaseException] = None,
    ) -> None:
        super().__init__(message)
        self.message = message
        self.cause = cause
        if cause is not None:
            self.__cause__ = cause

    def to_agent_message(self) -> str:
        """Return a clean, actionable error string for a LangChain agent.

        The string is intentionally free of stack traces so the agent can
        include it verbatim in its final answer or retry logic.

        Returns:
            A formatted error string, e.g.
            ``'[CONNECTION_ERROR] Broker unreachable | Caused by: OSError: ...'``.

        Example:
            >>> err = RabbitMQConnectionError("Broker unreachable")
            >>> err.to_agent_message()
            '[CONNECTION_ERROR] Broker unreachable'
        """
        label = f"[{self.category.value.upper()}_ERROR]"
        parts = [f"{label} {self.message}"]
        if self.cause is not None:
            parts.append(f"Caused by: {type(self.cause).__name__}: {self.cause}")
        return " | ".join(parts)

    def __repr__(self) -> str:
        return (
            f"{type(self).__name__}("
            f"message={self.message!r}, "
            f"category={self.category!r}, "
            f"cause={self.cause!r})"
        )


class RabbitMQConnectionError(RabbitMQToolException):
    """Raised when a connection to RabbitMQ cannot be established or is lost.

    Args:
        message: Description of the connection failure.
        cause: Optional underlying OS/network exception.

    Example:
        >>> raise RabbitMQConnectionError("Timeout after 10 s connecting to localhost:5672")
    """

    category: ClassVar[ErrorCategory] = ErrorCategory.CONNECTION


class RabbitMQChannelError(RabbitMQToolException):
    """Raised when a channel-level AMQP operation fails.

    Common causes include declaring a queue or exchange that already exists
    with incompatible parameters, or a channel being closed by the broker.

    Args:
        message: Description of the channel failure.
        cause: Optional underlying pika exception.

    Example:
        >>> raise RabbitMQChannelError("Channel 1 unexpectedly closed by broker")
    """

    category: ClassVar[ErrorCategory] = ErrorCategory.CHANNEL


class RabbitMQMessageError(RabbitMQToolException):
    """Raised when a message operation fails.

    Covers publish, consume, ack, nack, and reject operations.

    Args:
        message: Description of the message operation failure.
        cause: Optional underlying exception.

    Example:
        >>> raise RabbitMQMessageError("Basic.publish returned unroutable for key 'orders'")
    """

    category: ClassVar[ErrorCategory] = ErrorCategory.MESSAGE


class RabbitMQAdminError(RabbitMQToolException):
    """Raised when a Management HTTP API or admin AMQP operation fails.

    Args:
        message: Description of the admin operation failure.
        cause: Optional underlying HTTP or AMQP exception.

    Example:
        >>> raise RabbitMQAdminError("Management API returned 403 Forbidden")
    """

    category: ClassVar[ErrorCategory] = ErrorCategory.ADMIN


class RabbitMQValidationError(RabbitMQToolException):
    """Raised when input validation fails before any broker interaction.

    Typically triggered by invalid queue names, exchange names, or routing
    keys that violate AMQP naming constraints.

    Args:
        message: Description of the validation failure.
        cause: Optional underlying validation exception.

    Example:
        >>> raise RabbitMQValidationError("Queue name 'a b' contains whitespace")
    """

    category: ClassVar[ErrorCategory] = ErrorCategory.VALIDATION


__all__: list[str] = [
    "ErrorCategory",
    "RabbitMQAdminError",
    "RabbitMQChannelError",
    "RabbitMQConnectionError",
    "RabbitMQMessageError",
    "RabbitMQToolException",
    "RabbitMQValidationError",
]
