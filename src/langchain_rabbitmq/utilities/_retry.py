"""Tenacity retry helpers for transient AMQP errors.

Both the synchronous (pika) and asynchronous (aio-pika) clients use these
helpers so that connection-level retry logic lives in one place.

Example:
    Wrapping a synchronous call::

        from langchain_rabbitmq.utilities._retry import make_sync_retry
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings()
        for attempt in make_sync_retry(settings):
            with attempt:
                connection.channel()
"""

from __future__ import annotations

import logging
from typing import TYPE_CHECKING

from tenacity import (
    AsyncRetrying,
    RetryCallState,
    Retrying,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

if TYPE_CHECKING:
    from langchain_rabbitmq.config import RabbitMQSettings

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# pika transient exception types — imported lazily so the module does not
# crash when pika is not installed in environments that only use aio-pika.
# ---------------------------------------------------------------------------


def _pika_transient_exceptions() -> tuple[type[BaseException], ...]:
    """Return pika transient exception types, or empty tuple if unavailable."""
    try:
        import pika.exceptions  # type: ignore[import-untyped]

        return (
            pika.exceptions.AMQPConnectionError,  # type: ignore[attr-defined]
            pika.exceptions.StreamLostError,  # type: ignore[attr-defined]
            pika.exceptions.ConnectionClosedByBroker,  # type: ignore[attr-defined]
        )
    except ImportError:
        return ()


def _aio_pika_transient_exceptions() -> tuple[type[BaseException], ...]:
    """Return aio-pika transient exception types, or empty tuple if unavailable."""
    try:
        import aio_pika.exceptions  # type: ignore[import-untyped]

        return (aio_pika.exceptions.AMQPConnectionError,)  # type: ignore[attr-defined]
    except ImportError:
        return ()


# ---------------------------------------------------------------------------
# Retry attempt logger
# ---------------------------------------------------------------------------


def _log_retry_attempt(retry_state: RetryCallState) -> None:
    """Log each retry attempt at WARNING level.

    Args:
        retry_state: Tenacity state bag for the current attempt.
    """
    exc = retry_state.outcome.exception() if retry_state.outcome else None
    logger.warning(
        "RabbitMQ transient error — attempt %d/%s.  Error: %s",
        retry_state.attempt_number,
        retry_state.retry_object.stop,  # type: ignore[attr-defined]
        exc,
    )


# ---------------------------------------------------------------------------
# Public factory functions
# ---------------------------------------------------------------------------


def make_sync_retry(settings: RabbitMQSettings) -> Retrying:
    """Build a :class:`tenacity.Retrying` policy tuned to pika transient errors.

    Args:
        settings: :class:`~langchain_rabbitmq.config.RabbitMQSettings` instance
            that carries ``max_retries`` and ``retry_delay``.

    Returns:
        A configured :class:`tenacity.Retrying` context manager / iterator.
    """
    transient: tuple[type[BaseException], ...] = _pika_transient_exceptions()

    retry_condition: retry_if_exception_type | bool
    if transient:
        retry_condition = retry_if_exception_type(transient)
    else:
        # If pika is unavailable, never retry (no transient errors possible).
        retry_condition = retry_if_exception_type(())  # type: ignore[arg-type]

    return Retrying(
        retry=retry_condition,
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(
            multiplier=settings.retry_delay,
            min=settings.retry_delay,
            max=settings.retry_delay * 8,
        ),
        before_sleep=_log_retry_attempt,
        reraise=True,
    )


def make_async_retry(settings: RabbitMQSettings) -> AsyncRetrying:
    """Build an :class:`tenacity.AsyncRetrying` policy for aio-pika errors.

    Args:
        settings: :class:`~langchain_rabbitmq.config.RabbitMQSettings` instance.

    Returns:
        A configured :class:`tenacity.AsyncRetrying` async context manager.
    """
    transient: tuple[type[BaseException], ...] = _aio_pika_transient_exceptions()

    retry_condition: retry_if_exception_type | bool
    if transient:
        retry_condition = retry_if_exception_type(transient)
    else:
        retry_condition = retry_if_exception_type(())  # type: ignore[arg-type]

    return AsyncRetrying(
        retry=retry_condition,
        stop=stop_after_attempt(settings.max_retries),
        wait=wait_exponential(
            multiplier=settings.retry_delay,
            min=settings.retry_delay,
            max=settings.retry_delay * 8,
        ),
        before_sleep=_log_retry_attempt,
        reraise=True,
    )


__all__: list[str] = [
    "make_async_retry",
    "make_sync_retry",
]
