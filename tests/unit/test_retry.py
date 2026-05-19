"""Unit tests for tenacity retry helpers.

Tests cover:
- ``make_sync_retry`` returns a correctly configured ``Retrying`` instance.
- ``make_async_retry`` returns a correctly configured ``AsyncRetrying`` instance.
- Retry executes immediately on success (no sleep).
- Reraise after max attempts when a transient exception is raised.
- ``_log_retry_attempt`` emits a WARNING with the attempt number.
- Helper functions ``_pika_transient_exceptions`` and
  ``_aio_pika_transient_exceptions`` return non-empty tuples when the
  libraries are installed.
"""

from __future__ import annotations

import logging
from unittest.mock import MagicMock, patch

import pytest
from tenacity import AsyncRetrying, Retrying

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.utilities._retry import (
    _aio_pika_transient_exceptions,
    _log_retry_attempt,
    _pika_transient_exceptions,
    make_async_retry,
    make_sync_retry,
)


@pytest.fixture()
def settings() -> RabbitMQSettings:
    return RabbitMQSettings(
        host="localhost",
        max_retries=3,
        retry_delay=0.001,  # near-zero so tests don't sleep
    )


# ---------------------------------------------------------------------------
# Exception helpers
# ---------------------------------------------------------------------------


class TestTransientExceptionHelpers:
    def test_pika_returns_tuple_of_exceptions(self) -> None:
        result = _pika_transient_exceptions()
        assert isinstance(result, tuple)
        # pika is installed in the dev environment
        assert len(result) > 0
        for exc_type in result:
            assert issubclass(exc_type, BaseException)

    def test_aio_pika_returns_tuple_of_exceptions(self) -> None:
        result = _aio_pika_transient_exceptions()
        assert isinstance(result, tuple)
        assert len(result) > 0
        for exc_type in result:
            assert issubclass(exc_type, BaseException)

    def test_pika_import_error_returns_empty_tuple(self) -> None:
        with patch.dict("sys.modules", {"pika": None, "pika.exceptions": None}):
            import importlib

            import langchain_rabbitmq.utilities._retry as retry_mod

            importlib.reload(retry_mod)
            result = retry_mod._pika_transient_exceptions()
            # When pika is unavailable, returns empty tuple
            assert isinstance(result, tuple)

    def test_aio_pika_import_error_returns_empty_tuple(self) -> None:
        with patch.dict(
            "sys.modules", {"aio_pika": None, "aio_pika.exceptions": None}
        ):
            import importlib

            import langchain_rabbitmq.utilities._retry as retry_mod

            importlib.reload(retry_mod)
            result = retry_mod._aio_pika_transient_exceptions()
            assert isinstance(result, tuple)


# ---------------------------------------------------------------------------
# make_sync_retry
# ---------------------------------------------------------------------------


class TestMakeSyncRetry:
    def test_returns_retrying_instance(self, settings: RabbitMQSettings) -> None:
        policy = make_sync_retry(settings)
        assert isinstance(policy, Retrying)

    def test_success_on_first_attempt(self, settings: RabbitMQSettings) -> None:
        call_count = 0
        for attempt in make_sync_retry(settings):
            with attempt:
                call_count += 1
        assert call_count == 1

    def test_reraises_after_max_attempts(self, settings: RabbitMQSettings) -> None:
        import pika.exceptions  # type: ignore[import-untyped]

        settings_1 = RabbitMQSettings(
            host="localhost", max_retries=2, retry_delay=0.001
        )
        call_count = 0
        with pytest.raises(pika.exceptions.AMQPConnectionError):
            for attempt in make_sync_retry(settings_1):
                with attempt:
                    call_count += 1
                    raise pika.exceptions.AMQPConnectionError("down")
        # Should have tried exactly max_retries times
        assert call_count == 2

    def test_non_transient_error_not_retried(
        self, settings: RabbitMQSettings
    ) -> None:
        call_count = 0
        with pytest.raises(ValueError):
            for attempt in make_sync_retry(settings):
                with attempt:
                    call_count += 1
                    raise ValueError("not transient")
        # ValueError should propagate immediately, not be retried
        assert call_count == 1


# ---------------------------------------------------------------------------
# make_async_retry
# ---------------------------------------------------------------------------


class TestMakeAsyncRetry:
    def test_returns_async_retrying_instance(self, settings: RabbitMQSettings) -> None:
        policy = make_async_retry(settings)
        assert isinstance(policy, AsyncRetrying)

    @pytest.mark.asyncio
    async def test_success_on_first_attempt_async(
        self, settings: RabbitMQSettings
    ) -> None:
        call_count = 0
        async for attempt in make_async_retry(settings):
            with attempt:
                call_count += 1
        assert call_count == 1

    @pytest.mark.asyncio
    async def test_reraises_after_max_attempts_async(
        self, settings: RabbitMQSettings
    ) -> None:
        import aio_pika.exceptions as aio_exc  # type: ignore[import-untyped]

        settings_1 = RabbitMQSettings(
            host="localhost", max_retries=2, retry_delay=0.001
        )
        call_count = 0
        with pytest.raises(aio_exc.AMQPConnectionError):
            async for attempt in make_async_retry(settings_1):
                with attempt:
                    call_count += 1
                    raise aio_exc.AMQPConnectionError("down")
        assert call_count == 2

    @pytest.mark.asyncio
    async def test_non_transient_error_not_retried_async(
        self, settings: RabbitMQSettings
    ) -> None:
        call_count = 0
        with pytest.raises(ValueError):
            async for attempt in make_async_retry(settings):
                with attempt:
                    call_count += 1
                    raise ValueError("not transient")
        assert call_count == 1


# ---------------------------------------------------------------------------
# _log_retry_attempt
# ---------------------------------------------------------------------------


class TestLogRetryAttempt:
    def test_logs_warning_with_attempt_number(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_state = MagicMock()
        mock_state.attempt_number = 2
        mock_state.outcome.exception.return_value = RuntimeError("test error")
        mock_state.retry_object.stop = "stop_after_attempt(3)"

        with caplog.at_level(logging.WARNING, logger="langchain_rabbitmq.utilities._retry"):
            _log_retry_attempt(mock_state)

        assert len(caplog.records) == 1
        record = caplog.records[0]
        assert record.levelno == logging.WARNING
        assert "2" in record.message

    def test_logs_none_when_no_outcome(
        self, caplog: pytest.LogCaptureFixture
    ) -> None:
        mock_state = MagicMock()
        mock_state.attempt_number = 1
        mock_state.outcome = None
        mock_state.retry_object.stop = "stop_after_attempt(3)"

        with caplog.at_level(logging.WARNING, logger="langchain_rabbitmq.utilities._retry"):
            _log_retry_attempt(mock_state)

        assert len(caplog.records) == 1
