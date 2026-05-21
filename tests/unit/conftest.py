"""Shared fixtures for unit tests.

All fixtures in this module mock the external broker (pika, aio-pika, httpx)
so tests run without a live RabbitMQ instance.
"""

from __future__ import annotations

from unittest.mock import MagicMock, patch

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.utilities._models import (
    ConnectionInfo,
    ExchangeInfo,
    ExchangeType,
    HealthInfo,
    HealthStatus,
    MessageResult,
    QueueInfo,
)

pytestmark = pytest.mark.unit

# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------


@pytest.fixture()
def settings() -> RabbitMQSettings:
    """Return default settings pointing at localhost (no env var side-effects)."""
    return RabbitMQSettings(
        host="localhost",
        port=5672,
        username="guest",
        password="guest",  # type: ignore[arg-type]
    )


# ---------------------------------------------------------------------------
# Pika mock building blocks
# ---------------------------------------------------------------------------


def _make_queue_declare_result(
    name: str = "test-queue",
    message_count: int = 0,
    consumer_count: int = 0,
) -> MagicMock:
    """Return a mock pika ``Queue.DeclareOk`` frame."""
    result = MagicMock()
    result.method.queue = name
    result.method.message_count = message_count
    result.method.consumer_count = consumer_count
    return result


def _make_queue_delete_result(message_count: int = 0) -> MagicMock:
    result = MagicMock()
    result.method.message_count = message_count
    return result


def _make_queue_purge_result(message_count: int = 5) -> MagicMock:
    result = MagicMock()
    result.method.message_count = message_count
    return result


def _make_basic_get_result(
    body: bytes = b"hello",
    delivery_tag: int = 1,
    exchange: str = "",
    routing_key: str = "test",
    redelivered: bool = False,
) -> tuple[MagicMock, MagicMock, bytes]:
    method = MagicMock()
    method.delivery_tag = delivery_tag
    method.exchange = exchange
    method.routing_key = routing_key
    method.redelivered = redelivered
    props = MagicMock()
    props.headers = {}
    props.content_type = "text/plain"
    props.content_encoding = "utf-8"
    return method, props, body


@pytest.fixture()
def mock_channel() -> MagicMock:
    """Return a fully-stubbed pika blocking channel."""
    ch = MagicMock()
    ch.is_open = True

    # Queue operations
    ch.queue_declare.return_value = _make_queue_declare_result()
    ch.queue_delete.return_value = _make_queue_delete_result()
    ch.queue_purge.return_value = _make_queue_purge_result()
    ch.queue_bind.return_value = None
    ch.queue_unbind.return_value = None

    # Exchange operations
    ch.exchange_declare.return_value = None
    ch.exchange_delete.return_value = None
    ch.exchange_bind.return_value = None

    # Message operations
    method, props, body = _make_basic_get_result()
    ch.basic_get.return_value = (method, props, body)
    ch.basic_publish.return_value = None
    ch.basic_ack.return_value = None
    ch.basic_nack.return_value = None
    ch.basic_reject.return_value = None

    # Confirms
    ch.confirm_delivery.return_value = None
    return ch


@pytest.fixture()
def mock_connection(mock_channel: MagicMock) -> MagicMock:
    """Return a stubbed pika BlockingConnection."""
    conn = MagicMock()
    conn.is_open = True
    conn.channel.return_value = mock_channel
    conn.server_properties = {
        "version": b"3.12.4",
        "platform": b"Erlang/OTP 26.1",
    }
    return conn


@pytest.fixture()
def patched_pika(mock_connection: MagicMock) -> MagicMock:
    """Patch ``pika.BlockingConnection`` for the entire test."""
    with patch(
        "langchain_rabbitmq.utilities.rabbitmq.pika.BlockingConnection",
        return_value=mock_connection,
    ) as p:
        yield p


# ---------------------------------------------------------------------------
# Pre-built model instances (reusable across tool tests)
# ---------------------------------------------------------------------------


@pytest.fixture()
def queue_info() -> QueueInfo:
    return QueueInfo(
        name="orders",
        durable=True,
        exclusive=False,
        auto_delete=False,
        message_count=3,
        consumer_count=1,
    )


@pytest.fixture()
def exchange_info() -> ExchangeInfo:
    return ExchangeInfo(
        name="events",
        exchange_type=ExchangeType.TOPIC,
        durable=True,
        auto_delete=False,
    )


@pytest.fixture()
def message_result() -> MessageResult:
    return MessageResult(
        body=b'{"id": 1}',
        delivery_tag=42,
        exchange="",
        routing_key="orders",
        redelivered=False,
        content_type="application/json",
    )


@pytest.fixture()
def health_ok() -> HealthInfo:
    return HealthInfo(
        status=HealthStatus.OK,
        host="localhost",
        port=5672,
        message="Broker connection healthy",
    )


@pytest.fixture()
def conn_info() -> ConnectionInfo:
    return ConnectionInfo(
        host="localhost",
        port=5672,
        virtual_host="/",
        server_version="3.12.4",
        server_platform="Erlang/OTP 26.1",
        connected=True,
    )
