"""Unit tests for Pydantic response models."""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from langchain_rabbitmq.utilities._models import (
    BindingInfo,
    ConnectionInfo,
    ExchangeInfo,
    ExchangeType,
    HealthInfo,
    HealthStatus,
    MessageResult,
    QueueInfo,
)

pytestmark = pytest.mark.unit



class TestExchangeType:
    def test_values(self) -> None:
        assert ExchangeType.DIRECT.value == "direct"
        assert ExchangeType.FANOUT.value == "fanout"
        assert ExchangeType.TOPIC.value == "topic"
        assert ExchangeType.HEADERS.value == "headers"

    def test_is_str(self) -> None:
        assert isinstance(ExchangeType.DIRECT, str)


class TestQueueInfo:
    def test_construction(self) -> None:
        q = QueueInfo(
            name="jobs",
            durable=True,
            exclusive=False,
            auto_delete=False,
        )
        assert q.name == "jobs"
        assert q.durable is True
        assert q.message_count == 0
        assert q.consumer_count == 0
        assert q.arguments == {}

    def test_with_counts(self) -> None:
        q = QueueInfo(
            name="q",
            durable=False,
            exclusive=True,
            auto_delete=True,
            message_count=10,
            consumer_count=2,
        )
        assert q.message_count == 10
        assert q.consumer_count == 2

    def test_frozen(self) -> None:
        q = QueueInfo(name="q", durable=False, exclusive=False, auto_delete=False)
        with pytest.raises(ValidationError):
            q.name = "new"  # type: ignore[misc]

    def test_extra_fields_forbidden(self) -> None:
        with pytest.raises(ValidationError):
            QueueInfo(  # type: ignore[call-arg]
                name="q",
                durable=False,
                exclusive=False,
                auto_delete=False,
                unknown_field="x",
            )


class TestExchangeInfo:
    def test_construction(self) -> None:
        e = ExchangeInfo(
            name="events",
            exchange_type=ExchangeType.TOPIC,
            durable=True,
            auto_delete=False,
        )
        assert e.name == "events"
        assert e.exchange_type == ExchangeType.TOPIC
        assert e.durable is True

    def test_arguments_default_empty(self) -> None:
        e = ExchangeInfo(
            name="x",
            exchange_type=ExchangeType.DIRECT,
            durable=False,
            auto_delete=False,
        )
        assert e.arguments == {}


class TestMessageResult:
    def test_construction(self) -> None:
        msg = MessageResult(
            body=b"hello",
            delivery_tag=1,
            exchange="",
            routing_key="orders",
            redelivered=False,
        )
        assert msg.body == b"hello"
        assert msg.delivery_tag == 1
        assert msg.redelivered is False
        assert msg.headers == {}
        assert msg.content_type is None

    def test_with_all_fields(self) -> None:
        msg = MessageResult(
            body=b"{}",
            delivery_tag=99,
            exchange="events",
            routing_key="order.created",
            redelivered=True,
            headers={"x-retry": 2},
            content_type="application/json",
            content_encoding="utf-8",
        )
        assert msg.headers == {"x-retry": 2}
        assert msg.content_type == "application/json"
        assert msg.content_encoding == "utf-8"


class TestBindingInfo:
    def test_construction(self) -> None:
        b = BindingInfo(
            source="events",
            destination="orders",
            destination_type="queue",
            routing_key="order.*",
        )
        assert b.source == "events"
        assert b.destination_type == "queue"
        assert b.arguments == {}


class TestConnectionInfo:
    def test_connected_default(self) -> None:
        c = ConnectionInfo(
            host="localhost",
            port=5672,
            virtual_host="/",
        )
        assert c.connected is True
        assert c.server_version is None

    def test_disconnected(self) -> None:
        c = ConnectionInfo(
            host="localhost",
            port=5672,
            virtual_host="/",
            connected=False,
        )
        assert c.connected is False


class TestHealthInfo:
    def test_ok_status(self) -> None:
        h = HealthInfo(status=HealthStatus.OK, host="localhost", port=5672)
        assert h.status == HealthStatus.OK
        assert h.message == ""

    def test_down_status(self) -> None:
        h = HealthInfo(
            status=HealthStatus.DOWN,
            host="localhost",
            port=5672,
            message="Connection refused",
        )
        assert h.status == HealthStatus.DOWN
        assert "refused" in h.message

    def test_degraded_status(self) -> None:
        h = HealthInfo(status=HealthStatus.DEGRADED, host="h", port=1, message="probe failed")
        assert h.status == HealthStatus.DEGRADED


class TestHealthStatus:
    def test_values(self) -> None:
        assert HealthStatus.OK.value == "ok"
        assert HealthStatus.DEGRADED.value == "degraded"
        assert HealthStatus.DOWN.value == "down"
