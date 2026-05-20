"""Pydantic response models returned by the RabbitMQ client classes.

These models are the structured output of every broker operation and are
also consumed by the LangChain tool layer to build agent-readable strings.

Example:
    Inspecting a queue response::

        info: QueueInfo = client.declare_queue("orders")
        print(info.message_count)  # 0
"""

from __future__ import annotations

from enum import Enum
from typing import Any, Optional

from pydantic import BaseModel, ConfigDict, Field


class ExchangeType(str, Enum):
    """Standard AMQP 0-9-1 exchange types.

    Attributes:
        DIRECT: Routes to the queue whose binding key matches the routing key.
        FANOUT: Broadcasts to all bound queues regardless of routing key.
        TOPIC: Routes using ``*`` / ``#`` wildcard pattern matching.
        HEADERS: Routes based on message header attributes, not routing key.
    """

    DIRECT = "direct"
    FANOUT = "fanout"
    TOPIC = "topic"
    HEADERS = "headers"


class _ImmutableModel(BaseModel):
    """Shared base: frozen, no extra fields allowed."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class QueueInfo(_ImmutableModel):
    """Metadata returned after declaring or passively inspecting a queue.

    Attributes:
        name: Resolved queue name.  For anonymous queues (empty name
            requested) this is the server-assigned identifier.
        durable: Survives a broker restart.
        exclusive: Exclusive to the declaring connection.
        auto_delete: Deleted when all consumers disconnect.
        message_count: Messages currently ready in the queue.
        consumer_count: Active consumer count.
        arguments: AMQP x-arguments (e.g. ``{"x-message-ttl": 60000}``).
    """

    name: str
    durable: bool
    exclusive: bool
    auto_delete: bool
    message_count: int = 0
    consumer_count: int = 0
    arguments: dict[str, Any] = Field(default_factory=dict)


class ExchangeInfo(_ImmutableModel):
    """Metadata for a declared exchange.

    Attributes:
        name: Exchange name.
        exchange_type: AMQP exchange type.
        durable: Survives a broker restart.
        auto_delete: Deleted when the last queue unbinds.
        arguments: AMQP x-arguments.
    """

    name: str
    exchange_type: ExchangeType
    durable: bool
    auto_delete: bool
    arguments: dict[str, Any] = Field(default_factory=dict)


class MessageResult(_ImmutableModel):
    """A single message pulled from a queue via ``basic.get``.

    Attributes:
        body: Raw message bytes.
        delivery_tag: Broker delivery tag — required for ack/nack/reject.
        exchange: Exchange that routed the message (empty = default exchange).
        routing_key: Routing key used when publishing.
        redelivered: ``True`` if the broker previously attempted delivery.
        headers: AMQP headers table.
        content_type: MIME type declared by the publisher.
        content_encoding: Content encoding (e.g. ``"utf-8"``).
    """

    body: bytes
    delivery_tag: int
    exchange: str
    routing_key: str
    redelivered: bool
    headers: dict[str, Any] = Field(default_factory=dict)
    content_type: Optional[str] = None
    content_encoding: Optional[str] = None


class BindingInfo(_ImmutableModel):
    """An AMQP binding between a source exchange and a queue or exchange.

    Attributes:
        source: Source exchange name.
        destination: Destination queue or exchange name.
        destination_type: ``"queue"`` or ``"exchange"``.
        routing_key: Binding routing key.
        arguments: Optional binding arguments (headers exchange).
    """

    source: str
    destination: str
    destination_type: str
    routing_key: str
    arguments: dict[str, Any] = Field(default_factory=dict)


class ConnectionInfo(_ImmutableModel):
    """Active AMQP connection metadata.

    Attributes:
        host: Broker hostname.
        port: AMQP port.
        virtual_host: Connected virtual host.
        server_version: Broker version string, if reported.
        server_platform: Broker platform string, if reported.
        connected: Whether the connection is currently open.
    """

    host: str
    port: int
    virtual_host: str
    server_version: Optional[str] = None
    server_platform: Optional[str] = None
    connected: bool = True


class HealthStatus(str, Enum):
    """Broker health classification.

    Attributes:
        OK: Connection open and broker responding normally.
        DEGRADED: Connected but a probe operation returned unexpected results.
        DOWN: Connection closed or broker unreachable.
    """

    OK = "ok"
    DEGRADED = "degraded"
    DOWN = "down"


class HealthInfo(_ImmutableModel):
    """Result of a broker health probe.

    Attributes:
        status: Overall health classification.
        host: Broker hostname that was probed.
        port: Broker port that was probed.
        message: Human-readable description of the probe result.
    """

    status: HealthStatus
    host: str
    port: int
    message: str = ""


__all__: list[str] = [
    "BindingInfo",
    "ConnectionInfo",
    "ExchangeInfo",
    "ExchangeType",
    "HealthInfo",
    "HealthStatus",
    "MessageResult",
    "QueueInfo",
]
