"""LangChain tools for RabbitMQ message operations.

Provides five structured tools covering the full message lifecycle:
publish, consume (pull/one-off), acknowledge, negatively acknowledge, and reject.

Design notes
------------
* **Publish / Consume** are stateless — each invocation opens and closes its
  own AMQP connection.  They are safe for standard LangChain agent use.

* **Ack / Nack / Reject** require the *same channel* that delivered the
  message.  In a standard agent loop where each tool call is a separate
  connection, these are most practical when ``auto_ack=True`` is used on
  consume (the default).  For worker patterns that need manual acknowledgement
  inject a long-lived :class:`~langchain_rabbitmq.utilities.rabbitmq.RabbitMQClient`
  via a custom subclass.

Example:
    Publish then consume with auto-ack::

        from langchain_rabbitmq.tools.message import PublishMessageTool, ConsumeMessageTool

        pub = PublishMessageTool()
        pub.invoke({"exchange": "", "routing_key": "jobs", "body": '{"task": "send_email"}'})

        con = ConsumeMessageTool()
        result = con.invoke({"queue": "jobs"})
        print(result)
"""

from __future__ import annotations

import json
from typing import Any, Optional

from pydantic import BaseModel, Field

from langchain_rabbitmq.tools._base import _RabbitMQBaseTool

# ---------------------------------------------------------------------------
# Input schemas
# ---------------------------------------------------------------------------


class _PublishMessageInput(BaseModel):
    """Input schema for :class:`PublishMessageTool`.

    Attributes:
        exchange: Target exchange name.  Use empty string ``""`` to publish
            directly to a queue via the default exchange.
        routing_key: Routing key (or queue name when using the default
            exchange).
        body: Message payload as a UTF-8 string.  JSON objects should be
            serialised to a string before passing.
        persistent: Delivery mode 2 — message survives a broker restart.
        content_type: MIME type header (e.g. ``"application/json"``).
        headers: Optional AMQP headers table as a JSON-serialisable dict.
        expiration_ms: Per-message TTL in milliseconds.  ``0`` means no TTL.
    """

    exchange: str = Field(
        default="",
        description=(
            "Target exchange name. "
            "Use empty string '' to publish directly to a queue (default exchange). "
            "The queue name is then used as the routing_key."
        ),
    )
    routing_key: str = Field(
        description=(
            "Routing key for the message. When exchange is '' this is the destination queue name."
        )
    )
    body: str = Field(
        description=(
            "Message payload as a UTF-8 string. "
            "For JSON payloads pass the serialised JSON string, "
            'e.g. \'{"event": "order.created", "id": 42}\'.'
        )
    )
    persistent: bool = Field(
        default=False,
        description="Set True to use delivery mode 2 (message survives broker restart).",
    )
    content_type: str = Field(
        default="text/plain",
        description='MIME type of the body, e.g. "application/json" or "text/plain".',
    )
    headers: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional AMQP headers table as a key-value dict.",
    )
    expiration_ms: int = Field(
        default=0,
        ge=0,
        description="Per-message TTL in milliseconds. 0 means no expiration.",
    )


class _ConsumeMessageInput(BaseModel):
    """Input schema for :class:`ConsumeMessageTool`.

    Attributes:
        queue: Name of the queue to pull from.
        auto_ack: Automatically acknowledge the message on delivery.
            Set to ``False`` only when the agent can issue a follow-up
            ack/nack/reject call within the same AMQP connection session.
            Defaults to ``True`` for standard agent use.
        encoding: Codec used to decode the raw message bytes into a string.
            Defaults to ``"utf-8"``.
    """

    queue: str = Field(description="Name of the queue to pull a single message from.")
    auto_ack: bool = Field(
        default=True,
        description=(
            "Automatically acknowledge the message. "
            "Set False only if you intend to call rabbitmq_ack_message, "
            "rabbitmq_nack_message, or rabbitmq_reject_message in the same session."
        ),
    )
    encoding: str = Field(
        default="utf-8",
        description="Encoding used to decode the message body bytes to a string.",
    )


class _AckMessageInput(BaseModel):
    """Input schema for :class:`AckMessageTool`.

    Attributes:
        delivery_tag: The delivery tag returned by a previous consume call.
        multiple: Acknowledge all unacknowledged messages up to and
            including this delivery tag.
    """

    delivery_tag: int = Field(
        description="Delivery tag returned by rabbitmq_consume_message.",
        gt=0,
    )
    multiple: bool = Field(
        default=False,
        description="Acknowledge all pending messages up to and including this tag.",
    )


class _NackMessageInput(BaseModel):
    """Input schema for :class:`NackMessageTool`.

    Attributes:
        delivery_tag: Delivery tag from a previous consume call.
        multiple: Nack all messages up to and including this tag.
        requeue: Requeue the message(s).  Set ``False`` to discard or
            route to a dead-letter exchange.
    """

    delivery_tag: int = Field(
        description="Delivery tag returned by rabbitmq_consume_message.",
        gt=0,
    )
    multiple: bool = Field(default=False, description="Nack multiple messages.")
    requeue: bool = Field(
        default=True,
        description="Requeue the message. Set False to discard or dead-letter.",
    )


class _RejectMessageInput(BaseModel):
    """Input schema for :class:`RejectMessageTool`.

    Attributes:
        delivery_tag: Delivery tag from a previous consume call.
        requeue: Requeue the message.  Set ``False`` to discard.
    """

    delivery_tag: int = Field(
        description="Delivery tag returned by rabbitmq_consume_message.",
        gt=0,
    )
    requeue: bool = Field(
        default=True,
        description="Requeue the rejected message. Set False to discard.",
    )


# ---------------------------------------------------------------------------
# Tool classes
# ---------------------------------------------------------------------------


class PublishMessageTool(_RabbitMQBaseTool):
    """Publish a single message to a RabbitMQ exchange.

    For direct queue delivery, leave ``exchange`` as ``""`` and set
    ``routing_key`` to the target queue name.

    With publisher confirms enabled, this call blocks until the broker
    acknowledges receipt — ensuring the message was received before the
    tool returns.

    Example agent call (publish JSON to a queue)::

        tool.invoke({
            "exchange": "",
            "routing_key": "orders",
            "body": "{\"order_id\": 99, \"item\": \"book\"}",
            "content_type": "application/json",
            "persistent": true
        })

    Example agent call (publish to a topic exchange)::

        tool.invoke({
            "exchange": "events",
            "routing_key": "order.created.eu",
            "body": "New EU order",
            "expiration_ms": 60000
        })
    """

    name: str = "rabbitmq_publish_message"
    description: str = (
        "Publish a message to a RabbitMQ exchange or directly to a queue. "
        "To publish directly to a queue, set exchange='' and routing_key=<queue_name>. "
        "body must be a UTF-8 string (serialise JSON before passing). "
        "Set persistent=true for durable messages that survive broker restarts. "
        "Set expiration_ms to limit message lifetime (0 = no limit). "
        "Returns a confirmation string when the broker acknowledges receipt."
    )
    args_schema: type[BaseModel] = _PublishMessageInput

    def _execute(  # type: ignore[override]
        self,
        routing_key: str,
        body: str,
        exchange: str = "",
        persistent: bool = False,
        content_type: str = "text/plain",
        headers: Optional[dict[str, Any]] = None,
        expiration_ms: int = 0,
        **_: Any,
    ) -> str:
        raw_body = body.encode("utf-8")
        expiration = str(expiration_ms) if expiration_ms > 0 else None
        with self._make_client() as client:
            client.publish_message(
                exchange=exchange,
                routing_key=routing_key,
                body=raw_body,
                persistent=persistent,
                content_type=content_type,
                headers=headers or {},
                expiration=expiration,
            )
        dest = f"queue '{routing_key}'" if exchange == "" else f"exchange '{exchange}'"
        return (
            f"Message published to {dest} "
            f"(routing_key='{routing_key}', "
            f"persistent={persistent}, "
            f"size={len(raw_body)} bytes). Broker acknowledged."
        )

    async def _aexecute(  # type: ignore[override]
        self,
        routing_key: str,
        body: str,
        exchange: str = "",
        persistent: bool = False,
        content_type: str = "text/plain",
        headers: Optional[dict[str, Any]] = None,
        expiration_ms: int = 0,
        **_: Any,
    ) -> str:
        raw_body = body.encode("utf-8")
        expiration_int = expiration_ms if expiration_ms > 0 else None
        async with self._make_async_client() as client:
            await client.publish_message(
                exchange=exchange,
                routing_key=routing_key,
                body=raw_body,
                persistent=persistent,
                content_type=content_type,
                headers=headers or {},
                expiration=expiration_int,
            )
        dest = f"queue '{routing_key}'" if exchange == "" else f"exchange '{exchange}'"
        return (
            f"Message published to {dest} "
            f"(routing_key='{routing_key}', "
            f"persistent={persistent}, "
            f"size={len(raw_body)} bytes). Broker acknowledged."
        )


class ConsumeMessageTool(_RabbitMQBaseTool):
    """Pull a single message from a RabbitMQ queue (basic.get).

    Returns the decoded message body along with metadata including the
    delivery tag, routing key, and headers.  If the queue is empty, returns
    an explicit "no messages" response so the agent can retry or stop.

    With ``auto_ack=True`` (the default) the message is immediately removed
    from the queue upon delivery.  Set ``auto_ack=False`` only if you intend
    to call ``rabbitmq_ack_message``, ``rabbitmq_nack_message``, or
    ``rabbitmq_reject_message`` within the same AMQP connection session.

    Example agent call::

        tool.invoke({"queue": "orders"})
        # Returns: delivery_tag=1, body='{"order_id": 99}', ...
    """

    name: str = "rabbitmq_consume_message"
    description: str = (
        "Pull (consume) a single message from a RabbitMQ queue. "
        "Returns the message body (decoded as UTF-8 string), delivery_tag, "
        "routing_key, exchange, and headers. "
        "Returns 'No messages available' if the queue is empty — the agent should "
        "retry later or stop polling. "
        "auto_ack=True (default) is recommended for standard agent use."
    )
    args_schema: type[BaseModel] = _ConsumeMessageInput

    def _execute(  # type: ignore[override]
        self,
        queue: str,
        auto_ack: bool = True,
        encoding: str = "utf-8",
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            result = client.consume_message(queue, auto_ack=auto_ack)

        if result is None:
            return f"No messages available in queue '{queue}'."

        body_str = result.body.decode(encoding, errors="replace")

        # Try pretty-printing if the body looks like JSON.
        display_body = body_str
        try:
            parsed = json.loads(body_str)
            display_body = json.dumps(parsed, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            pass

        parts = [
            f"Message received from queue '{queue}':",
            f"  delivery_tag={result.delivery_tag}",
            f"  exchange='{result.exchange}'",
            f"  routing_key='{result.routing_key}'",
            f"  redelivered={result.redelivered}",
        ]
        if result.content_type:
            parts.append(f"  content_type='{result.content_type}'")
        if result.headers:
            parts.append(f"  headers={result.headers}")
        parts.append(f"  body={display_body!r}")
        if not auto_ack:
            parts.append(
                f"  [NOTE] Message not auto-acknowledged. "
                f"Call rabbitmq_ack_message with delivery_tag={result.delivery_tag} "
                f"to acknowledge, or rabbitmq_nack_message to requeue/discard."
            )
        return "\n".join(parts)

    async def _aexecute(  # type: ignore[override]
        self,
        queue: str,
        auto_ack: bool = True,
        encoding: str = "utf-8",
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            result = await client.consume_message(queue, auto_ack=auto_ack)

        if result is None:
            return f"No messages available in queue '{queue}'."

        body_str = result.body.decode(encoding, errors="replace")

        display_body = body_str
        try:
            parsed = json.loads(body_str)
            display_body = json.dumps(parsed, ensure_ascii=False)
        except (json.JSONDecodeError, TypeError):
            pass

        parts = [
            f"Message received from queue '{queue}':",
            f"  delivery_tag={result.delivery_tag}",
            f"  exchange='{result.exchange}'",
            f"  routing_key='{result.routing_key}'",
            f"  redelivered={result.redelivered}",
        ]
        if result.content_type:
            parts.append(f"  content_type='{result.content_type}'")
        if result.headers:
            parts.append(f"  headers={result.headers}")
        parts.append(f"  body={display_body!r}")
        if not auto_ack:
            parts.append(
                f"  [NOTE] Message not auto-acknowledged. "
                f"Call rabbitmq_ack_message with delivery_tag={result.delivery_tag}."
            )
        return "\n".join(parts)


class AckMessageTool(_RabbitMQBaseTool):
    """Acknowledge a previously consumed message.

    Signals to the broker that the message has been successfully processed
    and can be removed from the queue.

    .. important::
        Ack/nack/reject operations must be issued on the **same AMQP channel**
        that delivered the message.  In standard agent usage, use
        ``auto_ack=True`` with :class:`ConsumeMessageTool` instead.

    Example agent call::

        tool.invoke({"delivery_tag": 1})
    """

    name: str = "rabbitmq_ack_message"
    description: str = (
        "Acknowledge a consumed RabbitMQ message, signalling successful processing. "
        "Requires the delivery_tag returned by rabbitmq_consume_message. "
        "IMPORTANT: This must be called on the same AMQP connection that consumed "
        "the message. For standard agent use, prefer auto_ack=True on consume."
    )
    args_schema: type[BaseModel] = _AckMessageInput

    def _execute(  # type: ignore[override]
        self,
        delivery_tag: int,
        multiple: bool = False,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            client.ack_message(delivery_tag, multiple=multiple)
        scope = f"all messages up to {delivery_tag}" if multiple else str(delivery_tag)
        return f"Acknowledged delivery_tag={scope}."

    async def _aexecute(  # type: ignore[override]
        self,
        delivery_tag: int,
        multiple: bool = False,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            await client.ack_message(delivery_tag, multiple=multiple)
        scope = f"all messages up to {delivery_tag}" if multiple else str(delivery_tag)
        return f"Acknowledged delivery_tag={scope}."


class NackMessageTool(_RabbitMQBaseTool):
    """Negatively acknowledge a consumed message.

    The message is returned to the queue (``requeue=True``) or discarded /
    sent to a dead-letter exchange (``requeue=False``).

    .. important::
        Must be called on the same AMQP connection that consumed the message.

    Example agent call::

        tool.invoke({"delivery_tag": 1, "requeue": false})
    """

    name: str = "rabbitmq_nack_message"
    description: str = (
        "Negatively acknowledge a RabbitMQ message. "
        "Use requeue=True to return the message to the queue for reprocessing. "
        "Use requeue=False to discard the message or route it to a dead-letter exchange. "
        "Requires delivery_tag from a prior rabbitmq_consume_message call on the same connection."
    )
    args_schema: type[BaseModel] = _NackMessageInput

    def _execute(  # type: ignore[override]
        self,
        delivery_tag: int,
        multiple: bool = False,
        requeue: bool = True,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            client.nack_message(delivery_tag, multiple=multiple, requeue=requeue)
        action = "requeued" if requeue else "discarded"
        return f"Nacked delivery_tag={delivery_tag}. Message {action}."

    async def _aexecute(  # type: ignore[override]
        self,
        delivery_tag: int,
        multiple: bool = False,
        requeue: bool = True,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            await client.nack_message(delivery_tag, multiple=multiple, requeue=requeue)
        action = "requeued" if requeue else "discarded"
        return f"Nacked delivery_tag={delivery_tag}. Message {action}."


class RejectMessageTool(_RabbitMQBaseTool):
    """Reject a single consumed message.

    Functionally equivalent to :class:`NackMessageTool` but rejects only
    one message and does not support the ``multiple`` flag.

    Example agent call::

        tool.invoke({"delivery_tag": 1, "requeue": false})
    """

    name: str = "rabbitmq_reject_message"
    description: str = (
        "Reject a single RabbitMQ message. "
        "Use requeue=True to return it to the queue. "
        "Use requeue=False to discard it or route to a dead-letter exchange. "
        "Requires delivery_tag from a prior rabbitmq_consume_message call on the same connection."
    )
    args_schema: type[BaseModel] = _RejectMessageInput

    def _execute(  # type: ignore[override]
        self,
        delivery_tag: int,
        requeue: bool = True,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            client.reject_message(delivery_tag, requeue=requeue)
        action = "requeued" if requeue else "discarded"
        return f"Rejected delivery_tag={delivery_tag}. Message {action}."

    async def _aexecute(  # type: ignore[override]
        self,
        delivery_tag: int,
        requeue: bool = True,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            await client.reject_message(delivery_tag, requeue=requeue)
        action = "requeued" if requeue else "discarded"
        return f"Rejected delivery_tag={delivery_tag}. Message {action}."


# Ordered list used by toolkit / test factories
MESSAGE_TOOLS: list[type[_RabbitMQBaseTool]] = [
    PublishMessageTool,
    ConsumeMessageTool,
    AckMessageTool,
    NackMessageTool,
    RejectMessageTool,
]

__all__: list[str] = [
    "MESSAGE_TOOLS",
    "AckMessageTool",
    "ConsumeMessageTool",
    "NackMessageTool",
    "PublishMessageTool",
    "RejectMessageTool",
]
