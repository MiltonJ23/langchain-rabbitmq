"""LangChain tools for RabbitMQ queue management.

Provides six structured tools that an AI agent can use to manage the full
lifecycle of AMQP queues: declare, delete, purge, bind, unbind, and inspect.

Each tool accepts a :class:`~langchain_rabbitmq.config.RabbitMQSettings`
instance at construction (defaulting to environment-variable-based config)
and uses a short-lived connection per invocation — no shared state.

Example:
    Registering all queue tools with an agent::

        from langchain_rabbitmq.tools.queue import QUEUE_TOOLS
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings(host="broker.internal")
        tools = [cls(settings=settings) for cls in QUEUE_TOOLS]

    Using a single tool directly::

        from langchain_rabbitmq.tools.queue import DeclareQueueTool

        tool = DeclareQueueTool()
        result = tool.invoke({"name": "orders", "durable": True})
        print(result)  # Queue 'orders' declared. messages=0, consumers=0
"""

from __future__ import annotations

from typing import Any, Optional, Type

from pydantic import BaseModel, Field

from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.utilities._models import ExchangeType

# ---------------------------------------------------------------------------
# Input schemas
# ---------------------------------------------------------------------------


class _DeclareQueueInput(BaseModel):
    """Input schema for :class:`DeclareQueueTool`.

    Attributes:
        name: Queue name.  Pass an empty string to request a broker-assigned
            unique name.
        durable: Survive broker restarts when ``True``.
        exclusive: Restrict access to the declaring connection.
        auto_delete: Delete automatically when the last consumer disconnects.
        arguments: Optional AMQP x-arguments, e.g.
            ``{"x-message-ttl": 60000}`` for a 60-second message TTL.
    """

    name: str = Field(description="Queue name. Use empty string for a server-named queue.")
    durable: bool = Field(default=False, description="Survive broker restarts.")
    exclusive: bool = Field(default=False, description="Restrict to this connection only.")
    auto_delete: bool = Field(
        default=False, description="Delete when the last consumer disconnects."
    )
    arguments: dict[str, Any] = Field(
        default_factory=dict,
        description='Optional AMQP x-arguments, e.g. {"x-message-ttl": 60000}.',
    )


class _DeleteQueueInput(BaseModel):
    """Input schema for :class:`DeleteQueueTool`.

    Attributes:
        name: Name of the queue to delete.
        if_unused: Only delete if no consumers are attached.
        if_empty: Only delete if the queue contains no messages.
    """

    name: str = Field(description="Name of the queue to delete.")
    if_unused: bool = Field(
        default=False, description="Refuse to delete if consumers are attached."
    )
    if_empty: bool = Field(
        default=False, description="Refuse to delete if messages remain in the queue."
    )


class _PurgeQueueInput(BaseModel):
    """Input schema for :class:`PurgeQueueTool`.

    Attributes:
        name: Name of the queue to purge.
    """

    name: str = Field(description="Name of the queue to purge of all ready messages.")


class _BindQueueInput(BaseModel):
    """Input schema for :class:`BindQueueTool`.

    Attributes:
        queue: Destination queue name.
        exchange: Source exchange name.
        routing_key: Binding routing key (pattern for topic exchanges).
        arguments: Optional binding arguments (headers exchange).
    """

    queue: str = Field(description="Destination queue name.")
    exchange: str = Field(description="Source exchange name to bind from.")
    routing_key: str = Field(
        default="",
        description="Routing key or pattern. Use '#' to match everything on topic exchanges.",
    )
    arguments: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional binding arguments used by headers exchanges.",
    )


class _UnbindQueueInput(BaseModel):
    """Input schema for :class:`UnbindQueueTool`.

    Attributes:
        queue: Queue name.
        exchange: Exchange name.
        routing_key: Binding routing key that was used when binding.
        arguments: Binding arguments used when binding.
    """

    queue: str = Field(description="Queue name to unbind.")
    exchange: str = Field(description="Exchange name to unbind from.")
    routing_key: str = Field(
        default="", description="Routing key that was used when binding."
    )
    arguments: dict[str, Any] = Field(default_factory=dict)


class _GetQueueInfoInput(BaseModel):
    """Input schema for :class:`GetQueueInfoTool`.

    Attributes:
        name: Queue name to inspect.
    """

    name: str = Field(description="Name of the queue to inspect.")


# ---------------------------------------------------------------------------
# Tool classes
# ---------------------------------------------------------------------------


class DeclareQueueTool(_RabbitMQBaseTool):
    """Declare an AMQP queue on the broker.

    Use this tool to create a new queue or verify that an existing queue
    matches the given parameters.  If the queue already exists with different
    durability or exclusivity parameters the broker will return an error.

    Returns a summary string with the resolved queue name and current message
    and consumer counts.

    Example agent call::

        tool.invoke({
            "name": "orders",
            "durable": true,
            "arguments": {"x-message-ttl": 86400000}
        })
    """

    name: str = "rabbitmq_declare_queue"
    description: str = (
        "Declare (create) a queue on the RabbitMQ broker. "
        "Use this before publishing to or consuming from a queue that may not exist. "
        "Returns the resolved queue name and current message/consumer counts. "
        "Set durable=true for queues that must survive a broker restart."
    )
    args_schema: Type[BaseModel] = _DeclareQueueInput

    def _execute(  # type: ignore[override]
        self,
        name: str,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        """Declare the queue and return a summary string.

        Args:
            name: Queue name.
            durable: Survive broker restarts.
            exclusive: Restrict to declaring connection.
            auto_delete: Delete when last consumer disconnects.
            arguments: AMQP x-arguments.
            **_: Ignored extra kwargs.

        Returns:
            Human-readable confirmation including resolved name and counts.
        """
        with self._make_client() as client:
            info = client.declare_queue(
                name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
        return (
            f"Queue '{info.name}' declared successfully. "
            f"durable={info.durable}, exclusive={info.exclusive}, "
            f"auto_delete={info.auto_delete}, "
            f"messages={info.message_count}, consumers={info.consumer_count}."
        )

    async def _aexecute(  # type: ignore[override]
        self,
        name: str,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            info = await client.declare_queue(
                name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
        return (
            f"Queue '{info.name}' declared successfully. "
            f"durable={info.durable}, exclusive={info.exclusive}, "
            f"auto_delete={info.auto_delete}, "
            f"messages={info.message_count}, consumers={info.consumer_count}."
        )


class DeleteQueueTool(_RabbitMQBaseTool):
    """Delete a queue from the broker.

    Use this tool to permanently remove a queue and all its messages.
    Set ``if_unused=true`` or ``if_empty=true`` for safe deletion.

    Returns the number of messages that were discarded.

    Example agent call::

        tool.invoke({"name": "old-queue", "if_empty": true})
    """

    name: str = "rabbitmq_delete_queue"
    description: str = (
        "Delete a queue from the RabbitMQ broker. "
        "All messages in the queue are discarded. "
        "Use if_unused=true to prevent deletion while consumers are active. "
        "Use if_empty=true to prevent deletion when messages are still present. "
        "Returns the number of messages that were discarded."
    )
    args_schema: Type[BaseModel] = _DeleteQueueInput

    def _execute(  # type: ignore[override]
        self,
        name: str,
        if_unused: bool = False,
        if_empty: bool = False,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            discarded = client.delete_queue(name, if_unused=if_unused, if_empty=if_empty)
        return f"Queue '{name}' deleted. Messages discarded: {discarded}."

    async def _aexecute(  # type: ignore[override]
        self,
        name: str,
        if_unused: bool = False,
        if_empty: bool = False,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            discarded = await client.delete_queue(
                name, if_unused=if_unused, if_empty=if_empty
            )
        return f"Queue '{name}' deleted. Messages discarded: {discarded}."


class PurgeQueueTool(_RabbitMQBaseTool):
    """Remove all ready messages from a queue without deleting the queue itself.

    Use this when you want to drain a queue but keep its declaration and
    bindings intact.

    Returns the number of messages removed.

    Example agent call::

        tool.invoke({"name": "dead-letter"})
    """

    name: str = "rabbitmq_purge_queue"
    description: str = (
        "Remove all ready messages from a RabbitMQ queue without deleting the queue. "
        "Bindings and the queue declaration are preserved. "
        "Returns the number of messages that were removed."
    )
    args_schema: Type[BaseModel] = _PurgeQueueInput

    def _execute(self, name: str, **_: Any) -> str:  # type: ignore[override]
        with self._make_client() as client:
            removed = client.purge_queue(name)
        return f"Queue '{name}' purged. Messages removed: {removed}."

    async def _aexecute(self, name: str, **_: Any) -> str:  # type: ignore[override]
        async with self._make_async_client() as client:
            removed = await client.purge_queue(name)
        return f"Queue '{name}' purged. Messages removed: {removed}."


class BindQueueTool(_RabbitMQBaseTool):
    """Bind a queue to an exchange with an optional routing key.

    Messages published to the exchange will be routed to the queue when the
    routing key matches.  For fanout exchanges the routing key is ignored.

    Example agent call::

        tool.invoke({
            "queue": "orders",
            "exchange": "events",
            "routing_key": "order.created"
        })
    """

    name: str = "rabbitmq_bind_queue"
    description: str = (
        "Bind a RabbitMQ queue to an exchange so messages published to that exchange "
        "are routed to the queue. "
        "Specify routing_key for direct/topic exchanges "
        "(use '#' to match all routing keys on a topic exchange). "
        "For fanout exchanges the routing_key is ignored."
    )
    args_schema: Type[BaseModel] = _BindQueueInput

    def _execute(  # type: ignore[override]
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            client.bind_queue(queue, exchange, routing_key, arguments)
        key_desc = f" with routing_key='{routing_key}'" if routing_key else ""
        return f"Queue '{queue}' bound to exchange '{exchange}'{key_desc}."

    async def _aexecute(  # type: ignore[override]
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            await client.bind_queue(queue, exchange, routing_key, arguments)
        key_desc = f" with routing_key='{routing_key}'" if routing_key else ""
        return f"Queue '{queue}' bound to exchange '{exchange}'{key_desc}."


class UnbindQueueTool(_RabbitMQBaseTool):
    """Remove a binding between a queue and an exchange.

    After unbinding, messages published to the exchange will no longer be
    routed to the queue for the given routing key.

    Example agent call::

        tool.invoke({
            "queue": "orders",
            "exchange": "events",
            "routing_key": "order.created"
        })
    """

    name: str = "rabbitmq_unbind_queue"
    description: str = (
        "Remove a binding between a RabbitMQ queue and an exchange. "
        "After this call, messages with the specified routing key will no longer "
        "be delivered to the queue from that exchange."
    )
    args_schema: Type[BaseModel] = _UnbindQueueInput

    def _execute(  # type: ignore[override]
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            client.unbind_queue(queue, exchange, routing_key, arguments)
        key_desc = f" (routing_key='{routing_key}')" if routing_key else ""
        return f"Queue '{queue}' unbound from exchange '{exchange}'{key_desc}."

    async def _aexecute(  # type: ignore[override]
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            await client.unbind_queue(queue, exchange, routing_key, arguments)
        key_desc = f" (routing_key='{routing_key}')" if routing_key else ""
        return f"Queue '{queue}' unbound from exchange '{exchange}'{key_desc}."


class GetQueueInfoTool(_RabbitMQBaseTool):
    """Inspect an existing queue without modifying it.

    Returns the current state of the queue including message count, consumer
    count, and configuration flags.

    Example agent call::

        tool.invoke({"name": "orders"})
        # → "Queue 'orders': messages=42, consumers=2, durable=True, ..."
    """

    name: str = "rabbitmq_get_queue_info"
    description: str = (
        "Inspect a RabbitMQ queue to get its current status: "
        "number of ready messages, active consumers, durability, and configuration. "
        "Returns an error if the queue does not exist."
    )
    args_schema: Type[BaseModel] = _GetQueueInfoInput

    def _execute(self, name: str, **_: Any) -> str:  # type: ignore[override]
        with self._make_client() as client:
            info = client.get_queue_info(name)
        return (
            f"Queue '{info.name}': "
            f"messages={info.message_count}, "
            f"consumers={info.consumer_count}, "
            f"durable={info.durable}, "
            f"exclusive={info.exclusive}, "
            f"auto_delete={info.auto_delete}."
        )

    async def _aexecute(self, name: str, **_: Any) -> str:  # type: ignore[override]
        async with self._make_async_client() as client:
            info = await client.get_queue_info(name)
        return (
            f"Queue '{info.name}': "
            f"messages={info.message_count}, "
            f"consumers={info.consumer_count}, "
            f"durable={info.durable}, "
            f"exclusive={info.exclusive}, "
            f"auto_delete={info.auto_delete}."
        )


# Ordered list used by toolkit / test factories
QUEUE_TOOLS: list[type[_RabbitMQBaseTool]] = [
    DeclareQueueTool,
    DeleteQueueTool,
    PurgeQueueTool,
    BindQueueTool,
    UnbindQueueTool,
    GetQueueInfoTool,
]

__all__: list[str] = [
    "DeclareQueueTool",
    "DeleteQueueTool",
    "PurgeQueueTool",
    "BindQueueTool",
    "UnbindQueueTool",
    "GetQueueInfoTool",
    "QUEUE_TOOLS",
]
