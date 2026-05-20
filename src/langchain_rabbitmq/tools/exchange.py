"""LangChain tools for RabbitMQ exchange management.

Provides three structured tools for the full exchange lifecycle:
declare, delete, and exchange-to-exchange binding.

Example:
    Registering all exchange tools with an agent::

        from langchain_rabbitmq.tools.exchange import EXCHANGE_TOOLS
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings(host="broker.internal")
        tools = [cls(settings=settings) for cls in EXCHANGE_TOOLS]

    Declaring a topic exchange directly::

        from langchain_rabbitmq.tools.exchange import DeclareExchangeTool

        tool = DeclareExchangeTool()
        result = tool.invoke({"name": "events", "exchange_type": "topic", "durable": True})
        print(result)  # Exchange 'events' (topic) declared. durable=True, auto_delete=False.
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.utilities._models import ExchangeType

# ---------------------------------------------------------------------------
# Input schemas
# ---------------------------------------------------------------------------


class _DeclareExchangeInput(BaseModel):
    """Input schema for :class:`DeclareExchangeTool`.

    Attributes:
        name: Exchange name.  Must not start with ``amq.`` (broker-reserved).
        exchange_type: AMQP routing algorithm.  One of ``direct``, ``fanout``,
            ``topic``, or ``headers``.
        durable: Survive broker restarts when ``True``.
        auto_delete: Delete when the last bound queue detaches.
        arguments: Optional AMQP x-arguments (e.g. ``{"alternate-exchange": "ae"}``).
    """

    name: str = Field(
        description=(
            "Exchange name. Must not start with 'amq.' (broker-reserved). "
            "Use empty string only with passive=True to inspect the default exchange."
        )
    )
    exchange_type: ExchangeType = Field(
        default=ExchangeType.DIRECT,
        description=(
            "AMQP exchange type controlling how messages are routed: "
            "'direct' (exact routing key match), "
            "'fanout' (broadcast to all bound queues), "
            "'topic' (wildcard pattern matching with * and #), "
            "'headers' (route by message header attributes)."
        ),
    )
    durable: bool = Field(
        default=False,
        description="Survive broker restarts. Set True for production exchanges.",
    )
    auto_delete: bool = Field(
        default=False,
        description="Delete automatically when the last queue unbinds.",
    )
    arguments: dict[str, Any] = Field(
        default_factory=dict,
        description='Optional AMQP x-arguments, e.g. {"alternate-exchange": "fallback"}.',
    )


class _DeleteExchangeInput(BaseModel):
    """Input schema for :class:`DeleteExchangeTool`.

    Attributes:
        name: Name of the exchange to delete.
        if_unused: Only delete if no queues are bound to this exchange.
    """

    name: str = Field(description="Name of the exchange to delete.")
    if_unused: bool = Field(
        default=False,
        description="Refuse to delete if queues are still bound to this exchange.",
    )


class _BindExchangeInput(BaseModel):
    """Input schema for :class:`BindExchangeTool`.

    Attributes:
        destination: Exchange that will receive routed messages.
        source: Exchange that publishes messages into the binding.
        routing_key: Routing key or pattern for the binding.
        arguments: Optional binding arguments (headers exchange).
    """

    destination: str = Field(
        description="Destination exchange name (receives messages routed from source)."
    )
    source: str = Field(description="Source exchange name (publishes messages into this binding).")
    routing_key: str = Field(
        default="",
        description=(
            "Routing key or pattern for the binding. "
            "Use '#' to forward all messages from the source exchange."
        ),
    )
    arguments: dict[str, Any] = Field(
        default_factory=dict,
        description="Optional binding arguments used by headers exchanges.",
    )


# ---------------------------------------------------------------------------
# Tool classes
# ---------------------------------------------------------------------------


class DeclareExchangeTool(_RabbitMQBaseTool):
    """Declare an AMQP exchange on the broker.

    Use this tool to create a new exchange or verify that an existing exchange
    matches the given parameters.  Choose the exchange type based on your
    routing requirements:

    * ``direct`` — route by exact routing key (default).
    * ``fanout`` — broadcast to every bound queue regardless of routing key.
    * ``topic`` — route by wildcard pattern (``*`` = one word, ``#`` = zero or more).
    * ``headers`` — route by message header attributes.

    Returns a summary string confirming the exchange configuration.

    Example agent call::

        tool.invoke({
            "name": "order_events",
            "exchange_type": "topic",
            "durable": true
        })
    """

    name: str = "rabbitmq_declare_exchange"
    description: str = (
        "Declare (create) a RabbitMQ exchange. "
        "Choose exchange_type: 'direct' for exact routing key matching, "
        "'fanout' to broadcast to all bound queues, "
        "'topic' for wildcard pattern routing (* matches one word, # matches zero or more), "
        "'headers' to route by message headers. "
        "Set durable=true for exchanges that must survive a broker restart. "
        "Returns a confirmation with the exchange configuration."
    )
    args_schema: type[BaseModel] = _DeclareExchangeInput

    def _execute(  # type: ignore[override]
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        durable: bool = False,
        auto_delete: bool = False,
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        """Declare the exchange and return a summary string.

        Args:
            name: Exchange name.
            exchange_type: AMQP exchange type.
            durable: Survive broker restarts.
            auto_delete: Delete when last queue unbinds.
            arguments: AMQP x-arguments.
            **_: Ignored extra kwargs.

        Returns:
            Human-readable confirmation of the declaration.
        """
        with self._make_client() as client:
            info = client.declare_exchange(
                name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
        return (
            f"Exchange '{info.name}' ({info.exchange_type.value}) declared successfully. "
            f"durable={info.durable}, auto_delete={info.auto_delete}."
        )

    async def _aexecute(  # type: ignore[override]
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        durable: bool = False,
        auto_delete: bool = False,
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            info = await client.declare_exchange(
                name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
        return (
            f"Exchange '{info.name}' ({info.exchange_type.value}) declared successfully. "
            f"durable={info.durable}, auto_delete={info.auto_delete}."
        )


class DeleteExchangeTool(_RabbitMQBaseTool):
    """Delete an exchange from the broker.

    After deletion, messages published to this exchange will be unroutable.
    Use ``if_unused=true`` for a safe deletion that aborts if queues are
    still bound.

    Returns a confirmation string on success.

    Example agent call::

        tool.invoke({"name": "old_exchange", "if_unused": true})
    """

    name: str = "rabbitmq_delete_exchange"
    description: str = (
        "Delete a RabbitMQ exchange. "
        "After deletion, messages published to it will be unroutable. "
        "Use if_unused=true to prevent deletion while queues are still bound. "
        "Returns a confirmation string on success."
    )
    args_schema: type[BaseModel] = _DeleteExchangeInput

    def _execute(  # type: ignore[override]
        self,
        name: str,
        if_unused: bool = False,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            client.delete_exchange(name, if_unused=if_unused)
        return f"Exchange '{name}' deleted successfully."

    async def _aexecute(  # type: ignore[override]
        self,
        name: str,
        if_unused: bool = False,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            await client.delete_exchange(name, if_unused=if_unused)
        return f"Exchange '{name}' deleted successfully."


class BindExchangeTool(_RabbitMQBaseTool):
    """Create an exchange-to-exchange (E2E) binding.

    Messages published to the ``source`` exchange are forwarded to the
    ``destination`` exchange when the routing key matches.  This is useful
    for building fan-out pipelines, dead-letter chains, or routing trees
    without coupling producers to multiple exchanges directly.

    Returns a :class:`~langchain_rabbitmq.utilities._models.BindingInfo`
    summary string.

    Example agent call::

        tool.invoke({
            "destination": "orders_eu",
            "source": "orders",
            "routing_key": "order.*.eu"
        })
    """

    name: str = "rabbitmq_bind_exchange"
    description: str = (
        "Create an exchange-to-exchange (E2E) binding in RabbitMQ. "
        "Messages published to the source exchange are forwarded to the destination "
        "exchange when the routing key matches. "
        "Useful for routing trees, dead-letter chains, and fan-out pipelines. "
        "Returns a confirmation with binding details."
    )
    args_schema: type[BaseModel] = _BindExchangeInput

    def _execute(  # type: ignore[override]
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        with self._make_client() as client:
            binding = client.bind_exchange(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments or {},
            )
        key_desc = f" with routing_key='{binding.routing_key}'" if binding.routing_key else ""
        return (
            f"Exchange '{binding.destination}' bound to source exchange "
            f"'{binding.source}'{key_desc}."
        )

    async def _aexecute(  # type: ignore[override]
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
        **_: Any,
    ) -> str:
        async with self._make_async_client() as client:
            binding = await client.bind_exchange(
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments or {},
            )
        key_desc = f" with routing_key='{binding.routing_key}'" if binding.routing_key else ""
        return (
            f"Exchange '{binding.destination}' bound to source exchange "
            f"'{binding.source}'{key_desc}."
        )


# Ordered list used by toolkit / test factories
EXCHANGE_TOOLS: list[type[_RabbitMQBaseTool]] = [
    DeclareExchangeTool,
    DeleteExchangeTool,
    BindExchangeTool,
]

__all__: list[str] = [
    "EXCHANGE_TOOLS",
    "BindExchangeTool",
    "DeclareExchangeTool",
    "DeleteExchangeTool",
]
