"""LangChain tools for RabbitMQ admin and monitoring operations.

Provides seven tools that leverage both the AMQP protocol and the RabbitMQ
Management HTTP API:

* :class:`ListQueuesTool` — list queues via Management API.
* :class:`ListExchangesTool` — list exchanges via Management API.
* :class:`ListBindingsTool` — list bindings via Management API.
* :class:`GetNodeStatsTool` — retrieve cluster node statistics.
* :class:`CheckHealthTool` — AMQP connectivity health probe.
* :class:`GetConnectionInfoTool` — AMQP connection metadata.
* :class:`CloseConnectionTool` — gracefully verify and close an AMQP connection.

Example:
    Registering admin tools with an agent::

        from langchain_rabbitmq.tools.admin import ADMIN_TOOLS
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings(host="broker.internal")
        tools = [cls(settings=settings) for cls in ADMIN_TOOLS]
"""

from __future__ import annotations

from typing import Any, Optional

from pydantic import BaseModel, Field

from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.utilities.management import (
    AsyncManagementAPIClient,
    ManagementAPIClient,
)

# ---------------------------------------------------------------------------
# Input schemas
# ---------------------------------------------------------------------------


class _ListQueuesInput(BaseModel):
    """Input schema for :class:`ListQueuesTool`.

    Attributes:
        vhost: Optional virtual host filter.  ``""`` lists queues across all
            virtual hosts.
    """

    vhost: str = Field(
        default="",
        description=(
            "Virtual host to filter by. "
            "Use empty string '' to list queues across all virtual hosts."
        ),
    )


class _ListExchangesInput(BaseModel):
    """Input schema for :class:`ListExchangesTool`.

    Attributes:
        vhost: Optional virtual host filter.
        include_defaults: Include the default broker exchanges (names starting
            with ``amq.``).
    """

    vhost: str = Field(
        default="",
        description="Virtual host filter. Empty string lists all virtual hosts.",
    )
    include_defaults: bool = Field(
        default=False,
        description="Include default broker exchanges (names starting with 'amq.' or '').",
    )


class _ListBindingsInput(BaseModel):
    """Input schema for :class:`ListBindingsTool`.

    Attributes:
        vhost: Optional virtual host filter.
    """

    vhost: str = Field(
        default="",
        description="Virtual host filter. Empty string lists bindings in all virtual hosts.",
    )


class _GetNodeStatsInput(BaseModel):
    """Input schema for :class:`GetNodeStatsTool`.

    Attributes:
        fields: Optional list of stat field names to include in the output.
            Defaults to a concise set.  Pass ``[]`` to include all fields.
    """

    fields: list[str] = Field(
        default_factory=lambda: [
            "name",
            "type",
            "running",
            "mem_used",
            "mem_limit",
            "fd_used",
            "fd_total",
            "proc_used",
            "proc_total",
            "uptime",
            "rates_mode",
        ],
        description=(
            "List of node stat field names to include. Pass [] to return all available fields."
        ),
    )


class _CheckHealthInput(BaseModel):
    """Input schema for :class:`CheckHealthTool`.

    No configurable inputs — the settings are taken from the tool's
    ``settings`` field.
    """


class _GetConnectionInfoInput(BaseModel):
    """Input schema for :class:`GetConnectionInfoTool`.

    No configurable inputs.
    """


class _CloseConnectionInput(BaseModel):
    """Input schema for :class:`CloseConnectionTool`.

    No configurable inputs.
    """


# ---------------------------------------------------------------------------
# Tool classes
# ---------------------------------------------------------------------------


class ListQueuesTool(_RabbitMQBaseTool):
    """List queues via the RabbitMQ Management HTTP API.

    Returns a formatted table of queue names, message counts, consumer counts,
    and durability flags.

    Requires the Management Plugin to be enabled and
    ``RABBITMQ_MANAGEMENT_API_URL`` to be configured.

    Example agent call::

        tool.invoke({"vhost": "/"})
    """

    name: str = "rabbitmq_list_queues"
    description: str = (
        "List all RabbitMQ queues via the Management API. "
        "Returns queue name, ready message count, consumer count, and durability. "
        "Optionally filter by virtual host. "
        "Requires RABBITMQ_MANAGEMENT_API_URL to be set."
    )
    args_schema: type[BaseModel] = _ListQueuesInput

    def _execute(self, vhost: str = "", **_: Any) -> str:  # type: ignore[override]
        vhost_filter: Optional[str] = vhost or None
        with ManagementAPIClient(self.settings) as mgmt:
            queues = mgmt.list_queues(vhost=vhost_filter)
        if not queues:
            scope = f"vhost '{vhost}'" if vhost else "all virtual hosts"
            return f"No queues found in {scope}."
        lines = [f"Found {len(queues)} queue(s):"]
        for q in queues:
            lines.append(
                f"  {q.get('name', '?')!r:40s}  "
                f"messages={q.get('messages', 0):>6}  "
                f"consumers={q.get('consumers', 0):>4}  "
                f"durable={q.get('durable', False)}"
            )
        return "\n".join(lines)

    async def _aexecute(self, vhost: str = "", **_: Any) -> str:  # type: ignore[override]
        vhost_filter: Optional[str] = vhost or None
        async with AsyncManagementAPIClient(self.settings) as mgmt:
            queues = await mgmt.list_queues(vhost=vhost_filter)
        if not queues:
            scope = f"vhost '{vhost}'" if vhost else "all virtual hosts"
            return f"No queues found in {scope}."
        lines = [f"Found {len(queues)} queue(s):"]
        for q in queues:
            lines.append(
                f"  {q.get('name', '?')!r:40s}  "
                f"messages={q.get('messages', 0):>6}  "
                f"consumers={q.get('consumers', 0):>4}  "
                f"durable={q.get('durable', False)}"
            )
        return "\n".join(lines)


class ListExchangesTool(_RabbitMQBaseTool):
    """List exchanges via the RabbitMQ Management HTTP API.

    Example agent call::

        tool.invoke({"vhost": "/", "include_defaults": false})
    """

    name: str = "rabbitmq_list_exchanges"
    description: str = (
        "List all RabbitMQ exchanges via the Management API. "
        "Returns exchange name, type, and durability. "
        "Set include_defaults=false (default) to hide built-in broker exchanges. "
        "Requires RABBITMQ_MANAGEMENT_API_URL to be set."
    )
    args_schema: type[BaseModel] = _ListExchangesInput

    def _execute(  # type: ignore[override]
        self,
        vhost: str = "",
        include_defaults: bool = False,
        **_: Any,
    ) -> str:
        vhost_filter: Optional[str] = vhost or None
        with ManagementAPIClient(self.settings) as mgmt:
            exchanges = mgmt.list_exchanges(vhost=vhost_filter)
        if not include_defaults:
            exchanges = [
                e
                for e in exchanges
                if e.get("name", "") and not e.get("name", "").startswith("amq.")
            ]
        if not exchanges:
            return "No user-defined exchanges found."
        lines = [f"Found {len(exchanges)} exchange(s):"]
        for e in exchanges:
            lines.append(
                f"  {e.get('name', '?')!r:40s}  "
                f"type={e.get('type', '?'):10s}  "
                f"durable={e.get('durable', False)}"
            )
        return "\n".join(lines)

    async def _aexecute(  # type: ignore[override]
        self,
        vhost: str = "",
        include_defaults: bool = False,
        **_: Any,
    ) -> str:
        vhost_filter: Optional[str] = vhost or None
        async with AsyncManagementAPIClient(self.settings) as mgmt:
            exchanges = await mgmt.list_exchanges(vhost=vhost_filter)
        if not include_defaults:
            exchanges = [
                e
                for e in exchanges
                if e.get("name", "") and not e.get("name", "").startswith("amq.")
            ]
        if not exchanges:
            return "No user-defined exchanges found."
        lines = [f"Found {len(exchanges)} exchange(s):"]
        for e in exchanges:
            lines.append(
                f"  {e.get('name', '?')!r:40s}  "
                f"type={e.get('type', '?'):10s}  "
                f"durable={e.get('durable', False)}"
            )
        return "\n".join(lines)


class ListBindingsTool(_RabbitMQBaseTool):
    """List all AMQP bindings via the RabbitMQ Management HTTP API.

    Example agent call::

        tool.invoke({"vhost": "/"})
    """

    name: str = "rabbitmq_list_bindings"
    description: str = (
        "List all RabbitMQ bindings (queue-to-exchange and exchange-to-exchange) "
        "via the Management API. "
        "Returns source exchange, destination, and routing key for each binding. "
        "Requires RABBITMQ_MANAGEMENT_API_URL to be set."
    )
    args_schema: type[BaseModel] = _ListBindingsInput

    def _execute(self, vhost: str = "", **_: Any) -> str:  # type: ignore[override]
        vhost_filter: Optional[str] = vhost or None
        with ManagementAPIClient(self.settings) as mgmt:
            bindings = mgmt.list_bindings(vhost=vhost_filter)
        if not bindings:
            return "No bindings found."
        lines = [f"Found {len(bindings)} binding(s):"]
        for b in bindings:
            src = b.get("source", "(default)")
            dst = b.get("destination", "?")
            dst_type = b.get("destination_type", "?")
            key = b.get("routing_key", "")
            lines.append(f"  {src!r} → {dst!r} ({dst_type})" + (f" key={key!r}" if key else ""))
        return "\n".join(lines)

    async def _aexecute(self, vhost: str = "", **_: Any) -> str:  # type: ignore[override]
        vhost_filter: Optional[str] = vhost or None
        async with AsyncManagementAPIClient(self.settings) as mgmt:
            bindings = await mgmt.list_bindings(vhost=vhost_filter)
        if not bindings:
            return "No bindings found."
        lines = [f"Found {len(bindings)} binding(s):"]
        for b in bindings:
            src = b.get("source", "(default)")
            dst = b.get("destination", "?")
            dst_type = b.get("destination_type", "?")
            key = b.get("routing_key", "")
            lines.append(f"  {src!r} → {dst!r} ({dst_type})" + (f" key={key!r}" if key else ""))
        return "\n".join(lines)


class GetNodeStatsTool(_RabbitMQBaseTool):
    """Retrieve cluster node statistics via the Management HTTP API.

    Returns memory usage, file descriptor usage, process count, and uptime
    for each node in the RabbitMQ cluster.

    Example agent call::

        tool.invoke({})
        # Returns: node stats for rabbit@hostname
    """

    name: str = "rabbitmq_get_node_stats"
    description: str = (
        "Retrieve RabbitMQ cluster node statistics via the Management API. "
        "Returns memory usage, file descriptors, process counts, and uptime. "
        "Useful for diagnosing broker performance issues. "
        "Requires RABBITMQ_MANAGEMENT_API_URL to be set."
    )
    args_schema: type[BaseModel] = _GetNodeStatsInput

    def _execute(self, fields: Optional[list[str]] = None, **_: Any) -> str:  # type: ignore[override]
        selected = set(fields) if fields else set()
        with ManagementAPIClient(self.settings) as mgmt:
            nodes = mgmt.get_node_stats()
        if not nodes:
            return "No node statistics available."
        lines = [f"Cluster has {len(nodes)} node(s):"]
        for node in nodes:
            filtered: dict[str, Any] = (
                {k: v for k, v in node.items() if k in selected} if selected else node
            )
            lines.append(f"\nNode: {node.get('name', '?')}")
            for k, v in filtered.items():
                if k == "name":
                    continue
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)

    async def _aexecute(self, fields: Optional[list[str]] = None, **_: Any) -> str:  # type: ignore[override]
        selected = set(fields) if fields else set()
        async with AsyncManagementAPIClient(self.settings) as mgmt:
            nodes = await mgmt.get_node_stats()
        if not nodes:
            return "No node statistics available."
        lines = [f"Cluster has {len(nodes)} node(s):"]
        for node in nodes:
            filtered = {k: v for k, v in node.items() if k in selected} if selected else node
            lines.append(f"\nNode: {node.get('name', '?')}")
            for k, v in filtered.items():
                if k == "name":
                    continue
                lines.append(f"  {k}: {v}")
        return "\n".join(lines)


class CheckHealthTool(_RabbitMQBaseTool):
    """Probe AMQP broker connectivity and report health status.

    Performs a lightweight declare/delete of a transient exclusive queue to
    verify that the broker accepts connections and channel operations.  Does
    not require the Management Plugin.

    Example agent call::

        tool.invoke({})
        # → "Broker health: OK — Broker connection healthy (localhost:5672)"
    """

    name: str = "rabbitmq_check_health"
    description: str = (
        "Check RabbitMQ broker connectivity by opening a test AMQP connection. "
        "Returns OK, DEGRADED, or DOWN with a human-readable explanation. "
        "Use this to verify the broker is reachable before other operations."
    )
    args_schema: type[BaseModel] = _CheckHealthInput

    def _execute(self, **_: Any) -> str:  # type: ignore[override]
        with self._make_client() as client:
            health = client.check_health()
        return (
            f"Broker health: {health.status.value.upper()} — "
            f"{health.message} ({health.host}:{health.port})"
        )

    async def _aexecute(self, **_: Any) -> str:  # type: ignore[override]
        async with self._make_async_client() as client:
            health = await client.check_health()
        return (
            f"Broker health: {health.status.value.upper()} — "
            f"{health.message} ({health.host}:{health.port})"
        )


class GetConnectionInfoTool(_RabbitMQBaseTool):
    """Return metadata about the current AMQP connection.

    Reports host, port, virtual host, server version, and platform string.

    Example agent call::

        tool.invoke({})
        # → "Connected to RabbitMQ 3.12.4 at localhost:5672 (vhost='/')"
    """

    name: str = "rabbitmq_get_connection_info"
    description: str = (
        "Get metadata about the active RabbitMQ AMQP connection: "
        "host, port, virtual host, broker version, and platform. "
        "Use this to confirm which broker the agent is connected to."
    )
    args_schema: type[BaseModel] = _GetConnectionInfoInput

    def _execute(self, **_: Any) -> str:  # type: ignore[override]
        with self._make_client() as client:
            info = client.get_connection_info()
        if not info.connected:
            return f"Not connected to broker at {info.host}:{info.port}."
        version = f" v{info.server_version}" if info.server_version else ""
        platform = f" ({info.server_platform})" if info.server_platform else ""
        return (
            f"Connected to RabbitMQ{version}{platform} "
            f"at {info.host}:{info.port} "
            f"(vhost='{info.virtual_host}')"
        )

    async def _aexecute(self, **_: Any) -> str:  # type: ignore[override]
        async with self._make_async_client() as client:
            info = await client.get_connection_info()
        if not info.connected:
            return f"Not connected to broker at {info.host}:{info.port}."
        version = f" v{info.server_version}" if info.server_version else ""
        platform = f" ({info.server_platform})" if info.server_platform else ""
        return (
            f"Connected to RabbitMQ{version}{platform} "
            f"at {info.host}:{info.port} "
            f"(vhost='{info.virtual_host}')"
        )


class CloseConnectionTool(_RabbitMQBaseTool):
    """Gracefully open and then close an AMQP connection.

    In a short-lived tool invocation model each connection is already closed
    after every operation.  This tool is useful for explicit connection
    teardown verification in long-running agent sessions or integration tests.

    Example agent call::

        tool.invoke({})
        # → "AMQP connection to localhost:5672 closed gracefully."
    """

    name: str = "rabbitmq_close_connection"
    description: str = (
        "Open and gracefully close an AMQP connection to the RabbitMQ broker. "
        "Useful to verify connection teardown or to reset connection state "
        "in a long-running agent session."
    )
    args_schema: type[BaseModel] = _CloseConnectionInput

    def _execute(self, **_: Any) -> str:  # type: ignore[override]
        client = self._make_client()
        try:
            client.connect()
        finally:
            client.close()
        return f"AMQP connection to {self.settings.host}:{self.settings.port} closed gracefully."

    async def _aexecute(self, **_: Any) -> str:  # type: ignore[override]
        client = self._make_async_client()
        try:
            await client.connect()
        finally:
            await client.close()
        return f"AMQP connection to {self.settings.host}:{self.settings.port} closed gracefully."


# Ordered list used by RabbitMQToolkit / test factories
ADMIN_TOOLS: list[type[_RabbitMQBaseTool]] = [
    ListQueuesTool,
    ListExchangesTool,
    ListBindingsTool,
    GetNodeStatsTool,
    CheckHealthTool,
    GetConnectionInfoTool,
    CloseConnectionTool,
]

__all__: list[str] = [
    "ADMIN_TOOLS",
    "CheckHealthTool",
    "CloseConnectionTool",
    "GetConnectionInfoTool",
    "GetNodeStatsTool",
    "ListBindingsTool",
    "ListExchangesTool",
    "ListQueuesTool",
]
