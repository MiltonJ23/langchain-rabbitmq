"""Integration tests — Management HTTP API against a live broker.

Uses the session-scoped RabbitMQ container provided by conftest.py.
All tests require the ``rabbitmq:3.12-management`` image so the
Management Plugin is available on port 15672.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pytest

from langchain_rabbitmq.utilities.management import (
    AsyncManagementAPIClient,
    ManagementAPIClient,
)
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

from .conftest import _unique_name

if TYPE_CHECKING:
    from langchain_rabbitmq.config import RabbitMQSettings

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Sync client
# ---------------------------------------------------------------------------


class TestSyncManagementAPI:
    def test_get_overview(self, rabbitmq_settings: RabbitMQSettings) -> None:
        with ManagementAPIClient(rabbitmq_settings) as client:
            overview = client.get_overview()

        assert isinstance(overview, dict)
        assert "rabbitmq_version" in overview
        assert "management_version" in overview

    def test_get_node_stats(self, rabbitmq_settings: RabbitMQSettings) -> None:
        with ManagementAPIClient(rabbitmq_settings) as client:
            nodes = client.get_node_stats()

        assert isinstance(nodes, list)
        assert len(nodes) >= 1
        node = nodes[0]
        assert "name" in node
        assert "type" in node

    def test_list_exchanges_all(self, rabbitmq_settings: RabbitMQSettings) -> None:
        with ManagementAPIClient(rabbitmq_settings) as client:
            exchanges = client.list_exchanges()

        assert isinstance(exchanges, list)
        names = {ex["name"] for ex in exchanges}
        # Default AMQP exchanges must be present
        assert "" in names  # default exchange
        assert "amq.direct" in names

    def test_list_exchanges_vhost(self, rabbitmq_settings: RabbitMQSettings) -> None:
        with ManagementAPIClient(rabbitmq_settings) as client:
            exchanges = client.list_exchanges(vhost="/")

        assert isinstance(exchanges, list)
        # All returned exchanges must belong to vhost "/"
        for ex in exchanges:
            assert ex.get("vhost") == "/"

    def test_list_queues_empty_by_default(self, rabbitmq_settings: RabbitMQSettings) -> None:
        # Declare a brand-new queue and make sure it appears
        queue_name = _unique_name("mgmt.list.q")
        sync = RabbitMQClient(rabbitmq_settings)
        sync.connect()
        try:
            sync.declare_queue(queue_name, durable=False)
        finally:
            sync.close()

        with ManagementAPIClient(rabbitmq_settings) as client:
            queues = client.list_queues()

        names = [q["name"] for q in queues]
        assert queue_name in names

        # Cleanup
        sync = RabbitMQClient(rabbitmq_settings)
        sync.connect()
        try:
            sync.delete_queue(queue_name)
        finally:
            sync.close()

    def test_list_queues_vhost(self, rabbitmq_settings: RabbitMQSettings) -> None:
        with ManagementAPIClient(rabbitmq_settings) as client:
            queues = client.list_queues(vhost="/")

        assert isinstance(queues, list)
        for q in queues:
            assert q.get("vhost") == "/"

    def test_list_bindings(self, rabbitmq_settings: RabbitMQSettings) -> None:
        with ManagementAPIClient(rabbitmq_settings) as client:
            bindings = client.list_bindings()

        assert isinstance(bindings, list)

    def test_list_bindings_after_bind(self, rabbitmq_settings: RabbitMQSettings) -> None:
        queue_name = _unique_name("mgmt.bind.q")
        exchange_name = _unique_name("mgmt.bind.ex")
        routing_key = "mgmt.rk"

        sync = RabbitMQClient(rabbitmq_settings)
        sync.connect()
        try:
            sync.declare_exchange(exchange_name, durable=False)
            sync.declare_queue(queue_name, durable=False)
            sync.bind_queue(queue_name, exchange_name, routing_key=routing_key)
        finally:
            sync.close()

        with ManagementAPIClient(rabbitmq_settings) as client:
            bindings = client.list_bindings(vhost="/")

        binding_keys = {
            (b.get("source"), b.get("destination"), b.get("routing_key")) for b in bindings
        }
        assert (exchange_name, queue_name, routing_key) in binding_keys

        # Cleanup
        sync = RabbitMQClient(rabbitmq_settings)
        sync.connect()
        try:
            sync.delete_queue(queue_name)
            sync.delete_exchange(exchange_name)
        finally:
            sync.close()

    def test_context_manager_closes_client(self, rabbitmq_settings: RabbitMQSettings) -> None:
        client = ManagementAPIClient(rabbitmq_settings)
        with client:
            _ = client.get_overview()
        # After exiting, the underlying httpx.Client should be closed
        assert client._client is None


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


class TestAsyncManagementAPI:
    @pytest.mark.asyncio
    async def test_get_overview_async(self, rabbitmq_settings: RabbitMQSettings) -> None:
        async with AsyncManagementAPIClient(rabbitmq_settings) as client:
            overview = await client.get_overview()

        assert isinstance(overview, dict)
        assert "rabbitmq_version" in overview

    @pytest.mark.asyncio
    async def test_get_node_stats_async(self, rabbitmq_settings: RabbitMQSettings) -> None:
        async with AsyncManagementAPIClient(rabbitmq_settings) as client:
            nodes = await client.get_node_stats()

        assert isinstance(nodes, list)
        assert len(nodes) >= 1

    @pytest.mark.asyncio
    async def test_list_exchanges_async(self, rabbitmq_settings: RabbitMQSettings) -> None:
        async with AsyncManagementAPIClient(rabbitmq_settings) as client:
            exchanges = await client.list_exchanges()

        assert isinstance(exchanges, list)
        names = {ex["name"] for ex in exchanges}
        assert "" in names
        assert "amq.direct" in names

    @pytest.mark.asyncio
    async def test_list_queues_async(self, rabbitmq_settings: RabbitMQSettings) -> None:
        queue_name = _unique_name("amgmt.q")
        sync = RabbitMQClient(rabbitmq_settings)
        sync.connect()
        try:
            sync.declare_queue(queue_name, durable=False)
        finally:
            sync.close()

        async with AsyncManagementAPIClient(rabbitmq_settings) as client:
            queues = await client.list_queues()

        names = [q["name"] for q in queues]
        assert queue_name in names

        sync = RabbitMQClient(rabbitmq_settings)
        sync.connect()
        try:
            sync.delete_queue(queue_name)
        finally:
            sync.close()

    @pytest.mark.asyncio
    async def test_list_bindings_async(self, rabbitmq_settings: RabbitMQSettings) -> None:
        async with AsyncManagementAPIClient(rabbitmq_settings) as client:
            bindings = await client.list_bindings(vhost="/")

        assert isinstance(bindings, list)

    @pytest.mark.asyncio
    async def test_context_manager_async_closes(self, rabbitmq_settings: RabbitMQSettings) -> None:
        client = AsyncManagementAPIClient(rabbitmq_settings)
        async with client:
            _ = await client.get_overview()
        assert client._client is None
