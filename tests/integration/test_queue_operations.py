"""Integration tests — queue lifecycle against a live RabbitMQ broker.

Covers declare, get_info, purge, bind, unbind, and delete for the sync
:class:`.RabbitMQClient` and its async mirror.
"""

from __future__ import annotations

import pytest

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import RabbitMQChannelError
from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

from .conftest import _unique_name

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Sync client
# ---------------------------------------------------------------------------


class TestSyncQueueLifecycle:
    def test_declare_and_delete(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.queue")
        info = sync_client.declare_queue(name, durable=False)
        assert info.name == name
        assert info.durable is False

        deleted = sync_client.delete_queue(name)
        assert deleted == 0  # no messages purged

    def test_declare_durable(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.durable")
        info = sync_client.declare_queue(name, durable=True)
        assert info.durable is True
        sync_client.delete_queue(name)

    def test_get_queue_info_passive(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.passive")
        sync_client.declare_queue(name, durable=False)
        info = sync_client.get_queue_info(name)
        assert info.name == name
        sync_client.delete_queue(name)

    def test_get_queue_info_missing_raises(
        self, sync_client: RabbitMQClient
    ) -> None:
        with pytest.raises(RabbitMQChannelError):
            sync_client.get_queue_info("nonexistent.queue.xyz.abc")

    def test_purge_queue(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.purge")
        sync_client.declare_queue(name, durable=False)
        # Publish a message directly via pika to have something to purge
        import pika  # type: ignore[import-untyped]

        sync_client.publish_message(
            exchange="", routing_key=name, body=b"purge-me"
        )
        purged = sync_client.purge_queue(name)
        assert purged >= 1
        sync_client.delete_queue(name)

    def test_bind_and_unbind_queue(self, sync_client: RabbitMQClient) -> None:
        queue_name = _unique_name("test.bindq")
        exchange_name = _unique_name("test.bindex")

        sync_client.declare_queue(queue_name, durable=False)
        sync_client.declare_exchange(exchange_name, durable=False)
        sync_client.bind_queue(queue_name, exchange_name, routing_key="test.key")
        # Unbind should not raise
        sync_client.unbind_queue(queue_name, exchange_name, routing_key="test.key")

        sync_client.delete_queue(queue_name)
        sync_client.delete_exchange(exchange_name)

    def test_delete_nonexistent_queue(self, sync_client: RabbitMQClient) -> None:
        # delete_queue on a missing queue returns 0 (no error from RabbitMQ)
        deleted = sync_client.delete_queue(_unique_name("ghost.queue"))
        assert deleted == 0


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


class TestAsyncQueueLifecycle:
    @pytest.mark.asyncio
    async def test_declare_and_delete_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        name = _unique_name("test.aqueue")
        info = await async_client.declare_queue(name, durable=False)
        assert info.name == name
        deleted = await async_client.delete_queue(name)
        assert deleted == 0

    @pytest.mark.asyncio
    async def test_get_queue_info_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        name = _unique_name("test.apassive")
        await async_client.declare_queue(name, durable=False)
        info = await async_client.get_queue_info(name)
        assert info.name == name
        await async_client.delete_queue(name)

    @pytest.mark.asyncio
    async def test_purge_queue_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        name = _unique_name("test.apurge")
        await async_client.declare_queue(name, durable=False)
        await async_client.publish_message("", name, b"hello")
        purged = await async_client.purge_queue(name)
        assert purged >= 1
        await async_client.delete_queue(name)

    @pytest.mark.asyncio
    async def test_bind_unbind_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        q = _unique_name("test.abindq")
        ex = _unique_name("test.abindex")
        await async_client.declare_queue(q, durable=False)
        await async_client.declare_exchange(ex, durable=False)
        await async_client.bind_queue(q, ex, routing_key="rk")
        await async_client.unbind_queue(q, ex, routing_key="rk")
        await async_client.delete_queue(q)
        await async_client.delete_exchange(ex)
