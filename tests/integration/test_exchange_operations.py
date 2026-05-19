"""Integration tests — exchange lifecycle against a live RabbitMQ broker."""

from __future__ import annotations

import pytest

from langchain_rabbitmq.utilities._models import ExchangeType
from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

from .conftest import _unique_name

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Sync client
# ---------------------------------------------------------------------------


class TestSyncExchangeLifecycle:
    def test_declare_direct_and_delete(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.direct")
        info = sync_client.declare_exchange(name, ExchangeType.DIRECT, durable=False)
        assert info.name == name
        assert info.exchange_type == ExchangeType.DIRECT
        sync_client.delete_exchange(name)

    def test_declare_topic_exchange(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.topic")
        info = sync_client.declare_exchange(name, ExchangeType.TOPIC, durable=False)
        assert info.exchange_type == ExchangeType.TOPIC
        sync_client.delete_exchange(name)

    def test_declare_fanout_exchange(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.fanout")
        info = sync_client.declare_exchange(name, ExchangeType.FANOUT, durable=False)
        assert info.exchange_type == ExchangeType.FANOUT
        sync_client.delete_exchange(name)

    def test_declare_headers_exchange(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.headers")
        info = sync_client.declare_exchange(name, ExchangeType.HEADERS, durable=False)
        assert info.exchange_type == ExchangeType.HEADERS
        sync_client.delete_exchange(name)

    def test_bind_exchange_to_exchange(self, sync_client: RabbitMQClient) -> None:
        src = _unique_name("test.src")
        dst = _unique_name("test.dst")
        sync_client.declare_exchange(src, ExchangeType.TOPIC, durable=False)
        sync_client.declare_exchange(dst, ExchangeType.DIRECT, durable=False)

        binding = sync_client.bind_exchange(
            destination=dst, source=src, routing_key="test.*"
        )
        assert binding.source == src
        assert binding.destination == dst

        sync_client.delete_exchange(src)
        sync_client.delete_exchange(dst)

    def test_delete_exchange_if_unused(self, sync_client: RabbitMQClient) -> None:
        name = _unique_name("test.unused")
        sync_client.declare_exchange(name, ExchangeType.DIRECT, durable=False)
        # Should delete without error since no queues are bound
        sync_client.delete_exchange(name, if_unused=True)


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


class TestAsyncExchangeLifecycle:
    @pytest.mark.asyncio
    async def test_declare_topic_and_delete_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        name = _unique_name("test.atopic")
        info = await async_client.declare_exchange(
            name, ExchangeType.TOPIC, durable=False
        )
        assert info.name == name
        assert info.exchange_type == ExchangeType.TOPIC
        await async_client.delete_exchange(name)

    @pytest.mark.asyncio
    async def test_bind_exchange_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        src = _unique_name("test.asrc")
        dst = _unique_name("test.adst")
        await async_client.declare_exchange(src, ExchangeType.FANOUT, durable=False)
        await async_client.declare_exchange(dst, ExchangeType.DIRECT, durable=False)
        binding = await async_client.bind_exchange(
            destination=dst, source=src, routing_key=""
        )
        assert binding.source == src
        await async_client.delete_exchange(src)
        await async_client.delete_exchange(dst)
