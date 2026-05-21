"""Integration tests — full publish → consume → ack/nack/reject cycle.

Uses a live RabbitMQ broker via testcontainers.  Every test is isolated
through uniquely-named transient queues that are cleaned up in teardown.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import pytest

from langchain_rabbitmq.utilities._models import ExchangeType

from .conftest import _unique_name

if TYPE_CHECKING:
    from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
    from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

pytestmark = pytest.mark.integration


# ---------------------------------------------------------------------------
# Sync client
# ---------------------------------------------------------------------------


class TestSyncMessageCycle:
    def test_publish_and_consume_text(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.text")
        sync_client.declare_queue(queue, durable=False)

        sync_client.publish_message(exchange="", routing_key=queue, body=b"hello world")
        result = sync_client.consume_message(queue, auto_ack=True)
        assert result is not None
        assert result.body == b"hello world"
        assert result.routing_key == queue

        sync_client.delete_queue(queue)

    def test_publish_and_consume_json(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.json")
        sync_client.declare_queue(queue, durable=False)

        payload = {"event": "order.created", "order_id": 42}
        sync_client.publish_message(
            exchange="",
            routing_key=queue,
            body=json.dumps(payload).encode(),
            content_type="application/json",
        )
        result = sync_client.consume_message(queue, auto_ack=True)
        assert result is not None
        assert json.loads(result.body) == payload
        assert result.content_type == "application/json"

        sync_client.delete_queue(queue)

    def test_consume_empty_queue_returns_none(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.empty")
        sync_client.declare_queue(queue, durable=False)

        result = sync_client.consume_message(queue)
        assert result is None

        sync_client.delete_queue(queue)

    def test_publish_persistent_message(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.persist")
        sync_client.declare_queue(queue, durable=True)

        sync_client.publish_message(
            exchange="", routing_key=queue, body=b"durable", persistent=True
        )
        result = sync_client.consume_message(queue, auto_ack=True)
        assert result is not None
        assert result.body == b"durable"

        sync_client.delete_queue(queue)

    def test_ack_message(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.ack")
        sync_client.declare_queue(queue, durable=False)

        sync_client.publish_message(exchange="", routing_key=queue, body=b"ack-me")
        result = sync_client.consume_message(queue, auto_ack=False)
        assert result is not None

        # Should not raise
        sync_client.ack_message(result.delivery_tag)
        sync_client.delete_queue(queue)

    def test_nack_with_requeue(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.nack")
        sync_client.declare_queue(queue, durable=False)

        sync_client.publish_message(exchange="", routing_key=queue, body=b"nack-me")
        result = sync_client.consume_message(queue, auto_ack=False)
        assert result is not None

        # Requeue the message
        sync_client.nack_message(result.delivery_tag, requeue=True)
        # It should be back in the queue
        requeued = sync_client.consume_message(queue, auto_ack=True)
        assert requeued is not None
        assert requeued.body == b"nack-me"

        sync_client.delete_queue(queue)

    def test_reject_without_requeue(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.reject")
        sync_client.declare_queue(queue, durable=False)

        sync_client.publish_message(exchange="", routing_key=queue, body=b"reject-me")
        result = sync_client.consume_message(queue, auto_ack=False)
        assert result is not None

        sync_client.reject_message(result.delivery_tag, requeue=False)
        # Queue should now be empty
        gone = sync_client.consume_message(queue, auto_ack=True)
        assert gone is None

        sync_client.delete_queue(queue)

    def test_publish_via_topic_exchange(self, sync_client: RabbitMQClient) -> None:
        exchange = _unique_name("test.topic.ex")
        queue = _unique_name("test.topic.q")

        sync_client.declare_exchange(exchange, ExchangeType.TOPIC, durable=False)
        sync_client.declare_queue(queue, durable=False)
        sync_client.bind_queue(queue, exchange, routing_key="order.#")

        sync_client.publish_message(
            exchange=exchange,
            routing_key="order.created",
            body=b"via-topic",
        )
        result = sync_client.consume_message(queue, auto_ack=True)
        assert result is not None
        assert result.body == b"via-topic"

        sync_client.delete_queue(queue)
        sync_client.delete_exchange(exchange)

    def test_publish_with_headers(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.headers")
        sync_client.declare_queue(queue, durable=False)

        sync_client.publish_message(
            exchange="",
            routing_key=queue,
            body=b"with-headers",
            headers={"x-source": "integration-test", "x-version": "1"},
        )
        result = sync_client.consume_message(queue, auto_ack=True)
        assert result is not None
        assert result.headers.get("x-source") == "integration-test"

        sync_client.delete_queue(queue)

    def test_publish_multiple_and_count(self, sync_client: RabbitMQClient) -> None:
        queue = _unique_name("test.msg.multi")
        sync_client.declare_queue(queue, durable=False)

        for i in range(10):
            sync_client.publish_message(exchange="", routing_key=queue, body=f"msg-{i}".encode())

        purged = sync_client.purge_queue(queue)
        assert purged == 10

        sync_client.delete_queue(queue)


# ---------------------------------------------------------------------------
# Async client
# ---------------------------------------------------------------------------


class TestAsyncMessageCycle:
    @pytest.mark.asyncio
    async def test_publish_and_consume_async(self, async_client: AsyncRabbitMQClient) -> None:
        queue = _unique_name("test.amsg")
        await async_client.declare_queue(queue, durable=False)

        await async_client.publish_message("", queue, b"async-hello")
        result = await async_client.consume_message(queue, auto_ack=True)
        assert result is not None
        assert result.body == b"async-hello"

        await async_client.delete_queue(queue)

    @pytest.mark.asyncio
    async def test_consume_empty_returns_none_async(
        self, async_client: AsyncRabbitMQClient
    ) -> None:
        queue = _unique_name("test.amsg.empty")
        await async_client.declare_queue(queue, durable=False)

        result = await async_client.consume_message(queue)
        assert result is None

        await async_client.delete_queue(queue)

    @pytest.mark.asyncio
    async def test_ack_async(self, async_client: AsyncRabbitMQClient) -> None:
        queue = _unique_name("test.amsg.ack")
        await async_client.declare_queue(queue, durable=False)

        await async_client.publish_message("", queue, b"ack-async")
        result = await async_client.consume_message(queue, auto_ack=False)
        assert result is not None
        await async_client.ack_message(result.delivery_tag)

        await async_client.delete_queue(queue)

    @pytest.mark.asyncio
    async def test_nack_requeue_async(self, async_client: AsyncRabbitMQClient) -> None:
        queue = _unique_name("test.amsg.nack")
        await async_client.declare_queue(queue, durable=False)

        await async_client.publish_message("", queue, b"nack-async")
        result = await async_client.consume_message(queue, auto_ack=False)
        assert result is not None
        await async_client.nack_message(result.delivery_tag, requeue=True)

        requeued = await async_client.consume_message(queue, auto_ack=True)
        assert requeued is not None
        assert requeued.body == b"nack-async"

        await async_client.delete_queue(queue)

    @pytest.mark.asyncio
    async def test_reject_async(self, async_client: AsyncRabbitMQClient) -> None:
        queue = _unique_name("test.amsg.reject")
        await async_client.declare_queue(queue, durable=False)

        await async_client.publish_message("", queue, b"reject-async")
        result = await async_client.consume_message(queue, auto_ack=False)
        assert result is not None
        await async_client.reject_message(result.delivery_tag, requeue=False)

        gone = await async_client.consume_message(queue, auto_ack=True)
        assert gone is None

        await async_client.delete_queue(queue)
