"""Asynchronous RabbitMQ client built on top of the ``aio-pika`` library.

This module provides :class:`AsyncRabbitMQClient`, mirroring every operation
available in :class:`~langchain_rabbitmq.utilities.rabbitmq.RabbitMQClient`
but using ``asyncio``-native primitives.

Example:
    Using as an async context manager::

        import asyncio
        from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient

        async def main() -> None:
            async with AsyncRabbitMQClient() as client:
                await client.declare_queue("orders", durable=True)
                await client.publish_message(
                    exchange="",
                    routing_key="orders",
                    body=b'{"item": "book"}',
                )

        asyncio.run(main())
"""

from __future__ import annotations

import logging
import ssl
import uuid
from typing import Any, Optional

import aio_pika  # type: ignore[import-untyped]
import aio_pika.exceptions  # type: ignore[import-untyped]

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import (
    RabbitMQChannelError,
    RabbitMQConnectionError,
    RabbitMQMessageError,
    RabbitMQValidationError,
)
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
from langchain_rabbitmq.utilities._retry import make_async_retry
from langchain_rabbitmq.utilities.rabbitmq import (
    _validate_amqp_name,
    _validate_exchange_name,
)

logger = logging.getLogger(__name__)


class AsyncRabbitMQClient:
    """Asynchronous AMQP 0-9-1 client wrapping ``aio_pika.RobustConnection``.

    Uses ``connect_robust`` for automatic reconnection on transient failures.
    All public methods are ``async`` and must be awaited.

    Args:
        settings: Broker configuration.  Defaults to
            :class:`~langchain_rabbitmq.config.RabbitMQSettings` loaded from
            the environment.

    Example:
        Publish and immediately consume a message::

            async with AsyncRabbitMQClient() as client:
                await client.declare_queue("test", durable=False)
                await client.publish_message("", "test", b"hello")
                msg = await client.consume_message("test")
                if msg:
                    await client.ack_message(msg.delivery_tag)
    """

    def __init__(self, settings: Optional[RabbitMQSettings] = None) -> None:
        self._settings: RabbitMQSettings = settings or RabbitMQSettings()
        self._connection: Optional[Any] = None  # aio_pika.RobustConnection
        self._channel: Optional[Any] = None  # aio_pika.RobustChannel
        # Maps delivery_tag → IncomingMessage for manual ack/nack/reject
        self._pending_messages: dict[int, Any] = {}

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_url(self) -> str:
        """Return the AMQP URL for ``aio_pika.connect_robust``.

        Returns:
            Full ``amqp[s]://…`` URL string.
        """
        return self._settings.amqp_url

    def _build_ssl_context(self) -> Optional[ssl.SSLContext]:
        """Build an :class:`ssl.SSLContext` from settings, or ``None``.

        Returns:
            Configured :class:`ssl.SSLContext` when SSL is enabled,
            ``None`` otherwise.
        """
        if not self._settings.ssl_enabled:
            return None
        ctx = ssl.create_default_context()
        if self._settings.ssl_ca_certs:
            ctx.load_verify_locations(cafile=str(self._settings.ssl_ca_certs))
        if self._settings.ssl_certfile and self._settings.ssl_keyfile:
            ctx.load_cert_chain(
                certfile=str(self._settings.ssl_certfile),
                keyfile=str(self._settings.ssl_keyfile),
            )
        return ctx

    async def _require_channel(self) -> Any:  # aio_pika.RobustChannel
        """Return an open channel, opening connection + channel if needed.

        Returns:
            An open aio-pika channel.

        Raises:
            RabbitMQConnectionError: If the connection cannot be established.
        """
        if self._connection is None or self._connection.is_closed:  # type: ignore[attr-defined]
            await self._connect_once()
        if self._channel is None or self._channel.is_closed:  # type: ignore[attr-defined]
            assert self._connection is not None
            self._channel = await self._connection.channel()  # type: ignore[attr-defined]
        return self._channel

    async def _connect_once(self) -> None:
        """Open a single aio-pika connection (no retry wrapper).

        Raises:
            RabbitMQConnectionError: On any connection failure.
        """
        ssl_context = self._build_ssl_context()
        try:
            self._connection = await aio_pika.connect_robust(  # type: ignore[attr-defined]
                url=self._build_url(),
                ssl=self._settings.ssl_enabled,
                ssl_context=ssl_context,
                heartbeat=self._settings.heartbeat,
                timeout=self._settings.connection_timeout,
            )
            self._channel = await self._connection.channel()  # type: ignore[attr-defined]
            # Enable publisher confirms
            await self._channel.set_qos(prefetch_count=0)  # type: ignore[attr-defined]
        except aio_pika.exceptions.AMQPConnectionError as exc:  # type: ignore[attr-defined]
            raise RabbitMQConnectionError(
                f"Cannot connect to RabbitMQ at {self._settings.host}:{self._settings.port}",
                cause=exc,
            ) from exc
        except Exception as exc:
            raise RabbitMQConnectionError(
                f"Unexpected error while connecting to RabbitMQ: {exc}",
                cause=exc,
            ) from exc

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def connect(self) -> None:
        """Explicitly open the connection with retry.

        Raises:
            RabbitMQConnectionError: If all attempts fail.
        """
        async for attempt in make_async_retry(self._settings):
            with attempt:
                await self._connect_once()

    async def close(self) -> None:
        """Close the channel and connection.

        Idempotent — safe to call multiple times.
        """
        self._pending_messages.clear()
        try:
            if self._channel is not None and not self._channel.is_closed:  # type: ignore[attr-defined]
                await self._channel.close()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._channel = None

        try:
            if self._connection is not None and not self._connection.is_closed:  # type: ignore[attr-defined]
                await self._connection.close()  # type: ignore[attr-defined]
        except Exception:  # noqa: BLE001
            pass
        finally:
            self._connection = None

    async def __aenter__(self) -> "AsyncRabbitMQClient":
        await self.connect()
        return self

    async def __aexit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    async def declare_queue(
        self,
        name: str,
        *,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        passive: bool = False,
        arguments: Optional[dict[str, Any]] = None,
    ) -> QueueInfo:
        """Declare or passively inspect a queue.

        Args:
            name: Queue name.
            durable: Survive broker restarts.
            exclusive: Limit to the declaring connection.
            auto_delete: Delete when the last consumer detaches.
            passive: Only check existence; do not create.
            arguments: AMQP x-arguments.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.QueueInfo`.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
            RabbitMQConnectionError: If the broker is unreachable.
        """
        _validate_amqp_name(name, "queue")
        channel = await self._require_channel()
        try:
            queue = await channel.declare_queue(  # type: ignore[attr-defined]
                name=name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                passive=passive,
                arguments=arguments or {},
            )
            return QueueInfo(
                name=queue.name,  # type: ignore[attr-defined]
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                message_count=getattr(queue, "declaration_result", None)  # type: ignore[attr-defined]
                and queue.declaration_result.message_count or 0,  # type: ignore[attr-defined]
                consumer_count=getattr(queue, "declaration_result", None)  # type: ignore[attr-defined]
                and queue.declaration_result.consumer_count or 0,  # type: ignore[attr-defined]
                arguments=arguments or {},
            )
        except aio_pika.exceptions.ChannelNotFoundEntity as exc:  # type: ignore[attr-defined]
            raise RabbitMQChannelError(
                f"Queue {name!r} does not exist (passive check)", cause=exc
            ) from exc
        except aio_pika.exceptions.ChannelPreconditionFailed as exc:  # type: ignore[attr-defined]
            raise RabbitMQChannelError(
                f"Queue {name!r} already exists with different parameters", cause=exc
            ) from exc

    async def delete_queue(
        self,
        name: str,
        *,
        if_unused: bool = False,
        if_empty: bool = False,
    ) -> int:
        """Delete a queue.

        Args:
            name: Queue name.
            if_unused: Only delete if no consumers are attached.
            if_empty: Only delete if the queue is empty.

        Returns:
            Number of messages discarded.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(name, "queue")
        channel = await self._require_channel()
        try:
            # aio-pika: channel.queue_delete() returns message_count int
            result = await channel.queue_delete(  # type: ignore[attr-defined]
                queue_name=name,
                if_unused=if_unused,
                if_empty=if_empty,
            )
            return int(result.message_count)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to delete queue {name!r}: {exc}", cause=exc
            ) from exc

    async def purge_queue(self, name: str) -> int:
        """Remove all ready messages from a queue.

        Args:
            name: Queue name.

        Returns:
            Number of messages purged.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(name, "queue")
        channel = await self._require_channel()
        try:
            # Declare passive to get the aio-pika Queue object, then purge
            q = await channel.declare_queue(name, passive=True)  # type: ignore[attr-defined]
            result = await q.purge()  # type: ignore[attr-defined]
            return int(result.message_count)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to purge queue {name!r}: {exc}", cause=exc
            ) from exc

    async def bind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
    ) -> None:
        """Bind a queue to an exchange.

        Args:
            queue: Destination queue name.
            exchange: Source exchange name.
            routing_key: Routing key.
            arguments: Optional binding arguments.

        Raises:
            RabbitMQValidationError: For invalid names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(queue, "queue")
        _validate_exchange_name(exchange)
        channel = await self._require_channel()
        try:
            # Get the Queue object via passive declare, then call bind()
            q = await channel.declare_queue(queue, passive=True)  # type: ignore[attr-defined]
            await q.bind(  # type: ignore[attr-defined]
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {},
            )
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to bind queue {queue!r} to exchange {exchange!r}: {exc}",
                cause=exc,
            ) from exc

    async def unbind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
    ) -> None:
        """Remove a queue-exchange binding.

        Args:
            queue: Queue name.
            exchange: Exchange name.
            routing_key: Binding routing key.
            arguments: Binding arguments used at bind time.

        Raises:
            RabbitMQValidationError: For invalid names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(queue, "queue")
        _validate_exchange_name(exchange)
        channel = await self._require_channel()
        try:
            q = await channel.declare_queue(queue, passive=True)  # type: ignore[attr-defined]
            await q.unbind(  # type: ignore[attr-defined]
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {},
            )
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to unbind queue {queue!r} from exchange {exchange!r}: {exc}",
                cause=exc,
            ) from exc

    async def get_queue_info(self, name: str) -> QueueInfo:
        """Passively inspect a queue.

        Args:
            name: Queue name.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.QueueInfo`.

        Raises:
            RabbitMQChannelError: If the queue does not exist.
        """
        return await self.declare_queue(name, passive=True)

    # ------------------------------------------------------------------
    # Exchange management
    # ------------------------------------------------------------------

    async def declare_exchange(
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        *,
        durable: bool = False,
        auto_delete: bool = False,
        passive: bool = False,
        arguments: Optional[dict[str, Any]] = None,
    ) -> ExchangeInfo:
        """Declare or passively inspect an exchange.

        Args:
            name: Exchange name.
            exchange_type: AMQP exchange type.
            durable: Survive broker restarts.
            auto_delete: Delete when last queue unbinds.
            passive: Only check existence.
            arguments: AMQP x-arguments.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.ExchangeInfo`.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_exchange_name(name)
        channel = await self._require_channel()
        try:
            # Map to aio-pika's ExchangeType enum by value (string)
            aio_exchange_type = aio_pika.ExchangeType(exchange_type.value)  # type: ignore[attr-defined]
            await channel.declare_exchange(  # type: ignore[attr-defined]
                name=name,
                type=aio_exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                passive=passive,
                arguments=arguments or {},
            )
            return ExchangeInfo(
                name=name,
                exchange_type=exchange_type,
                durable=durable,
                auto_delete=auto_delete,
                arguments=arguments or {},
            )
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to declare exchange {name!r}: {exc}", cause=exc
            ) from exc

    async def delete_exchange(self, name: str, *, if_unused: bool = False) -> None:
        """Delete an exchange.

        Args:
            name: Exchange name.
            if_unused: Only delete if no queues are bound.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_exchange_name(name)
        channel = await self._require_channel()
        try:
            await channel.exchange_delete(  # type: ignore[attr-defined]
                exchange_name=name, if_unused=if_unused
            )
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to delete exchange {name!r}: {exc}", cause=exc
            ) from exc

    async def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
    ) -> BindingInfo:
        """Create an exchange-to-exchange binding.

        Args:
            destination: Destination exchange name.
            source: Source exchange name.
            routing_key: Binding routing key.
            arguments: Optional binding arguments.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.BindingInfo`.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_exchange_name(destination)
        _validate_exchange_name(source)
        channel = await self._require_channel()
        try:
            # Passive declare returns the Exchange object without creating it.
            # The type argument is irrelevant for passive=True.
            dest = await channel.declare_exchange(  # type: ignore[attr-defined]
                destination, passive=True
            )
            await dest.bind(  # type: ignore[attr-defined]
                source, routing_key=routing_key, arguments=arguments or {}
            )
            return BindingInfo(
                source=source,
                destination=destination,
                destination_type="exchange",
                routing_key=routing_key,
                arguments=arguments or {},
            )
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to bind exchange {destination!r} ← {source!r}: {exc}",
                cause=exc,
            ) from exc

    # ------------------------------------------------------------------
    # Message operations
    # ------------------------------------------------------------------

    async def publish_message(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        *,
        persistent: bool = False,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        headers: Optional[dict[str, Any]] = None,
        expiration: Optional[int] = None,
    ) -> None:
        """Publish a single message.

        Args:
            exchange: Target exchange name (``""`` = default exchange).
            routing_key: Routing key.
            body: Message payload.
            persistent: Use delivery mode 2.
            content_type: MIME type header.
            content_encoding: Encoding header.
            headers: AMQP headers table.
            expiration: Per-message TTL in milliseconds.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQMessageError: On publish failure.
        """
        _validate_exchange_name(exchange)
        channel = await self._require_channel()
        try:
            message = aio_pika.Message(  # type: ignore[attr-defined]
                body=body,
                delivery_mode=aio_pika.DeliveryMode.PERSISTENT if persistent else aio_pika.DeliveryMode.NOT_PERSISTENT,  # type: ignore[attr-defined]
                content_type=content_type,
                content_encoding=content_encoding,
                headers=headers,
                expiration=expiration,
            )
            if exchange == "":
                # Default exchange: use channel.default_exchange
                await channel.default_exchange.publish(  # type: ignore[attr-defined]
                    message, routing_key=routing_key
                )
            else:
                exch = await channel.get_exchange(exchange)  # type: ignore[attr-defined]
                await exch.publish(message, routing_key=routing_key)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Publish failed to exchange={exchange!r}, routing_key={routing_key!r}: {exc}",
                cause=exc,
            ) from exc

    async def consume_message(
        self,
        queue: str,
        *,
        auto_ack: bool = False,
    ) -> Optional[MessageResult]:
        """Pull a single message via ``basic.get``.

        Args:
            queue: Queue name.
            auto_ack: Auto-acknowledge on delivery.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.MessageResult` or
            ``None`` if the queue is empty.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(queue, "queue")
        channel = await self._require_channel()
        try:
            # aio-pika's get() raises QueueEmpty when no message available.
            q = await channel.get_queue(queue, ensure=False)  # type: ignore[attr-defined]
            msg = await q.get(no_ack=auto_ack, fail=False)  # type: ignore[attr-defined]
        except aio_pika.exceptions.QueueEmpty:  # type: ignore[attr-defined]
            return None
        except Exception as exc:
            raise RabbitMQChannelError(
                f"Failed to get message from queue {queue!r}: {exc}", cause=exc
            ) from exc

        if msg is None:
            return None

        delivery_tag = int(msg.delivery_tag)  # type: ignore[attr-defined]
        # Store the raw IncomingMessage so callers can ack/nack/reject it.
        if not auto_ack:
            self._pending_messages[delivery_tag] = msg

        hdrs: dict[str, Any] = {}
        if msg.headers:  # type: ignore[attr-defined]
            hdrs = dict(msg.headers)  # type: ignore[attr-defined]

        return MessageResult(
            body=bytes(msg.body),  # type: ignore[attr-defined]
            delivery_tag=delivery_tag,
            exchange=str(msg.exchange),  # type: ignore[attr-defined]
            routing_key=str(msg.routing_key),  # type: ignore[attr-defined]
            redelivered=bool(msg.redelivered),  # type: ignore[attr-defined]
            headers=hdrs,
            content_type=msg.content_type,  # type: ignore[attr-defined]
            content_encoding=msg.content_encoding,  # type: ignore[attr-defined]
        )

    async def ack_message(self, delivery_tag: int, *, multiple: bool = False) -> None:
        """Acknowledge a message.

        Args:
            delivery_tag: Delivery tag from :class:`.MessageResult`.
            multiple: Acknowledge all messages up to this tag.

        Raises:
            RabbitMQMessageError: On channel errors.
        """
        msg = self._pending_messages.pop(delivery_tag, None)
        if msg is None:
            raise RabbitMQMessageError(
                f"No pending message for delivery_tag={delivery_tag}. "
                "The message may have been auto-acked or already acknowledged.",
                cause=None,
            )
        try:
            await msg.ack(multiple=multiple)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Failed to ack delivery_tag={delivery_tag}: {exc}", cause=exc
            ) from exc

    async def nack_message(
        self,
        delivery_tag: int,
        *,
        multiple: bool = False,
        requeue: bool = True,
    ) -> None:
        """Negatively acknowledge a message.

        Args:
            delivery_tag: Delivery tag.
            multiple: Nack all messages up to this tag.
            requeue: Requeue the message(s).

        Raises:
            RabbitMQMessageError: On channel errors.
        """
        msg = self._pending_messages.pop(delivery_tag, None)
        if msg is None:
            raise RabbitMQMessageError(
                f"No pending message for delivery_tag={delivery_tag}.",
                cause=None,
            )
        try:
            await msg.nack(multiple=multiple, requeue=requeue)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Failed to nack delivery_tag={delivery_tag}: {exc}", cause=exc
            ) from exc

    async def reject_message(self, delivery_tag: int, *, requeue: bool = True) -> None:
        """Reject a single message.

        Args:
            delivery_tag: Delivery tag.
            requeue: Requeue the message.

        Raises:
            RabbitMQMessageError: On channel errors.
        """
        msg = self._pending_messages.pop(delivery_tag, None)
        if msg is None:
            raise RabbitMQMessageError(
                f"No pending message for delivery_tag={delivery_tag}.",
                cause=None,
            )
        try:
            await msg.reject(requeue=requeue)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Failed to reject delivery_tag={delivery_tag}: {exc}", cause=exc
            ) from exc

    # ------------------------------------------------------------------
    # Health & connection info
    # ------------------------------------------------------------------

    async def check_health(self) -> HealthInfo:
        """Probe broker connectivity.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.HealthInfo`.
        """
        probe_name = f"lgrabbitmq.health.{uuid.uuid4().hex}"
        try:
            channel = await self._require_channel()
            queue = await channel.declare_queue(  # type: ignore[attr-defined]
                name=probe_name,
                durable=False,
                exclusive=True,
                auto_delete=True,
            )
            await queue.delete()  # type: ignore[attr-defined]
            return HealthInfo(
                status=HealthStatus.OK,
                host=self._settings.host,
                port=self._settings.port,
                message="Broker connection healthy",
            )
        except RabbitMQConnectionError as exc:
            return HealthInfo(
                status=HealthStatus.DOWN,
                host=self._settings.host,
                port=self._settings.port,
                message=exc.message,
            )
        except Exception as exc:  # noqa: BLE001
            return HealthInfo(
                status=HealthStatus.DEGRADED,
                host=self._settings.host,
                port=self._settings.port,
                message=f"Health probe failed: {exc}",
            )

    async def get_connection_info(self) -> ConnectionInfo:
        """Return metadata about the active AMQP connection.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.ConnectionInfo`.
        """
        if self._connection is None or self._connection.is_closed:  # type: ignore[attr-defined]
            return ConnectionInfo(
                host=self._settings.host,
                port=self._settings.port,
                virtual_host=self._settings.virtual_host,
                connected=False,
            )
        return ConnectionInfo(
            host=self._settings.host,
            port=self._settings.port,
            virtual_host=self._settings.virtual_host,
            connected=True,
        )


__all__: list[str] = ["AsyncRabbitMQClient"]
