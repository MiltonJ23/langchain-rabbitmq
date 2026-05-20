"""Synchronous RabbitMQ client built on top of the ``pika`` library.

This module provides :class:`RabbitMQClient`, a low-level wrapper that
covers every AMQP 0-9-1 operation required by the LangChain tool layer:
queue management, exchange management, message operations, and connection
health probing.

The class is designed to be used as a context manager::

    from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient
    from langchain_rabbitmq.config import RabbitMQSettings

    settings = RabbitMQSettings()
    with RabbitMQClient(settings) as client:
        client.declare_queue("orders")
        client.publish_message(
            exchange="",
            routing_key="orders",
            body=b'{"item": "book"}',
        )

.. warning::
    ``pika.BlockingConnection`` is **not** thread-safe.  Each thread must
    create its own :class:`RabbitMQClient` instance.
"""

from __future__ import annotations

import contextlib
import logging
import ssl
import uuid
from typing import Any, Optional

import pika  # type: ignore[import-untyped]
import pika.exceptions  # type: ignore[import-untyped]

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
from langchain_rabbitmq.utilities._retry import make_sync_retry

logger = logging.getLogger(__name__)

# AMQP name constraints: 1-255 bytes, no control chars (0x00-0x1F / 0x7F).
_MAX_AMQP_NAME_BYTES = 255


def _validate_amqp_name(name: str, label: str) -> None:
    """Raise :class:`~langchain_rabbitmq.exceptions.RabbitMQValidationError` for
    invalid AMQP resource names.

    Args:
        name: The queue or exchange name to validate.
        label: Human-readable label for error messages (e.g. ``"queue"``).

    Raises:
        RabbitMQValidationError: If ``name`` violates AMQP naming rules.
    """
    if len(name.encode()) > _MAX_AMQP_NAME_BYTES:
        raise RabbitMQValidationError(f"{label} name exceeds 255 UTF-8 bytes: {name!r}")
    for ch in name:
        code = ord(ch)
        if code <= 0x1F or code == 0x7F:
            raise RabbitMQValidationError(
                f"{label} name contains control character U+{code:04X}: {name!r}"
            )


def _validate_exchange_name(name: str) -> None:
    """Validate an exchange name, including the broker-reserved ``amq.`` prefix.

    Args:
        name: Exchange name to validate.  The empty string (``""``) is the
            default exchange and is always valid.

    Raises:
        RabbitMQValidationError: If the name is invalid or broker-reserved.
    """
    if name == "":
        return  # default exchange — always valid
    _validate_amqp_name(name, "exchange")
    if name.startswith("amq."):
        raise RabbitMQValidationError(
            f"Exchange names starting with 'amq.' are broker-reserved: {name!r}"
        )


class RabbitMQClient:
    """Synchronous AMQP 0-9-1 client wrapping ``pika.BlockingConnection``.

    All operations automatically reconnect via a tenacity retry policy when
    a transient connection error occurs.  Non-transient errors (channel-level
    protocol violations, validation failures) are raised immediately.

    Args:
        settings: Broker configuration.  Defaults to
            :class:`~langchain_rabbitmq.config.RabbitMQSettings` loaded from
            the environment.

    Example:
        Using as a context manager::

            with RabbitMQClient() as client:
                client.declare_queue("jobs", durable=True)
                client.publish_message(exchange="", routing_key="jobs", body=b"hello")

        Checking broker health::

            client = RabbitMQClient()
            health = client.check_health()
            print(health.status)  # HealthStatus.OK
            client.close()
    """

    def __init__(self, settings: Optional[RabbitMQSettings] = None) -> None:
        self._settings: RabbitMQSettings = settings or RabbitMQSettings()
        # pika objects — None until connect() is called
        self._connection: Optional[Any] = None  # pika.BlockingConnection
        self._channel: Optional[Any] = None  # pika.adapters.blocking_connection.BlockingChannel

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_connection_params(self) -> Any:  # pika.ConnectionParameters
        """Build a ``pika.ConnectionParameters`` from the current settings.

        Returns:
            Configured :class:`pika.ConnectionParameters`.

        Raises:
            RabbitMQConnectionError: If SSL is enabled but certificate paths
                are missing.
        """
        credentials = pika.PlainCredentials(  # type: ignore[attr-defined]
            username=self._settings.username,
            password=self._settings.password.get_secret_value(),
        )

        ssl_options: Optional[pika.SSLOptions] = None  # type: ignore[attr-defined]
        if self._settings.ssl_enabled:
            ctx = ssl.create_default_context()
            if self._settings.ssl_ca_certs:
                ctx.load_verify_locations(cafile=str(self._settings.ssl_ca_certs))
            if self._settings.ssl_certfile and self._settings.ssl_keyfile:
                ctx.load_cert_chain(
                    certfile=str(self._settings.ssl_certfile),
                    keyfile=str(self._settings.ssl_keyfile),
                )
            ssl_options = pika.SSLOptions(context=ctx, server_hostname=self._settings.host)  # type: ignore[attr-defined]

        return pika.ConnectionParameters(  # type: ignore[attr-defined]
            host=self._settings.host,
            port=self._settings.port,
            virtual_host=self._settings.virtual_host,
            credentials=credentials,
            heartbeat=self._settings.heartbeat,
            connection_attempts=1,  # tenacity handles retries externally
            socket_timeout=self._settings.connection_timeout,
            blocked_connection_timeout=self._settings.connection_timeout,
            ssl_options=ssl_options,
        )

    def _require_channel(self) -> Any:  # pika.adapters.blocking_connection.BlockingChannel
        """Return the open channel, opening connection + channel if needed.

        Returns:
            An open pika blocking channel with publisher confirms enabled.

        Raises:
            RabbitMQConnectionError: If the connection cannot be established.
        """
        if (
            self._connection is None or not self._connection.is_open  # type: ignore[attr-defined]
        ):
            self._connect_once()
        assert self._channel is not None  # guaranteed by _connect_once
        return self._channel

    def _connect_once(self) -> None:
        """Open a single pika connection and channel (no retry).

        Raises:
            RabbitMQConnectionError: On any pika connection error.
        """
        try:
            params = self._build_connection_params()
            self._connection = pika.BlockingConnection(params)  # type: ignore[attr-defined]
            self._channel = self._connection.channel()  # type: ignore[attr-defined]
            # Publisher confirms: basic_publish blocks until broker acks
            self._channel.confirm_delivery()  # type: ignore[attr-defined]
        except pika.exceptions.AMQPConnectionError as exc:  # type: ignore[attr-defined]
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

    def connect(self) -> None:
        """Explicitly open the connection.

        This is called automatically on the first broker operation.  You only
        need to call it directly when you want to validate connectivity before
        issuing any AMQP commands.

        Raises:
            RabbitMQConnectionError: If the broker is unreachable.
        """
        for attempt in make_sync_retry(self._settings):
            with attempt:
                self._connect_once()

    def close(self) -> None:
        """Close the channel and connection.

        Idempotent — safe to call multiple times or when already closed.
        """
        try:
            if self._channel is not None and self._channel.is_open:  # type: ignore[attr-defined]
                self._channel.close()  # type: ignore[attr-defined]
        except Exception:
            pass
        finally:
            self._channel = None

        try:
            if self._connection is not None and self._connection.is_open:  # type: ignore[attr-defined]
                self._connection.close()  # type: ignore[attr-defined]
        except Exception:
            pass
        finally:
            self._connection = None

    def __enter__(self) -> RabbitMQClient:
        self.connect()
        return self

    def __exit__(
        self,
        exc_type: Optional[type[BaseException]],
        exc_val: Optional[BaseException],
        exc_tb: Optional[Any],
    ) -> None:
        self.close()

    def __del__(self) -> None:
        # Best-effort cleanup; swallow all errors to avoid issues at
        # interpreter shutdown where globals may already be freed.
        with contextlib.suppress(Exception):
            self.close()

    # ------------------------------------------------------------------
    # Queue management
    # ------------------------------------------------------------------

    def declare_queue(
        self,
        name: str,
        *,
        durable: bool = False,
        exclusive: bool = False,
        auto_delete: bool = False,
        passive: bool = False,
        arguments: Optional[dict[str, Any]] = None,
    ) -> QueueInfo:
        """Declare (or passively inspect) a queue.

        Args:
            name: Queue name.  Pass an empty string to let the broker assign
                a unique name.
            durable: Survive broker restarts.
            exclusive: Limit access to the declaring connection.
            auto_delete: Delete when the last consumer detaches.
            passive: Only check existence; do not create.
            arguments: Optional AMQP x-arguments (e.g. ``{"x-message-ttl": 30000}``).

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.QueueInfo` with
            broker-assigned name and current message/consumer counts.

        Raises:
            RabbitMQValidationError: If ``name`` violates AMQP naming rules.
            RabbitMQChannelError: On protocol-level failures (e.g. parameter mismatch).
            RabbitMQConnectionError: If the broker is unreachable.
        """
        _validate_amqp_name(name, "queue")
        channel = self._require_channel()
        try:
            result = channel.queue_declare(  # type: ignore[attr-defined]
                queue=name,
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                passive=passive,
                arguments=arguments or {},
            )
            return QueueInfo(
                name=result.method.queue,  # type: ignore[attr-defined]
                durable=durable,
                exclusive=exclusive,
                auto_delete=auto_delete,
                message_count=result.method.message_count,  # type: ignore[attr-defined]
                consumer_count=result.method.consumer_count,  # type: ignore[attr-defined]
                arguments=arguments or {},
            )
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None  # channel is now invalid
            raise RabbitMQChannelError(
                f"Broker refused queue.declare for {name!r}: {exc}",
                cause=exc,
            ) from exc

    def delete_queue(
        self,
        name: str,
        *,
        if_unused: bool = False,
        if_empty: bool = False,
    ) -> int:
        """Delete a queue.

        Args:
            name: Queue name.
            if_unused: Only delete if the queue has no consumers.
            if_empty: Only delete if the queue has no messages.

        Returns:
            Number of messages that were discarded.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(name, "queue")
        channel = self._require_channel()
        try:
            result = channel.queue_delete(  # type: ignore[attr-defined]
                queue=name,
                if_unused=if_unused,
                if_empty=if_empty,
            )
            return int(result.method.message_count)  # type: ignore[attr-defined]
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused queue.delete for {name!r}: {exc}",
                cause=exc,
            ) from exc

    def purge_queue(self, name: str) -> int:
        """Remove all ready messages from a queue without deleting it.

        Args:
            name: Queue name.

        Returns:
            Number of messages purged.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(name, "queue")
        channel = self._require_channel()
        try:
            result = channel.queue_purge(queue=name)  # type: ignore[attr-defined]
            return int(result.method.message_count)  # type: ignore[attr-defined]
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused queue.purge for {name!r}: {exc}",
                cause=exc,
            ) from exc

    def bind_queue(
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
            routing_key: Routing key for direct/topic exchanges.
            arguments: Optional binding arguments (headers exchange).

        Raises:
            RabbitMQValidationError: For invalid names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(queue, "queue")
        _validate_exchange_name(exchange)
        channel = self._require_channel()
        try:
            channel.queue_bind(  # type: ignore[attr-defined]
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {},
            )
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused queue.bind {queue!r} ← {exchange!r}: {exc}",
                cause=exc,
            ) from exc

    def unbind_queue(
        self,
        queue: str,
        exchange: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
    ) -> None:
        """Remove a binding between a queue and an exchange.

        Args:
            queue: Queue name.
            exchange: Exchange name.
            routing_key: Binding routing key.
            arguments: Binding arguments used when the binding was created.

        Raises:
            RabbitMQValidationError: For invalid names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(queue, "queue")
        _validate_exchange_name(exchange)
        channel = self._require_channel()
        try:
            channel.queue_unbind(  # type: ignore[attr-defined]
                queue=queue,
                exchange=exchange,
                routing_key=routing_key,
                arguments=arguments or {},
            )
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused queue.unbind {queue!r} ← {exchange!r}: {exc}",
                cause=exc,
            ) from exc

    def get_queue_info(self, name: str) -> QueueInfo:
        """Passively inspect a queue without modifying it.

        Args:
            name: Queue name.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.QueueInfo` with
            current message and consumer counts.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: If the queue does not exist (broker returns
                404 NOT-FOUND).
        """
        return self.declare_queue(name, passive=True)

    # ------------------------------------------------------------------
    # Exchange management
    # ------------------------------------------------------------------

    def declare_exchange(
        self,
        name: str,
        exchange_type: ExchangeType = ExchangeType.DIRECT,
        *,
        durable: bool = False,
        auto_delete: bool = False,
        passive: bool = False,
        arguments: Optional[dict[str, Any]] = None,
    ) -> ExchangeInfo:
        """Declare (or passively inspect) an exchange.

        Args:
            name: Exchange name.  The empty string selects the default exchange
                and is only valid with ``passive=True``.
            exchange_type: AMQP exchange type.
            durable: Survive broker restarts.
            auto_delete: Delete when the last queue unbinds.
            passive: Only check existence; do not create.
            arguments: Optional AMQP x-arguments.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.ExchangeInfo`.

        Raises:
            RabbitMQValidationError: For invalid exchange names or the
                broker-reserved ``amq.`` prefix.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_exchange_name(name)
        channel = self._require_channel()
        try:
            channel.exchange_declare(  # type: ignore[attr-defined]
                exchange=name,
                exchange_type=exchange_type.value,
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
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused exchange.declare for {name!r}: {exc}",
                cause=exc,
            ) from exc

    def delete_exchange(self, name: str, *, if_unused: bool = False) -> None:
        """Delete an exchange.

        Args:
            name: Exchange name.
            if_unused: Only delete if no queues are bound.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_exchange_name(name)
        channel = self._require_channel()
        try:
            channel.exchange_delete(exchange=name, if_unused=if_unused)  # type: ignore[attr-defined]
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused exchange.delete for {name!r}: {exc}",
                cause=exc,
            ) from exc

    def bind_exchange(
        self,
        destination: str,
        source: str,
        routing_key: str = "",
        arguments: Optional[dict[str, Any]] = None,
    ) -> BindingInfo:
        """Create an exchange-to-exchange binding (E2E binding).

        Args:
            destination: Destination exchange name.
            source: Source exchange name.
            routing_key: Routing key for the binding.
            arguments: Optional binding arguments.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.BindingInfo`.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_exchange_name(destination)
        _validate_exchange_name(source)
        channel = self._require_channel()
        try:
            channel.exchange_bind(  # type: ignore[attr-defined]
                destination=destination,
                source=source,
                routing_key=routing_key,
                arguments=arguments or {},
            )
            return BindingInfo(
                source=source,
                destination=destination,
                destination_type="exchange",
                routing_key=routing_key,
                arguments=arguments or {},
            )
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused exchange.bind {destination!r} ← {source!r}: {exc}",
                cause=exc,
            ) from exc

    # ------------------------------------------------------------------
    # Message operations
    # ------------------------------------------------------------------

    def publish_message(
        self,
        exchange: str,
        routing_key: str,
        body: bytes,
        *,
        persistent: bool = False,
        content_type: Optional[str] = None,
        content_encoding: Optional[str] = None,
        headers: Optional[dict[str, Any]] = None,
        expiration: Optional[str] = None,
        mandatory: bool = False,
    ) -> None:
        """Publish a single message to an exchange.

        With publisher confirms enabled (``confirm_delivery``), this call
        blocks until the broker acknowledges receipt.

        Args:
            exchange: Target exchange name.  Use ``""`` for the default exchange.
            routing_key: Routing key (or queue name when using default exchange).
            body: Raw message payload.
            persistent: Use delivery mode 2 (survives broker restart).
            content_type: MIME type header (e.g. ``"application/json"``).
            content_encoding: Encoding header (e.g. ``"utf-8"``).
            headers: AMQP headers table.
            expiration: Per-message TTL in milliseconds as a string (e.g. ``"60000"``).
            mandatory: Raise :class:`~langchain_rabbitmq.exceptions.RabbitMQMessageError`
                if the message is unroutable.

        Raises:
            RabbitMQValidationError: For invalid exchange names.
            RabbitMQMessageError: If publish fails or mandatory message is unroutable.
            RabbitMQConnectionError: If the broker is unreachable.
        """
        _validate_exchange_name(exchange)
        properties = pika.BasicProperties(  # type: ignore[attr-defined]
            delivery_mode=2 if persistent else 1,
            content_type=content_type,
            content_encoding=content_encoding,
            headers=headers,
            expiration=expiration,
        )
        channel = self._require_channel()
        try:
            channel.basic_publish(  # type: ignore[attr-defined]
                exchange=exchange,
                routing_key=routing_key,
                body=body,
                properties=properties,
                mandatory=mandatory,
            )
        except pika.exceptions.UnroutableError as exc:  # type: ignore[attr-defined]
            raise RabbitMQMessageError(
                f"Message unroutable: exchange={exchange!r}, routing_key={routing_key!r}",
                cause=exc,
            ) from exc
        except pika.exceptions.NackError as exc:  # type: ignore[attr-defined]
            raise RabbitMQMessageError(
                f"Broker nack'd publish to exchange={exchange!r}, routing_key={routing_key!r}",
                cause=exc,
            ) from exc

    def consume_message(self, queue: str, *, auto_ack: bool = False) -> Optional[MessageResult]:
        """Pull a single message from a queue (``basic.get``).

        Args:
            queue: Queue name.
            auto_ack: Automatically acknowledge the message on delivery.
                When ``False``, the caller must call :meth:`ack_message`,
                :meth:`nack_message`, or :meth:`reject_message`.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.MessageResult` if
            a message is available, ``None`` if the queue is empty.

        Raises:
            RabbitMQValidationError: For invalid queue names.
            RabbitMQChannelError: On broker refusal.
        """
        _validate_amqp_name(queue, "queue")
        channel = self._require_channel()
        try:
            method, properties, body = channel.basic_get(  # type: ignore[attr-defined]
                queue=queue, auto_ack=auto_ack
            )
        except pika.exceptions.ChannelClosedByBroker as exc:  # type: ignore[attr-defined]
            self._channel = None
            raise RabbitMQChannelError(
                f"Broker refused basic.get on queue {queue!r}: {exc}",
                cause=exc,
            ) from exc

        if method is None:
            return None  # queue is empty

        hdrs: dict[str, Any] = {}
        if properties is not None and properties.headers:  # type: ignore[attr-defined]
            hdrs = dict(properties.headers)  # type: ignore[attr-defined]

        return MessageResult(
            body=body,
            delivery_tag=int(method.delivery_tag),  # type: ignore[attr-defined]
            exchange=str(method.exchange),  # type: ignore[attr-defined]
            routing_key=str(method.routing_key),  # type: ignore[attr-defined]
            redelivered=bool(method.redelivered),  # type: ignore[attr-defined]
            headers=hdrs,
            content_type=str(properties.content_type)
            if properties is not None and properties.content_type
            else None,  # type: ignore[attr-defined]
            content_encoding=str(properties.content_encoding)
            if properties is not None and properties.content_encoding
            else None,  # type: ignore[attr-defined]
        )

    def ack_message(self, delivery_tag: int, *, multiple: bool = False) -> None:
        """Acknowledge a message.

        Args:
            delivery_tag: Broker delivery tag from :class:`.MessageResult`.
            multiple: Acknowledge all messages up to and including this tag.

        Raises:
            RabbitMQMessageError: If the channel is not open.
        """
        channel = self._require_channel()
        try:
            channel.basic_ack(delivery_tag=delivery_tag, multiple=multiple)  # type: ignore[attr-defined]
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Failed to ack delivery_tag={delivery_tag}: {exc}", cause=exc
            ) from exc

    def nack_message(
        self,
        delivery_tag: int,
        *,
        multiple: bool = False,
        requeue: bool = True,
    ) -> None:
        """Negatively acknowledge a message.

        Args:
            delivery_tag: Broker delivery tag.
            multiple: Nack all messages up to and including this tag.
            requeue: Requeue the message(s).  Set to ``False`` to discard.

        Raises:
            RabbitMQMessageError: On channel errors.
        """
        channel = self._require_channel()
        try:
            channel.basic_nack(  # type: ignore[attr-defined]
                delivery_tag=delivery_tag, multiple=multiple, requeue=requeue
            )
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Failed to nack delivery_tag={delivery_tag}: {exc}", cause=exc
            ) from exc

    def reject_message(self, delivery_tag: int, *, requeue: bool = True) -> None:
        """Reject a single message.

        Args:
            delivery_tag: Broker delivery tag.
            requeue: Requeue the message.

        Raises:
            RabbitMQMessageError: On channel errors.
        """
        channel = self._require_channel()
        try:
            channel.basic_reject(  # type: ignore[attr-defined]
                delivery_tag=delivery_tag, requeue=requeue
            )
        except Exception as exc:
            raise RabbitMQMessageError(
                f"Failed to reject delivery_tag={delivery_tag}: {exc}", cause=exc
            ) from exc

    # ------------------------------------------------------------------
    # Health & connection info
    # ------------------------------------------------------------------

    def check_health(self) -> HealthInfo:
        """Probe broker connectivity by declaring a transient health-check queue.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.HealthInfo` with
            ``status=HealthStatus.OK`` on success or ``HealthStatus.DOWN``
            if the broker is unreachable.
        """
        probe_name = f"lgrabbitmq.health.{uuid.uuid4().hex}"
        try:
            channel = self._require_channel()
            # Declare a server-named exclusive queue as a connectivity probe.
            channel.queue_declare(  # type: ignore[attr-defined]
                queue=probe_name,
                durable=False,
                exclusive=True,
                auto_delete=True,
            )
            # Immediately delete — it was just a probe.
            channel.queue_delete(queue=probe_name)  # type: ignore[attr-defined]
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
        except Exception as exc:
            return HealthInfo(
                status=HealthStatus.DEGRADED,
                host=self._settings.host,
                port=self._settings.port,
                message=f"Health probe failed: {exc}",
            )

    def get_connection_info(self) -> ConnectionInfo:
        """Return metadata about the active AMQP connection.

        Returns:
            :class:`~langchain_rabbitmq.utilities._models.ConnectionInfo`.
            If no connection is open, ``connected`` will be ``False``.
        """
        if self._connection is None or not self._connection.is_open:  # type: ignore[attr-defined]
            return ConnectionInfo(
                host=self._settings.host,
                port=self._settings.port,
                virtual_host=self._settings.virtual_host,
                connected=False,
            )

        # pika stores server properties as bytes or str depending on version;
        # .decode() is called defensively to normalise to str.
        server_props: dict[str, Any] = dict(
            self._connection.server_properties  # type: ignore[attr-defined]
        )

        def _decode(val: Any) -> Optional[str]:
            if isinstance(val, bytes):
                return val.decode("utf-8", errors="replace")
            if isinstance(val, str):
                return val
            return None

        return ConnectionInfo(
            host=self._settings.host,
            port=self._settings.port,
            virtual_host=self._settings.virtual_host,
            server_version=_decode(server_props.get("version")),
            server_platform=_decode(server_props.get("platform")),
            connected=True,
        )


__all__: list[str] = ["RabbitMQClient"]
