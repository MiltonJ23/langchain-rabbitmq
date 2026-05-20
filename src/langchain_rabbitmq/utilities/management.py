"""RabbitMQ Management HTTP API client.

Wraps the RabbitMQ Management Plugin REST API (port 15672 by default)
to support admin and monitoring operations that are not available over
the AMQP protocol.

Both synchronous (``httpx.Client``) and asynchronous (``httpx.AsyncClient``)
variants are provided.

Example:
    Listing all queues via the sync client::

        from langchain_rabbitmq.utilities.management import ManagementAPIClient
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings()
        with ManagementAPIClient(settings) as client:
            queues = client.list_queues()
            for q in queues:
                print(q["name"], q["messages"])

.. note::
    The Management Plugin must be enabled on the broker::

        rabbitmq-plugins enable rabbitmq_management

    And ``RABBITMQ_MANAGEMENT_API_URL`` must point to its base URL,
    e.g. ``http://localhost:15672``.
"""

from __future__ import annotations

import logging
import urllib.parse
from typing import Any, Optional

import httpx

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import RabbitMQAdminError

logger = logging.getLogger(__name__)

_DEFAULT_TIMEOUT = 10.0  # seconds


def _encode_vhost(vhost: str) -> str:
    """URL-encode a virtual host name for use in Management API paths.

    The default vhost ``/`` must be encoded as ``%2F``.

    Args:
        vhost: Virtual host name.

    Returns:
        URL-safe string suitable for embedding in a path segment.
    """
    return urllib.parse.quote(vhost, safe="")


class ManagementAPIClient:
    """Synchronous RabbitMQ Management HTTP API client.

    Uses ``httpx.Client`` with Basic authentication.

    Args:
        settings: Broker and management API configuration.

    Example:
        Using as a context manager::

            settings = RabbitMQSettings()
            with ManagementAPIClient(settings) as client:
                overview = client.get_overview()
                print(overview["rabbitmq_version"])
    """

    def __init__(self, settings: Optional[RabbitMQSettings] = None) -> None:
        self._settings: RabbitMQSettings = settings or RabbitMQSettings()
        self._client: Optional[httpx.Client] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_client(self) -> httpx.Client:
        if self._client is None or self._client.is_closed:
            api_url = self._settings.management_api_url
            if api_url is None:
                raise RabbitMQAdminError(
                    "management_api_url is required for Management API operations. "
                    "Set the RABBITMQ_MANAGEMENT_API_URL environment variable."
                )
            self._client = httpx.Client(
                base_url=api_url,
                auth=(
                    self._settings.effective_management_username,
                    self._settings.effective_management_password,
                ),
                timeout=_DEFAULT_TIMEOUT,
            )
        return self._client

    def close(self) -> None:
        """Close the underlying HTTP client. Idempotent."""
        if self._client is not None and not self._client.is_closed:
            self._client.close()
        self._client = None

    def __enter__(self) -> ManagementAPIClient:
        self._ensure_client()
        return self

    def __exit__(self, *_: Any) -> None:
        self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get(self, path: str) -> Any:
        """Perform a GET request and return the parsed JSON body.

        Args:
            path: API path relative to the base URL (e.g. ``"/api/queues"``).

        Returns:
            Parsed JSON response (list or dict).

        Raises:
            RabbitMQAdminError: On HTTP errors or connection failures.
        """
        client = self._ensure_client()
        try:
            response = client.get(path)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            raise RabbitMQAdminError(
                f"Management API {exc.request.url} returned {exc.response.status_code}",
                cause=exc,
            ) from exc
        except httpx.RequestError as exc:
            raise RabbitMQAdminError(
                f"Management API request failed for {path}: {exc}",
                cause=exc,
            ) from exc

    # ------------------------------------------------------------------
    # API methods
    # ------------------------------------------------------------------

    def list_queues(self, vhost: Optional[str] = None) -> list[dict[str, Any]]:
        """List all queues, optionally filtered by virtual host.

        Args:
            vhost: Virtual host to filter by.  ``None`` lists queues across
                all vhosts.

        Returns:
            List of queue info dicts from the Management API.

        Raises:
            RabbitMQAdminError: On HTTP or connection errors.
        """
        path = f"/api/queues/{_encode_vhost(vhost)}" if vhost else "/api/queues"
        return list(self._get(path))  # type: ignore[arg-type]

    def list_exchanges(self, vhost: Optional[str] = None) -> list[dict[str, Any]]:
        """List all exchanges, optionally filtered by virtual host.

        Args:
            vhost: Virtual host to filter by.

        Returns:
            List of exchange info dicts.

        Raises:
            RabbitMQAdminError: On HTTP or connection errors.
        """
        path = f"/api/exchanges/{_encode_vhost(vhost)}" if vhost else "/api/exchanges"
        return list(self._get(path))  # type: ignore[arg-type]

    def list_bindings(self, vhost: Optional[str] = None) -> list[dict[str, Any]]:
        """List all bindings, optionally filtered by virtual host.

        Args:
            vhost: Virtual host to filter by.

        Returns:
            List of binding info dicts.

        Raises:
            RabbitMQAdminError: On HTTP or connection errors.
        """
        path = f"/api/bindings/{_encode_vhost(vhost)}" if vhost else "/api/bindings"
        return list(self._get(path))  # type: ignore[arg-type]

    def get_node_stats(self) -> list[dict[str, Any]]:
        """Return statistics for all cluster nodes.

        Returns:
            List of node stat dicts (name, type, memory, fd_used, etc.).

        Raises:
            RabbitMQAdminError: On HTTP or connection errors.
        """
        return list(self._get("/api/nodes"))  # type: ignore[arg-type]

    def get_overview(self) -> dict[str, Any]:
        """Return broker overview (version, rates, object counts).

        Returns:
            Overview dict from ``/api/overview``.

        Raises:
            RabbitMQAdminError: On HTTP or connection errors.
        """
        return dict(self._get("/api/overview"))  # type: ignore[arg-type]


class AsyncManagementAPIClient:
    """Asynchronous RabbitMQ Management HTTP API client.

    Uses ``httpx.AsyncClient`` with Basic authentication.

    Args:
        settings: Broker and management API configuration.

    Example:
        Using as an async context manager::

            async with AsyncManagementAPIClient() as client:
                queues = await client.list_queues()
    """

    def __init__(self, settings: Optional[RabbitMQSettings] = None) -> None:
        self._settings: RabbitMQSettings = settings or RabbitMQSettings()
        self._client: Optional[httpx.AsyncClient] = None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    def _ensure_client(self) -> httpx.AsyncClient:
        if self._client is None or self._client.is_closed:
            api_url = self._settings.management_api_url
            if api_url is None:
                raise RabbitMQAdminError(
                    "management_api_url is required for Management API operations. "
                    "Set the RABBITMQ_MANAGEMENT_API_URL environment variable."
                )
            self._client = httpx.AsyncClient(
                base_url=api_url,
                auth=(
                    self._settings.effective_management_username,
                    self._settings.effective_management_password,
                ),
                timeout=_DEFAULT_TIMEOUT,
            )
        return self._client

    async def close(self) -> None:
        """Close the async HTTP client. Idempotent."""
        if self._client is not None and not self._client.is_closed:
            await self._client.aclose()
        self._client = None

    async def __aenter__(self) -> AsyncManagementAPIClient:
        self._ensure_client()
        return self

    async def __aexit__(self, *_: Any) -> None:
        await self.close()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _aget(self, path: str) -> Any:
        """Perform an async GET and return parsed JSON.

        Args:
            path: API path relative to the base URL.

        Returns:
            Parsed JSON response.

        Raises:
            RabbitMQAdminError: On HTTP errors or connection failures.
        """
        client = self._ensure_client()
        try:
            response = await client.get(path)
            response.raise_for_status()
            return response.json()
        except httpx.HTTPStatusError as exc:
            raise RabbitMQAdminError(
                f"Management API {exc.request.url} returned {exc.response.status_code}",
                cause=exc,
            ) from exc
        except httpx.RequestError as exc:
            raise RabbitMQAdminError(
                f"Management API request failed for {path}: {exc}",
                cause=exc,
            ) from exc

    # ------------------------------------------------------------------
    # API methods
    # ------------------------------------------------------------------

    async def list_queues(self, vhost: Optional[str] = None) -> list[dict[str, Any]]:
        """List all queues asynchronously.

        Args:
            vhost: Optional virtual host filter.

        Returns:
            List of queue info dicts.
        """
        path = f"/api/queues/{_encode_vhost(vhost)}" if vhost else "/api/queues"
        return list(await self._aget(path))  # type: ignore[arg-type]

    async def list_exchanges(self, vhost: Optional[str] = None) -> list[dict[str, Any]]:
        """List all exchanges asynchronously.

        Args:
            vhost: Optional virtual host filter.

        Returns:
            List of exchange info dicts.
        """
        path = f"/api/exchanges/{_encode_vhost(vhost)}" if vhost else "/api/exchanges"
        return list(await self._aget(path))  # type: ignore[arg-type]

    async def list_bindings(self, vhost: Optional[str] = None) -> list[dict[str, Any]]:
        """List all bindings asynchronously.

        Args:
            vhost: Optional virtual host filter.

        Returns:
            List of binding info dicts.
        """
        path = f"/api/bindings/{_encode_vhost(vhost)}" if vhost else "/api/bindings"
        return list(await self._aget(path))  # type: ignore[arg-type]

    async def get_node_stats(self) -> list[dict[str, Any]]:
        """Return statistics for all cluster nodes asynchronously.

        Returns:
            List of node stat dicts.
        """
        return list(await self._aget("/api/nodes"))  # type: ignore[arg-type]

    async def get_overview(self) -> dict[str, Any]:
        """Return broker overview asynchronously.

        Returns:
            Overview dict.
        """
        return dict(await self._aget("/api/overview"))  # type: ignore[arg-type]


__all__: list[str] = [
    "AsyncManagementAPIClient",
    "ManagementAPIClient",
]
