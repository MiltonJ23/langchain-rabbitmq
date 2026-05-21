"""Session-scoped fixtures for integration tests.

All integration tests require a running Docker daemon.  The RabbitMQ container
is started once per test session via ``testcontainers``.  A separate fixture
provides the management-plugin-enabled container for HTTP API tests.

Skip entire session when Docker is unavailable::

    pytest tests/integration/ -m integration
"""

from __future__ import annotations

import time
from typing import TYPE_CHECKING

import pytest
from testcontainers.rabbitmq import RabbitMqContainer

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

pytestmark = pytest.mark.integration

if TYPE_CHECKING:
    from collections.abc import Generator

# ---------------------------------------------------------------------------
# RabbitMQ container (AMQP only — fast start, plain image)
# ---------------------------------------------------------------------------

_RABBITMQ_IMAGE = "rabbitmq:3.12-management"


@pytest.fixture(scope="session")
def rabbitmq_container() -> Generator[RabbitMqContainer, None, None]:
    """Start a RabbitMQ broker once for the full test session.

    Uses the ``rabbitmq:3.12-management`` image so that the Management HTTP
    API is also available on port 15672.  Port 15672 must be explicitly exposed
    before the container starts; ``RabbitMqContainer`` only maps AMQP (5672) by
    default.
    """
    container = RabbitMqContainer(
        image=_RABBITMQ_IMAGE,
        username="guest",
        password="guest",
    )
    # Expose the management port so get_exposed_port(15672) works.
    container.with_exposed_ports(15672)
    with container:
        # Give the management plugin an extra moment to start.
        time.sleep(5)
        yield container


@pytest.fixture(scope="session")
def rabbitmq_settings(rabbitmq_container: RabbitMqContainer) -> RabbitMQSettings:
    """RabbitMQSettings wired to the test container."""
    params = rabbitmq_container.get_connection_params()
    host = str(params.host)
    port = int(params.port)
    mgmt_port = rabbitmq_container.get_exposed_port(15672)
    return RabbitMQSettings(
        host=host,
        port=port,
        username="guest",
        password="guest",
        management_api_url=f"http://{host}:{mgmt_port}",
        max_retries=3,
        retry_delay=0.5,
    )


# ---------------------------------------------------------------------------
# Client fixtures (function-scoped — fresh connection per test)
# ---------------------------------------------------------------------------


@pytest.fixture()
def sync_client(rabbitmq_settings: RabbitMQSettings) -> Generator[RabbitMQClient, None, None]:
    """Yield a connected :class:`.RabbitMQClient`, closed after the test."""
    client = RabbitMQClient(rabbitmq_settings)
    client.connect()
    try:
        yield client
    finally:
        client.close()


@pytest.fixture()
async def async_client(
    rabbitmq_settings: RabbitMQSettings,
) -> AsyncRabbitMQClient:
    """Yield a connected :class:`.AsyncRabbitMQClient`, closed after the test."""
    client = AsyncRabbitMQClient(rabbitmq_settings)
    await client.connect()
    try:
        yield client  # type: ignore[misc]
    finally:
        await client.close()


# ---------------------------------------------------------------------------
# Helpers shared across test modules
# ---------------------------------------------------------------------------


def _unique_name(prefix: str) -> str:
    """Return a name unique within the test run to avoid cross-test pollution."""
    import uuid

    return f"{prefix}.{uuid.uuid4().hex[:8]}"
