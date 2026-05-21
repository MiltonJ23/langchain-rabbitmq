"""Unit tests for ManagementAPIClient and AsyncManagementAPIClient.

Uses ``respx`` to intercept httpx requests without a live broker.
"""

from __future__ import annotations

import httpx
import pytest
import respx

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import RabbitMQAdminError
from langchain_rabbitmq.utilities.management import (
    AsyncManagementAPIClient,
    ManagementAPIClient,
    _encode_vhost,
)

pytestmark = pytest.mark.unit


_BASE = "http://localhost:15672"


@pytest.fixture()
def mgmt_settings() -> RabbitMQSettings:
    """Settings with a management API URL configured."""
    return RabbitMQSettings(
        host="localhost",
        management_api_url=_BASE,
    )


# ---------------------------------------------------------------------------
# _encode_vhost helper
# ---------------------------------------------------------------------------


class TestEncodeVhost:
    def test_slash_encoded(self) -> None:
        assert _encode_vhost("/") == "%2F"

    def test_plain_vhost(self) -> None:
        assert _encode_vhost("production") == "production"

    def test_slash_in_middle(self) -> None:
        assert _encode_vhost("a/b") == "a%2Fb"


# ---------------------------------------------------------------------------
# ManagementAPIClient (sync)
# ---------------------------------------------------------------------------


class TestManagementAPIClientLifecycle:
    def test_context_manager_opens_and_closes(self, mgmt_settings: RabbitMQSettings) -> None:
        with respx.mock:
            respx.get(f"{_BASE}/api/queues").mock(return_value=httpx.Response(200, json=[]))
            with ManagementAPIClient(mgmt_settings) as client:
                assert client._client is not None
                assert not client._client.is_closed
            assert client._client is None

    def test_close_idempotent(self, mgmt_settings: RabbitMQSettings) -> None:
        client = ManagementAPIClient(mgmt_settings)
        client.close()  # no client opened
        client.close()  # second call is safe


class TestManagementAPIClientListQueues:
    @respx.mock
    def test_list_all_queues(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {"name": "orders", "messages": 5, "durable": True},
                    {"name": "dlq", "messages": 0, "durable": False},
                ],
            )
        )
        with ManagementAPIClient(mgmt_settings) as client:
            queues = client.list_queues()
        assert len(queues) == 2
        assert queues[0]["name"] == "orders"

    @respx.mock
    def test_list_queues_with_vhost(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues/%2F").mock(
            return_value=httpx.Response(200, json=[{"name": "q"}])
        )
        with ManagementAPIClient(mgmt_settings) as client:
            queues = client.list_queues(vhost="/")
        assert queues[0]["name"] == "q"

    @respx.mock
    def test_http_error_raises_admin_error(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues").mock(return_value=httpx.Response(401, text="Unauthorized"))
        with (
            ManagementAPIClient(mgmt_settings) as client,
            pytest.raises(RabbitMQAdminError, match="401"),
        ):
            client.list_queues()

    @respx.mock
    def test_request_error_raises_admin_error(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues").mock(side_effect=httpx.ConnectError("refused"))
        with (
            ManagementAPIClient(mgmt_settings) as client,
            pytest.raises(RabbitMQAdminError, match="request failed"),
        ):
            client.list_queues()


class TestManagementAPIClientListExchanges:
    @respx.mock
    def test_list_exchanges(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/exchanges").mock(
            return_value=httpx.Response(
                200,
                json=[{"name": "events", "type": "topic"}],
            )
        )
        with ManagementAPIClient(mgmt_settings) as client:
            exchanges = client.list_exchanges()
        assert exchanges[0]["name"] == "events"


class TestManagementAPIClientListBindings:
    @respx.mock
    def test_list_bindings(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/bindings").mock(
            return_value=httpx.Response(
                200,
                json=[
                    {
                        "source": "events",
                        "destination": "orders",
                        "routing_key": "order.*",
                    }
                ],
            )
        )
        with ManagementAPIClient(mgmt_settings) as client:
            bindings = client.list_bindings()
        assert len(bindings) == 1


class TestManagementAPIClientGetNodeStats:
    @respx.mock
    def test_get_node_stats(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/nodes").mock(
            return_value=httpx.Response(
                200,
                json=[{"name": "rabbit@node1", "type": "disc", "running": True}],
            )
        )
        with ManagementAPIClient(mgmt_settings) as client:
            nodes = client.get_node_stats()
        assert nodes[0]["name"] == "rabbit@node1"


class TestManagementAPIClientGetOverview:
    @respx.mock
    def test_get_overview(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/overview").mock(
            return_value=httpx.Response(
                200,
                json={"rabbitmq_version": "3.12.4", "object_totals": {"queues": 2}},
            )
        )
        with ManagementAPIClient(mgmt_settings) as client:
            overview = client.get_overview()
        assert overview["rabbitmq_version"] == "3.12.4"


# ---------------------------------------------------------------------------
# AsyncManagementAPIClient
# ---------------------------------------------------------------------------


class TestAsyncManagementAPIClientLifecycle:
    @pytest.mark.asyncio
    @respx.mock
    async def test_async_context_manager(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues").mock(return_value=httpx.Response(200, json=[]))
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            assert client._client is not None
        assert client._client is None

    @pytest.mark.asyncio
    async def test_close_idempotent(self, mgmt_settings: RabbitMQSettings) -> None:
        client = AsyncManagementAPIClient(mgmt_settings)
        await client.close()  # no client open
        await client.close()  # second call safe


class TestAsyncManagementAPIClientMethods:
    @pytest.mark.asyncio
    @respx.mock
    async def test_list_queues_async(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues").mock(
            return_value=httpx.Response(200, json=[{"name": "q1"}])
        )
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            queues = await client.list_queues()
        assert queues[0]["name"] == "q1"

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_queues_with_vhost_async(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/queues/%2F").mock(
            return_value=httpx.Response(200, json=[{"name": "vq"}])
        )
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            queues = await client.list_queues(vhost="/")
        assert queues[0]["name"] == "vq"

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_exchanges_async(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/exchanges").mock(
            return_value=httpx.Response(200, json=[{"name": "ex"}])
        )
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            exchanges = await client.list_exchanges()
        assert exchanges[0]["name"] == "ex"

    @pytest.mark.asyncio
    @respx.mock
    async def test_list_bindings_async(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/bindings").mock(
            return_value=httpx.Response(200, json=[{"source": "s"}])
        )
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            bindings = await client.list_bindings()
        assert bindings[0]["source"] == "s"

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_node_stats_async(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/nodes").mock(
            return_value=httpx.Response(200, json=[{"name": "rabbit@n"}])
        )
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            nodes = await client.get_node_stats()
        assert nodes[0]["name"] == "rabbit@n"

    @pytest.mark.asyncio
    @respx.mock
    async def test_get_overview_async(self, mgmt_settings: RabbitMQSettings) -> None:
        respx.get(f"{_BASE}/api/overview").mock(
            return_value=httpx.Response(200, json={"version": "3.12"})
        )
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            ov = await client.get_overview()
        assert ov["version"] == "3.12"

    @pytest.mark.asyncio
    @respx.mock
    async def test_http_error_raises_admin_error_async(
        self, mgmt_settings: RabbitMQSettings
    ) -> None:
        respx.get(f"{_BASE}/api/queues").mock(return_value=httpx.Response(403, text="Forbidden"))
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            with pytest.raises(RabbitMQAdminError, match="403"):
                await client.list_queues()

    @pytest.mark.asyncio
    @respx.mock
    async def test_request_error_raises_admin_error_async(
        self, mgmt_settings: RabbitMQSettings
    ) -> None:
        respx.get(f"{_BASE}/api/queues").mock(side_effect=httpx.ConnectError("timeout"))
        async with AsyncManagementAPIClient(mgmt_settings) as client:
            with pytest.raises(RabbitMQAdminError, match="request failed"):
                await client.list_queues()
