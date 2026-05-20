"""Unit tests for RabbitMQToolkit."""

from __future__ import annotations

import pytest
from langchain_core.tools import BaseTool

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.tools.toolkit import ALL_TOOLS, RabbitMQToolkit

pytestmark = pytest.mark.unit

class TestRabbitMQToolkit:
    def test_get_tools_returns_21_tools(self, settings: RabbitMQSettings) -> None:
        toolkit = RabbitMQToolkit(settings=settings)
        tools = toolkit.get_tools()
        assert len(tools) == 21

    def test_all_tools_are_base_tool(self, settings: RabbitMQSettings) -> None:
        for tool in RabbitMQToolkit(settings=settings).get_tools():
            assert isinstance(tool, BaseTool)

    def test_all_tool_names_unique(self, settings: RabbitMQSettings) -> None:
        names = [t.name for t in RabbitMQToolkit(settings=settings).get_tools()]
        assert len(names) == len(set(names))

    def test_tools_share_settings(self) -> None:
        settings = RabbitMQSettings(host="broker.internal")
        tools = RabbitMQToolkit(settings=settings).get_tools()
        for tool in tools:
            assert tool.settings.host == "broker.internal"  # type: ignore[attr-defined]

    def test_from_settings_classmethod(self) -> None:
        settings = RabbitMQSettings(host="myhost")
        toolkit = RabbitMQToolkit.from_settings(settings)
        assert toolkit.settings.host == "myhost"

    def test_from_settings_defaults_to_env_settings(self) -> None:
        toolkit = RabbitMQToolkit.from_settings()
        assert toolkit.settings.host == "localhost"

    def test_default_construction_uses_env_settings(self) -> None:
        toolkit = RabbitMQToolkit()
        assert toolkit.settings.host == "localhost"


class TestAllToolsList:
    def test_all_tools_length(self) -> None:
        assert len(ALL_TOOLS) == 21

    def test_all_tools_names_prefix(self) -> None:
        for cls in ALL_TOOLS:
            inst = cls()
            assert inst.name.startswith("rabbitmq_")

    def test_covers_all_groups(self) -> None:
        names = {cls().name for cls in ALL_TOOLS}
        # Spot-check one from each group
        assert "rabbitmq_declare_queue" in names  # queue
        assert "rabbitmq_declare_exchange" in names  # exchange
        assert "rabbitmq_publish_message" in names  # message
        assert "rabbitmq_list_queues" in names  # admin
