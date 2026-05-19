"""LangChain toolkit bundling all RabbitMQ tools.

Provides :class:`RabbitMQToolkit`, a :class:`~langchain_core.tools.BaseToolkit`
that instantiates and returns all 21 LangChain tools for RabbitMQ in one call.

Example:
    Build the full tool set from environment variables::

        from langchain_rabbitmq.tools.toolkit import RabbitMQToolkit

        toolkit = RabbitMQToolkit.from_settings()
        tools = toolkit.get_tools()

    Build with explicit settings::

        from langchain_rabbitmq.config import RabbitMQSettings
        from langchain_rabbitmq.tools.toolkit import RabbitMQToolkit

        settings = RabbitMQSettings(host="broker.internal", port=5672)
        toolkit = RabbitMQToolkit(settings=settings)
        tools = toolkit.get_tools()
"""

from __future__ import annotations

from typing import List

from langchain_core.tools import BaseToolkit, BaseTool

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.tools._base import _RabbitMQBaseTool
from langchain_rabbitmq.tools.admin import ADMIN_TOOLS
from langchain_rabbitmq.tools.exchange import EXCHANGE_TOOLS
from langchain_rabbitmq.tools.message import MESSAGE_TOOLS
from langchain_rabbitmq.tools.queue import QUEUE_TOOLS

# Complete ordered list of every tool class
ALL_TOOLS: list[type[_RabbitMQBaseTool]] = (
    QUEUE_TOOLS + EXCHANGE_TOOLS + MESSAGE_TOOLS + ADMIN_TOOLS
)


class RabbitMQToolkit(BaseToolkit):
    """Toolkit that bundles all 21 RabbitMQ LangChain tools.

    All tools share the same :class:`~langchain_rabbitmq.config.RabbitMQSettings`
    instance, ensuring consistent broker connection parameters across the agent.

    Attributes:
        settings: Broker connection configuration.  Defaults to environment
            variable discovery via ``RabbitMQSettings()``.

    Example:
        Using the toolkit with ``initialize_agent``::

            from langchain_rabbitmq.tools.toolkit import RabbitMQToolkit
            from langchain.agents import initialize_agent, AgentType

            toolkit = RabbitMQToolkit.from_settings()
            agent = initialize_agent(
                toolkit.get_tools(),
                llm,
                agent=AgentType.STRUCTURED_CHAT_ZERO_SHOT_REACT_DESCRIPTION,
            )
    """

    settings: RabbitMQSettings = RabbitMQSettings()  # type: ignore[call-arg]

    model_config = {"arbitrary_types_allowed": True}

    @classmethod
    def from_settings(cls, settings: RabbitMQSettings | None = None) -> "RabbitMQToolkit":
        """Create a toolkit, optionally overriding the settings.

        Args:
            settings: Broker settings.  When ``None`` the settings are loaded
                from environment variables.

        Returns:
            A configured :class:`RabbitMQToolkit` instance.

        Example:
            Create from explicit settings::

                from langchain_rabbitmq.config import RabbitMQSettings
                toolkit = RabbitMQToolkit.from_settings(
                    RabbitMQSettings(host="rabbit.prod")
                )
        """
        return cls(settings=settings or RabbitMQSettings())  # type: ignore[call-arg]

    def get_tools(self) -> List[BaseTool]:
        """Return one instantiated instance of every RabbitMQ tool.

        All tools are configured with the shared :attr:`settings` instance so
        they connect to the same broker.

        Returns:
            A list of 21 :class:`~langchain_core.tools.BaseTool` instances
            covering queue, exchange, message, and admin operations.

        Example:
            Register the full tool set with an agent executor::

                tools = RabbitMQToolkit().get_tools()
                # 21 tools: queue (6) + exchange (3) + message (5) + admin (7)
        """
        return [cls(settings=self.settings) for cls in ALL_TOOLS]


__all__: list[str] = [
    "RabbitMQToolkit",
    "ALL_TOOLS",
]
