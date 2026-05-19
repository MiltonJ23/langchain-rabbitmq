"""LangChain tools for interacting with RabbitMQ.

This package provides a suite of LangChain-compatible tools that allow
AI agents to interact with RabbitMQ via AMQP and the Management HTTP API.

Example:
    Basic usage with an agent::

        from langchain_rabbitmq.tools import RabbitMQToolkit
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings()
        toolkit = RabbitMQToolkit.from_settings(settings)
        tools = toolkit.get_tools()
"""

from importlib.metadata import PackageNotFoundError, version

try:
    __version__: str = version("langchain-rabbitmq")
except PackageNotFoundError:
    __version__ = "0.0.0"

__all__: list[str] = ["__version__"]
