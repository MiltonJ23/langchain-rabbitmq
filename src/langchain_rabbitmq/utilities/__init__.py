"""Utilities sub-package.

Contains low-level AMQP client wrappers (sync and async) and the
Management HTTP API client that back the LangChain tools.

Example:
    Synchronous AMQP usage::

        from langchain_rabbitmq.utilities import RabbitMQClient
        with RabbitMQClient() as client:
            client.declare_queue("orders")

    Management API usage::

        from langchain_rabbitmq.utilities import ManagementAPIClient
        with ManagementAPIClient() as mgmt:
            queues = mgmt.list_queues()
"""

from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.management import (
    AsyncManagementAPIClient,
    ManagementAPIClient,
)
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

__all__: list[str] = [
    "RabbitMQClient",
    "AsyncRabbitMQClient",
    "ManagementAPIClient",
    "AsyncManagementAPIClient",
]
