"""Utilities sub-package.

Contains low-level AMQP client wrappers (sync and async) that back
the LangChain tools.

Example:
    Synchronous usage::

        from langchain_rabbitmq.utilities import RabbitMQClient
        with RabbitMQClient() as client:
            client.declare_queue("orders")

    Asynchronous usage::

        from langchain_rabbitmq.utilities import AsyncRabbitMQClient
        async with AsyncRabbitMQClient() as client:
            await client.declare_queue("orders")
"""

from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

__all__: list[str] = [
    "RabbitMQClient",
    "AsyncRabbitMQClient",
]
