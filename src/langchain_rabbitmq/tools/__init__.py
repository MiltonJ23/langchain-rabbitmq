"""Tools sub-package.

Exposes all LangChain BaseTool implementations for interacting with
RabbitMQ queues, exchanges, messages, and the management API.

Example:
    Instantiate all queue tools with shared settings::

        from langchain_rabbitmq.tools import QUEUE_TOOLS
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings(host="broker.example.com")
        tools = [cls(settings=settings) for cls in QUEUE_TOOLS]
"""

from langchain_rabbitmq.tools.queue import (
    BindQueueTool,
    DeclareQueueTool,
    DeleteQueueTool,
    GetQueueInfoTool,
    PurgeQueueTool,
    QUEUE_TOOLS,
    UnbindQueueTool,
)

__all__: list[str] = [
    "DeclareQueueTool",
    "DeleteQueueTool",
    "PurgeQueueTool",
    "BindQueueTool",
    "UnbindQueueTool",
    "GetQueueInfoTool",
    "QUEUE_TOOLS",
]
