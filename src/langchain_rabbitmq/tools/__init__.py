"""Tools sub-package.

Exposes all LangChain BaseTool implementations for interacting with
RabbitMQ queues, exchanges, messages, and the management API.

Example:
    Instantiate all tools with shared settings::

        from langchain_rabbitmq.tools import QUEUE_TOOLS, EXCHANGE_TOOLS, MESSAGE_TOOLS
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings(host="broker.example.com")
        tools = [
            cls(settings=settings)
            for cls in QUEUE_TOOLS + EXCHANGE_TOOLS + MESSAGE_TOOLS
        ]
"""

from langchain_rabbitmq.tools.exchange import (
    BindExchangeTool,
    DeclareExchangeTool,
    DeleteExchangeTool,
    EXCHANGE_TOOLS,
)
from langchain_rabbitmq.tools.message import (
    AckMessageTool,
    ConsumeMessageTool,
    MESSAGE_TOOLS,
    NackMessageTool,
    PublishMessageTool,
    RejectMessageTool,
)
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
    # Queue tools
    "DeclareQueueTool",
    "DeleteQueueTool",
    "PurgeQueueTool",
    "BindQueueTool",
    "UnbindQueueTool",
    "GetQueueInfoTool",
    "QUEUE_TOOLS",
    # Exchange tools
    "DeclareExchangeTool",
    "DeleteExchangeTool",
    "BindExchangeTool",
    "EXCHANGE_TOOLS",
    # Message tools
    "PublishMessageTool",
    "ConsumeMessageTool",
    "AckMessageTool",
    "NackMessageTool",
    "RejectMessageTool",
    "MESSAGE_TOOLS",
]
