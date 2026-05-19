"""Tools sub-package.

Exposes all LangChain BaseTool implementations for interacting with
RabbitMQ queues, exchanges, messages, and the management API, as well
as the :class:`~langchain_rabbitmq.tools.toolkit.RabbitMQToolkit` which
bundles all 21 tools in one convenient accessor.

Example:
    Instantiate the full toolkit::

        from langchain_rabbitmq.tools import RabbitMQToolkit

        tools = RabbitMQToolkit.from_settings().get_tools()

    Or pick individual tool groups::

        from langchain_rabbitmq.tools import QUEUE_TOOLS, EXCHANGE_TOOLS
        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings(host="broker.example.com")
        tools = [cls(settings=settings) for cls in QUEUE_TOOLS + EXCHANGE_TOOLS]
"""

from langchain_rabbitmq.tools.admin import (
    ADMIN_TOOLS,
    CheckHealthTool,
    CloseConnectionTool,
    GetConnectionInfoTool,
    GetNodeStatsTool,
    ListBindingsTool,
    ListExchangesTool,
    ListQueuesTool,
)
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
from langchain_rabbitmq.tools.toolkit import ALL_TOOLS, RabbitMQToolkit

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
    # Admin tools
    "ListQueuesTool",
    "ListExchangesTool",
    "ListBindingsTool",
    "GetNodeStatsTool",
    "CheckHealthTool",
    "GetConnectionInfoTool",
    "CloseConnectionTool",
    "ADMIN_TOOLS",
    # Toolkit
    "RabbitMQToolkit",
    "ALL_TOOLS",
]
