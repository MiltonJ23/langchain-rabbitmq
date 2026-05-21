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
    EXCHANGE_TOOLS,
    BindExchangeTool,
    DeclareExchangeTool,
    DeleteExchangeTool,
)
from langchain_rabbitmq.tools.message import (
    MESSAGE_TOOLS,
    AckMessageTool,
    ConsumeMessageTool,
    NackMessageTool,
    PublishMessageTool,
    RejectMessageTool,
)
from langchain_rabbitmq.tools.queue import (
    QUEUE_TOOLS,
    BindQueueTool,
    DeclareQueueTool,
    DeleteQueueTool,
    GetQueueInfoTool,
    PurgeQueueTool,
    UnbindQueueTool,
)
from langchain_rabbitmq.tools.toolkit import ALL_TOOLS, RabbitMQToolkit

__all__: list[str] = [
    "ADMIN_TOOLS",
    "ALL_TOOLS",
    "EXCHANGE_TOOLS",
    "MESSAGE_TOOLS",
    "QUEUE_TOOLS",
    "AckMessageTool",
    "BindExchangeTool",
    "BindQueueTool",
    "CheckHealthTool",
    "CloseConnectionTool",
    "ConsumeMessageTool",
    # Exchange tools
    "DeclareExchangeTool",
    # Queue tools
    "DeclareQueueTool",
    "DeleteExchangeTool",
    "DeleteQueueTool",
    "GetConnectionInfoTool",
    "GetNodeStatsTool",
    "GetQueueInfoTool",
    "ListBindingsTool",
    "ListExchangesTool",
    # Admin tools
    "ListQueuesTool",
    "NackMessageTool",
    # Message tools
    "PublishMessageTool",
    "PurgeQueueTool",
    # Toolkit
    "RabbitMQToolkit",
    "RejectMessageTool",
    "UnbindQueueTool",
]
