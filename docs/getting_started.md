# Getting started

## Requirements

- Python 3.10 or later
- Docker (for integration / E2E tests; not required for production use)
- A running RabbitMQ broker (3.9+ recommended; 3.12+ for full Management API support)

---

## Installation

```bash
pip install langchain-rabbitmq
```

For development, testing, and documentation:

```bash
pip install "langchain-rabbitmq[dev,docs]"
```

---

## Start a local broker

The fastest way to get a broker is with Docker:

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 \
  -p 15672:15672 \
  rabbitmq:3.12-management
```

The Management UI is then available at <http://localhost:15672> (default credentials:
`guest` / `guest`).

---

## Environment variables

Set at minimum:

```bash
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
```

For Management API features (listing queues/exchanges, node stats):

```bash
export RABBITMQ_MANAGEMENT_API_URL="http://localhost:15672"
```

See the full [Configuration reference](configuration.md) for all options.

---

## Your first agent

```python
import os
from langchain_rabbitmq import RabbitMQToolkit
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub

os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"
os.environ["RABBITMQ_MANAGEMENT_API_URL"] = "http://localhost:15672"

llm     = ChatOpenAI(model="gpt-4o-mini", temperature=0)
toolkit = RabbitMQToolkit.from_settings()
tools   = toolkit.get_tools()

prompt  = hub.pull("hwchase17/react")
agent   = create_react_agent(llm=llm, tools=tools, prompt=prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

result = executor.invoke({
    "input": (
        "Declare a durable queue called 'notifications', "
        "publish the message 'Hello World' to it, "
        "then consume and return the message."
    )
})
print(result["output"])
```

---

## Using individual tools

If you only need one operation you can import and use a single tool directly:

```python
from langchain_rabbitmq.tools.queue import DeclareQueueTool
from langchain_rabbitmq.config import RabbitMQSettings

tool   = DeclareQueueTool(settings=RabbitMQSettings())
result = tool.invoke({"name": "orders", "durable": True})
print(result)  # "Queue 'orders' declared successfully."
```

---

## Selecting a subset of tools

Pass only the tools your agent needs to reduce the LLM's context window:

```python
from langchain_rabbitmq import RabbitMQToolkit
from langchain_rabbitmq.tools.message import PublishMessageTool, ConsumeMessageTool

toolkit = RabbitMQToolkit.from_settings()
all_tools = toolkit.get_tools()

# filter by class
tools = [t for t in all_tools if isinstance(t, (PublishMessageTool, ConsumeMessageTool))]
```

---

## Next steps

- [Configuration reference](configuration.md) — all settings and env vars
- [Tools reference](tools/index.md) — every tool with its input schema
- [Cookbook: Basic agent](cookbooks/01_basic_agent.md) — complete worked example
