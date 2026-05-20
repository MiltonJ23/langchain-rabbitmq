# Cookbook: Async agent

Use `langchain-rabbitmq` in a fully async workflow with `aio-pika` under the hood.

---

## Prerequisites

```bash
pip install langchain-rabbitmq langchain-openai

docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3.12-management
```

---

## Environment

```bash
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
export OPENAI_API_KEY="sk-..."
```

---

## Code

```python
import asyncio
import os

from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub
from langchain_rabbitmq import RabbitMQToolkit


async def main() -> None:
    llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

    toolkit = RabbitMQToolkit.from_settings()
    tools   = toolkit.get_tools()

    prompt   = hub.pull("hwchase17/react")
    agent    = create_react_agent(llm=llm, tools=tools, prompt=prompt)
    executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

    # ainvoke drives _arun on every tool — no blocking calls
    result = await executor.ainvoke({
        "input": (
            "Declare a topic exchange called 'events', "
            "then publish 'order.created' event with body '{\"order_id\": 42}' "
            "to routing key 'order.created'."
        )
    })
    print("Agent answer:", result["output"])


asyncio.run(main())
```

---

## How async works

Every tool implements `_arun` using `AsyncRabbitMQClient` (backed by `aio-pika`).
When `AgentExecutor.ainvoke` is called, LangChain awaits `tool.arun(...)` which
in turn awaits `_arun`, keeping the event loop free throughout.

```python
# You can also call a single tool directly in async context:
from langchain_rabbitmq.tools.exchange import DeclareExchangeTool
from langchain_rabbitmq.config import RabbitMQSettings

tool   = DeclareExchangeTool(settings=RabbitMQSettings())
result = await tool.ainvoke({"name": "events", "exchange_type": "topic"})
print(result)  # "Exchange 'events' declared successfully."
```

---

## Performance tip

For high-throughput scenarios, reuse the toolkit across multiple `ainvoke` calls.
The `AsyncRabbitMQClient` maintains a single persistent connection per settings
instance — avoid constructing a new toolkit per request.

```python
toolkit = RabbitMQToolkit.from_settings()  # create once at startup
tools   = toolkit.get_tools()

# call many times
results = await asyncio.gather(*[
    tools_by_name["rabbitmq_publish_message"].ainvoke({
        "exchange": "events",
        "routing_key": f"order.created",
        "body": f'{{"order_id": {i}}}',
    })
    for i in range(100)
])
```
