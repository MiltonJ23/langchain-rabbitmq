# langchain-rabbitmq

**Production-grade LangChain tools for RabbitMQ.**

Give your AI agents the full power of RabbitMQ — queue management, exchange routing,
message operations, and broker monitoring — through **21 structured LangChain tools**.

---

## Why langchain-rabbitmq?

Most message-broker integrations are thin wrappers. `langchain-rabbitmq` is different:

- **Complete surface area** — every AMQP operation your agent might need is exposed
  as a discrete, discoverable tool with a precise Pydantic schema
- **Type-safe to the core** — `pyrefly` strict mode passes with zero errors;
  `Any` is banned except where unavoidable in third-party signatures
- **Agent-transparent errors** — exceptions are returned as human-readable strings
  so the LLM can reason about failures and retry with different arguments
- **Async-native** — every tool has both `_run` and `_arun` implementations

---

## At a glance

```python
import os
from langchain_rabbitmq import RabbitMQToolkit

os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"

toolkit = RabbitMQToolkit.from_settings()
tools   = toolkit.get_tools()          # 21 BaseTool instances

# Plug directly into any agent
from langchain.agents import create_react_agent
agent = create_react_agent(llm=your_llm, tools=tools, prompt=prompt)
result = agent.invoke({"input": "Declare a durable queue called 'orders'."})
```

---

## Navigation

| Section | What you'll find |
|---|---|
| [Getting started](getting_started.md) | Install, configure, first agent |
| [Configuration](configuration.md) | All env vars and programmatic settings |
| [Tools](tools/index.md) | Every tool with input/output schema |
| [API reference](api/index.md) | Auto-generated from docstrings |
| [Cookbooks](cookbooks/01_basic_agent.md) | End-to-end worked examples |
