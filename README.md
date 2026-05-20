# langchain-rabbitmq

[![CI](https://github.com/MiltonJ23/langchain-rabbitmq/actions/workflows/ci.yml/badge.svg)](https://github.com/MiltonJ23/langchain-rabbitmq/actions/workflows/ci.yml)
[![Coverage](https://img.shields.io/badge/coverage-93%25-brightgreen)](https://github.com/MiltonJ23/langchain-rabbitmq/actions)
[![PyPI version](https://img.shields.io/pypi/v/langchain-rabbitmq)](https://pypi.org/project/langchain-rabbitmq/)
[![Python](https://img.shields.io/pypi/pyversions/langchain-rabbitmq)](https://pypi.org/project/langchain-rabbitmq/)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](https://opensource.org/licenses/MIT)

**Production-grade LangChain tools for RabbitMQ.**  
Give your AI agents the full power of RabbitMQ — queue management, exchange routing,
message operations, and broker monitoring — through 21 structured LangChain tools.

---

## Table of contents

- [Features](#features)
- [Installation](#installation)
- [Quick start](#quick-start)
- [Tools reference](#tools-reference)
- [Configuration](#configuration)
- [Advanced usage](#advanced-usage)
- [Compatibility](#compatibility)
- [Contributing](#contributing)
- [License](#license)

---

## Features

- **21 LangChain tools** covering every RabbitMQ operation an agent needs
- **Pydantic v2** schemas — every argument is validated before touching the broker
- **Sync _and_ async** — `_run` / `_arun` on every tool; backed by `pika` and `aio-pika`
- **LCEL / Runnable compatible** — drop tools straight into any LangChain chain
- **Structured error types** — `RabbitMQConnectionError`, `RabbitMQChannelError`,
  `RabbitMQMessageError`, `RabbitMQAdminError` surface as agent-readable strings, never raw tracebacks
- **Retry logic** via `tenacity` with configurable back-off
- **SSL/TLS** support for encrypted broker connections
- **Management API** integration for monitoring and admin tasks
- **≥ 90 % test coverage** — unit, integration (Testcontainers), and E2E agent tests

---

## Installation

```bash
pip install langchain-rabbitmq
```

For development (tests, linting, docs):

```bash
pip install "langchain-rabbitmq[dev,docs]"
# or
make install
```

---

## Quick start

```python
import os
from langchain_rabbitmq import RabbitMQToolkit

# Credentials read from environment variables automatically
os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"

toolkit = RabbitMQToolkit.from_settings()
tools   = toolkit.get_tools()          # returns all 21 BaseTool instances

# Use with any LangChain agent
from langchain.agents import create_react_agent
agent = create_react_agent(llm=your_llm, tools=tools, prompt=your_prompt)
```

### Minimal environment

```bash
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3.12-management
```

---

## Tools reference

| Category | Tool name | Description |
|---|---|---|
| **Queue** | `rabbitmq_declare_queue` | Declare a durable or transient queue |
| | `rabbitmq_delete_queue` | Delete a queue (optional: if-empty / if-unused guards) |
| | `rabbitmq_purge_queue` | Remove all messages from a queue |
| | `rabbitmq_bind_queue` | Bind a queue to an exchange with a routing key |
| | `rabbitmq_unbind_queue` | Remove a queue–exchange binding |
| | `rabbitmq_get_queue_info` | Fetch message count, consumer count, and state |
| **Exchange** | `rabbitmq_declare_exchange` | Declare a direct / fanout / topic / headers exchange |
| | `rabbitmq_delete_exchange` | Delete an exchange |
| | `rabbitmq_bind_exchange` | Create an exchange-to-exchange binding |
| **Message** | `rabbitmq_publish_message` | Publish with routing key, headers, TTL, persistence |
| | `rabbitmq_consume_message` | Pull a single message (basic.get) |
| | `rabbitmq_ack_message` | Acknowledge a delivery by tag |
| | `rabbitmq_nack_message` | Negative-acknowledge with optional requeue |
| | `rabbitmq_reject_message` | Reject a single delivery |
| **Admin** | `rabbitmq_list_queues` | List all queues with stats (Management API) |
| | `rabbitmq_list_exchanges` | List all exchanges |
| | `rabbitmq_list_bindings` | List all bindings |
| | `rabbitmq_get_node_stats` | Fetch node CPU, memory, and socket metrics |
| | `rabbitmq_check_health` | Ping the broker and return health status |
| | `rabbitmq_get_connection_info` | Report open connections and channel count |
| | `rabbitmq_close_connection` | Gracefully close the AMQP connection |

---

## Configuration

All settings are loaded from **environment variables** via `RabbitMQSettings`
(backed by `pydantic-settings`).

| Variable | Default | Description |
|---|---|---|
| `RABBITMQ_URL` | `amqp://guest:guest@localhost:5672/` | Full AMQP connection URL |
| `RABBITMQ_HOST` | `localhost` | Broker hostname (used when `RABBITMQ_URL` is not set) |
| `RABBITMQ_PORT` | `5672` | AMQP port |
| `RABBITMQ_VHOST` | `/` | Virtual host |
| `RABBITMQ_USER` | `guest` | AMQP username |
| `RABBITMQ_PASSWORD` | `guest` | AMQP password |
| `RABBITMQ_SSL` | `false` | Enable TLS |
| `RABBITMQ_SSL_CA_CERTS` | — | Path to CA bundle (PEM) |
| `RABBITMQ_CONNECTION_TIMEOUT` | `10` | Connection timeout (seconds) |
| `RABBITMQ_MANAGEMENT_API_URL` | — | Base URL of the Management HTTP API |
| `RABBITMQ_MANAGEMENT_USER` | _(same as AMQP user)_ | Management API username |
| `RABBITMQ_MANAGEMENT_PASSWORD` | _(same as AMQP password)_ | Management API password |

### Programmatic configuration

```python
from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq import RabbitMQToolkit

settings = RabbitMQSettings(
    host="broker.internal",
    port=5672,
    virtual_host="production",
    username="app",
    password="s3cr3t",
    ssl=True,
    management_api_url="http://broker.internal:15672",
)
toolkit = RabbitMQToolkit(settings=settings)
```

---

## Advanced usage

### Async agent

```python
from langchain_rabbitmq import RabbitMQToolkit

toolkit = RabbitMQToolkit.from_settings()
tools   = toolkit.get_tools()

# All tools implement _arun — use with async LangChain runtimes
result = await tools[0].ainvoke({"name": "orders", "durable": True})
```

### Individual tools

```python
from langchain_rabbitmq.tools.queue import DeclareQueueTool
from langchain_rabbitmq.config import RabbitMQSettings

tool   = DeclareQueueTool(settings=RabbitMQSettings())
result = tool.invoke({"name": "orders", "durable": True})
print(result)  # "Queue 'orders' declared successfully."
```

### Error handling

All errors derive from `RabbitMQToolException` and are returned to the agent as
human-readable strings. The agent can react and retry without seeing raw stack traces.

```python
from langchain_rabbitmq.exceptions import (
    RabbitMQConnectionError,
    RabbitMQChannelError,
    RabbitMQMessageError,
    RabbitMQAdminError,
)
```

---

## Compatibility

| langchain-rabbitmq | langchain-core | pika   | aio-pika | RabbitMQ | Python      |
|--------------------|----------------|--------|----------|----------|-------------|
| 0.1.x              | ≥ 0.2.0        | ≥ 1.3  | ≥ 9.0    | 3.9 – 3.13 | 3.9 – 3.12 |

---

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for commit conventions, branch strategy, and
how to run the test suite.

```bash
make test          # all tests (requires Docker for integration tests)
make test-unit     # unit tests only — no broker needed
make lint          # ruff check
make typecheck     # pyrefly strict
make security      # bandit scan
```

---

## License

[MIT](LICENSE) © 2024 MiltonJ23
