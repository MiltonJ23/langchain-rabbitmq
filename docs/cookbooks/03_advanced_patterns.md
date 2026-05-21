# Cookbook: Advanced patterns

Two production patterns — RPC over RabbitMQ and topic exchange routing.

---

## Pattern 1 — RPC over RabbitMQ

The classic RabbitMQ RPC pattern: a *client* publishes to a request queue with a
`reply_to` header, and a *server* processes the request and publishes the response
to the reply queue.

### Agent prompt

```python
RPC_PROMPT = """
You are an RPC orchestrator. Follow these steps exactly:

1. Declare a durable queue called 'rpc_requests'.
2. Declare a transient queue called 'rpc_replies'.
3. Publish the message '{"method": "add", "args": [3, 4]}' to the default exchange
   with routing key 'rpc_requests', content_type 'application/json',
   and headers {{"reply_to": "rpc_replies", "correlation_id": "req-001"}}.
4. Consume one message from 'rpc_replies' and return its body.
"""
```

### Code

```python
import os
import json
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub
from langchain_rabbitmq import RabbitMQToolkit

os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"

llm      = ChatOpenAI(model="gpt-4o-mini", temperature=0)
toolkit  = RabbitMQToolkit.from_settings()
tools    = toolkit.get_tools()
prompt   = hub.pull("hwchase17/react")
agent    = create_react_agent(llm=llm, tools=tools, prompt=prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# In a real system, a worker process would read from 'rpc_requests'
# and write the result to the reply_to queue. Here we simulate it:
tools_by_name = {t.name: t for t in tools}

tools_by_name["rabbitmq_declare_queue"].invoke({"name": "rpc_requests", "durable": True})
tools_by_name["rabbitmq_declare_queue"].invoke({"name": "rpc_replies"})

# Simulate worker: publish a fake reply
tools_by_name["rabbitmq_publish_message"].invoke({
    "exchange": "",
    "routing_key": "rpc_replies",
    "body": json.dumps({"result": 7, "correlation_id": "req-001"}),
    "content_type": "application/json",
})

result = executor.invoke({"input": RPC_PROMPT})
print("RPC result:", result["output"])
```

---

## Pattern 2 — Topic exchange routing

Use a `topic` exchange to fan messages out to multiple queues based on routing
key wildcards.

```
orders.emea.urgent  →  queue: urgent_orders
orders.emea.*       →  queue: emea_orders
orders.#            →  queue: all_orders
```

### Code

```python
import os
from langchain_rabbitmq import RabbitMQToolkit
from langchain_rabbitmq.config import RabbitMQSettings

os.environ["RABBITMQ_URL"] = "amqp://guest:guest@localhost:5672/"

toolkit       = RabbitMQToolkit.from_settings()
tools_by_name = {t.name: t for t in toolkit.get_tools()}

# Declare exchange
tools_by_name["rabbitmq_declare_exchange"].invoke({
    "name": "order_events",
    "exchange_type": "topic",
    "durable": True,
})

# Declare queues
for q in ("urgent_orders", "emea_orders", "all_orders"):
    tools_by_name["rabbitmq_declare_queue"].invoke({"name": q, "durable": True})

# Bind with routing key patterns
tools_by_name["rabbitmq_bind_queue"].invoke({
    "queue": "urgent_orders",
    "exchange": "order_events",
    "routing_key": "orders.*.urgent",
})
tools_by_name["rabbitmq_bind_queue"].invoke({
    "queue": "emea_orders",
    "exchange": "order_events",
    "routing_key": "orders.emea.*",
})
tools_by_name["rabbitmq_bind_queue"].invoke({
    "queue": "all_orders",
    "exchange": "order_events",
    "routing_key": "orders.#",
})

# Publish a test event
tools_by_name["rabbitmq_publish_message"].invoke({
    "exchange": "order_events",
    "routing_key": "orders.emea.urgent",
    "body": '{"order_id": 99, "region": "emea", "priority": "urgent"}',
    "content_type": "application/json",
    "persistent": True,
})

# Verify delivery to all three queues
for q in ("urgent_orders", "emea_orders", "all_orders"):
    msg = tools_by_name["rabbitmq_consume_message"].invoke({"queue": q, "auto_ack": True})
    print(f"{q}: {msg}")
```

### Expected output

```
urgent_orders: {"body": "{\"order_id\": 99, ...}", "delivery_tag": 1, ...}
emea_orders:   {"body": "{\"order_id\": 99, ...}", "delivery_tag": 1, ...}
all_orders:    {"body": "{\"order_id\": 99, ...}", "delivery_tag": 1, ...}
```

All three queues receive the message because the routing key
`orders.emea.urgent` matches all three patterns.
