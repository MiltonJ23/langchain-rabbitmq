# Cookbook: Basic agent

A complete worked example — declare a queue, publish a message, consume it —
using a ReAct agent driven by `langchain-rabbitmq` tools.

---

## Prerequisites

```bash
pip install langchain-rabbitmq langchain-openai

# Start a local broker
docker run -d --name rabbitmq \
  -p 5672:5672 -p 15672:15672 \
  rabbitmq:3.12-management
```

---

## Environment

```bash
export RABBITMQ_URL="amqp://guest:guest@localhost:5672/"
export RABBITMQ_MANAGEMENT_API_URL="http://localhost:15672"
export OPENAI_API_KEY="sk-..."
```

---

## Code

```python
import os
from langchain_openai import ChatOpenAI
from langchain.agents import create_react_agent, AgentExecutor
from langchain import hub
from langchain_rabbitmq import RabbitMQToolkit

# -- LLM ----------------------------------------------------------------
llm = ChatOpenAI(model="gpt-4o-mini", temperature=0)

# -- Tools --------------------------------------------------------------
toolkit = RabbitMQToolkit.from_settings()
tools   = toolkit.get_tools()

# -- Agent --------------------------------------------------------------
prompt   = hub.pull("hwchase17/react")
agent    = create_react_agent(llm=llm, tools=tools, prompt=prompt)
executor = AgentExecutor(agent=agent, tools=tools, verbose=True)

# -- Scenario -----------------------------------------------------------
result = executor.invoke({
    "input": (
        "1. Declare a durable queue called 'notifications'.\n"
        "2. Publish the message 'Hello, world!' to it using the default exchange "
        "   with routing key 'notifications'.\n"
        "3. Consume the message and return its body."
    )
})

print("Agent answer:", result["output"])
```

---

## What happens

1. The agent calls `rabbitmq_declare_queue` with `{"name": "notifications", "durable": true}`
2. It calls `rabbitmq_publish_message` with the default exchange and routing key `"notifications"`
3. It calls `rabbitmq_consume_message` and reads back the body
4. The agent returns the body as its final answer

---

## Expected output (verbose)

```
> Entering new AgentExecutor chain...
Thought: I need to declare a durable queue first.
Action: rabbitmq_declare_queue
Action Input: {"name": "notifications", "durable": true}
Observation: Queue 'notifications' declared successfully.
Thought: Now I'll publish a message.
Action: rabbitmq_publish_message
Action Input: {"exchange": "", "routing_key": "notifications", "body": "Hello, world!"}
Observation: Message published successfully to exchange '' with routing key 'notifications'.
Thought: Now I'll consume the message.
Action: rabbitmq_consume_message
Action Input: {"queue": "notifications"}
Observation: {"body": "Hello, world!", "delivery_tag": 1, "routing_key": "notifications", "redelivered": false}
Thought: I have the body.
Final Answer: Hello, world!
```

---

## Cleanup

```python
# delete the queue when you're done
tools_by_name = {t.name: t for t in tools}
tools_by_name["rabbitmq_delete_queue"].invoke({"name": "notifications"})
```
