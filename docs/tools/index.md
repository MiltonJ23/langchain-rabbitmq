# Tools overview

langchain-rabbitmq exposes **21 LangChain tools** organised into four categories.
Every tool:

- Inherits from `BaseTool` (via `_RabbitMQBaseTool`)
- Declares a strict `args_schema` (Pydantic v2 `BaseModel`)
- Implements both `_run` (sync) and `_arun` (async)
- Returns a human-readable string — success message or categorised error

---

## Quick reference

| Category | Count | Tools |
|---|---|---|
| [Queue management](queue.md) | 6 | declare, delete, purge, bind, unbind, info |
| [Exchange management](exchange.md) | 3 | declare, delete, bind |
| [Message operations](message.md) | 5 | publish, consume, ack, nack, reject |
| [Admin & monitoring](admin.md) | 7 | list queues/exchanges/bindings, node stats, health, connections |

---

## Discovery by an LLM

Each tool's `name` and `description` are crafted so a ReAct-style agent can decide
when and how to invoke it without additional context.  Example:

```text
Tool: rabbitmq_publish_message
Description: Publish a message to a RabbitMQ exchange.
  Specify the exchange name, routing key, and message body.
  Optionally set persistence, TTL (milliseconds), and custom headers.
  Returns a confirmation string on success or an error description on failure.
```

---

## Error types returned to the agent

| Exception class | Returned when |
|---|---|
| `RabbitMQConnectionError` | Broker is unreachable or authentication failed |
| `RabbitMQChannelError` | Channel-level AMQP error (wrong args, missing resource) |
| `RabbitMQMessageError` | Publish or consume operation failed |
| `RabbitMQAdminError` | Management API request failed |
