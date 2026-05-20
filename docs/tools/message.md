# Message operation tools

Five tools cover the full AMQP message lifecycle.

---

## PublishMessageTool

**Tool name:** `rabbitmq_publish_message`

Publish a message to an exchange with full control over routing and delivery semantics.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `exchange` | `str` | ✅ | — | Target exchange (use `""` for the default exchange) |
| `routing_key` | `str` | ✅ | — | Routing key for message delivery |
| `body` | `str` | ✅ | — | Message body (UTF-8 string or JSON) |
| `persistent` | `bool` | | `True` | Mark message as persistent (delivery mode 2) |
| `ttl` | `int \| None` | | `None` | Per-message TTL in milliseconds |
| `headers` | `dict` | | `{}` | Custom AMQP headers |
| `content_type` | `str` | | `"text/plain"` | MIME content type |

::: langchain_rabbitmq.tools.message.PublishMessageTool

---

## ConsumeMessageTool

**Tool name:** `rabbitmq_consume_message`

Pull a single message from a queue using `basic.get`. Returns the message body,
delivery tag, and routing key, or indicates the queue is empty.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `queue` | `str` | ✅ | — | Queue to consume from |
| `auto_ack` | `bool` | | `False` | Automatically acknowledge the message |

### Returns

A JSON string with `body`, `delivery_tag`, `routing_key`, and `redelivered` fields,
or `"Queue '<name>' is empty."`.

::: langchain_rabbitmq.tools.message.ConsumeMessageTool

---

## AckMessageTool

**Tool name:** `rabbitmq_ack_message`

Acknowledge a message delivery, signalling the broker that processing is complete.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `delivery_tag` | `int` | ✅ | — | Delivery tag returned by a previous consume |
| `multiple` | `bool` | | `False` | Acknowledge all deliveries up to and including this tag |

::: langchain_rabbitmq.tools.message.AckMessageTool

---

## NackMessageTool

**Tool name:** `rabbitmq_nack_message`

Negative-acknowledge a message. Optionally requeue it for re-delivery.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `delivery_tag` | `int` | ✅ | — | Delivery tag to nack |
| `multiple` | `bool` | | `False` | Nack all deliveries up to this tag |
| `requeue` | `bool` | | `True` | Requeue the message after nack |

::: langchain_rabbitmq.tools.message.NackMessageTool

---

## RejectMessageTool

**Tool name:** `rabbitmq_reject_message`

Reject a single message delivery.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `delivery_tag` | `int` | ✅ | — | Delivery tag to reject |
| `requeue` | `bool` | | `False` | Requeue after rejection |

::: langchain_rabbitmq.tools.message.RejectMessageTool
