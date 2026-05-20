# Queue management tools

Six tools cover the full lifecycle of a RabbitMQ queue.

---

## DeclareQueueTool

**Tool name:** `rabbitmq_declare_queue`

Declare a new queue or assert that an existing queue has the expected properties.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `str` | ✅ | — | Queue name (max 255 chars) |
| `durable` | `bool` | | `True` | Survive broker restart |
| `exclusive` | `bool` | | `False` | Delete on connection close |
| `auto_delete` | `bool` | | `False` | Delete when last consumer cancels |
| `arguments` | `dict` | | `{}` | Optional queue arguments (x-message-ttl, etc.) |

### Returns

`"Queue '<name>' declared successfully."` or an error description.

::: langchain_rabbitmq.tools.queue.DeclareQueueTool

---

## DeleteQueueTool

**Tool name:** `rabbitmq_delete_queue`

Delete a queue. Guards prevent deletion of non-empty or in-use queues when requested.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `str` | ✅ | — | Queue to delete |
| `if_empty` | `bool` | | `False` | Only delete if queue has no messages |
| `if_unused` | `bool` | | `False` | Only delete if queue has no consumers |

::: langchain_rabbitmq.tools.queue.DeleteQueueTool

---

## PurgeQueueTool

**Tool name:** `rabbitmq_purge_queue`

Remove all messages from a queue without deleting it.

### Input schema

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `str` | ✅ | Queue to purge |

::: langchain_rabbitmq.tools.queue.PurgeQueueTool

---

## BindQueueTool

**Tool name:** `rabbitmq_bind_queue`

Bind a queue to an exchange so messages routed to that exchange reach the queue.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `queue` | `str` | ✅ | — | Queue name |
| `exchange` | `str` | ✅ | — | Exchange name |
| `routing_key` | `str` | | `""` | Routing key pattern |
| `arguments` | `dict` | | `{}` | Header arguments for headers-type exchanges |

::: langchain_rabbitmq.tools.queue.BindQueueTool

---

## UnbindQueueTool

**Tool name:** `rabbitmq_unbind_queue`

Remove a binding between a queue and an exchange.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `queue` | `str` | ✅ | — | Queue name |
| `exchange` | `str` | ✅ | — | Exchange name |
| `routing_key` | `str` | | `""` | Routing key that was used to create the binding |

::: langchain_rabbitmq.tools.queue.UnbindQueueTool

---

## GetQueueInfoTool

**Tool name:** `rabbitmq_get_queue_info`

Fetch the current state of a queue: message count, consumer count, and readiness.

### Input schema

| Field | Type | Required | Description |
|---|---|---|---|
| `name` | `str` | ✅ | Queue to inspect |

### Returns

A JSON-formatted string with `messages`, `consumers`, and `state` fields.

::: langchain_rabbitmq.tools.queue.GetQueueInfoTool
