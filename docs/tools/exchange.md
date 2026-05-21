# Exchange management tools

Three tools manage the full lifecycle of a RabbitMQ exchange.

---

## DeclareExchangeTool

**Tool name:** `rabbitmq_declare_exchange`

Declare a new exchange or assert that an existing one matches the expected type.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `str` | ✅ | — | Exchange name |
| `exchange_type` | `str` | | `"direct"` | `direct`, `fanout`, `topic`, or `headers` |
| `durable` | `bool` | | `True` | Survive broker restart |
| `auto_delete` | `bool` | | `False` | Delete when last binding is removed |
| `arguments` | `dict` | | `{}` | Optional exchange arguments |

::: langchain_rabbitmq.tools.exchange.DeclareExchangeTool

---

## DeleteExchangeTool

**Tool name:** `rabbitmq_delete_exchange`

Delete an exchange. Optionally guard against deletion while bindings remain.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `name` | `str` | ✅ | — | Exchange to delete |
| `if_unused` | `bool` | | `False` | Only delete if no bindings exist |

::: langchain_rabbitmq.tools.exchange.DeleteExchangeTool

---

## BindExchangeTool

**Tool name:** `rabbitmq_bind_exchange`

Create an exchange-to-exchange binding (RabbitMQ extension). Messages published
to the *source* exchange that match the routing key are forwarded to the
*destination* exchange.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `source` | `str` | ✅ | — | Source exchange name |
| `destination` | `str` | ✅ | — | Destination exchange name |
| `routing_key` | `str` | | `""` | Routing key for the binding |
| `arguments` | `dict` | | `{}` | Optional binding arguments |

::: langchain_rabbitmq.tools.exchange.BindExchangeTool
