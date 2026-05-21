# Admin & monitoring tools

Seven tools expose broker-level administration and monitoring.
Most of these delegate to the **RabbitMQ Management HTTP API**; ensure
`RABBITMQ_MANAGEMENT_API_URL` is set.

---

## ListQueuesTool

**Tool name:** `rabbitmq_list_queues`

List all queues in the virtual host, including message counts and consumer counts.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `vhost` | `str` | | `/` | Virtual host to query |

### Returns

JSON array of queue objects.

::: langchain_rabbitmq.tools.admin.ListQueuesTool

---

## ListExchangesTool

**Tool name:** `rabbitmq_list_exchanges`

List all exchanges in the virtual host.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `vhost` | `str` | | `/` | Virtual host to query |

::: langchain_rabbitmq.tools.admin.ListExchangesTool

---

## ListBindingsTool

**Tool name:** `rabbitmq_list_bindings`

List all queue-to-exchange and exchange-to-exchange bindings.

### Input schema

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `vhost` | `str` | | `/` | Virtual host to query |

::: langchain_rabbitmq.tools.admin.ListBindingsTool

---

## GetNodeStatsTool

**Tool name:** `rabbitmq_get_node_stats`

Fetch resource metrics for the broker node: memory, CPU, sockets, and uptime.

### Input schema

No required inputs. Optionally specify:

| Field | Type | Required | Default | Description |
|---|---|---|---|---|
| `node_name` | `str \| None` | | `None` | Node to query (defaults to the local node) |

### Returns

JSON object with `mem_used`, `processors`, `sockets_used`, `uptime`, and more.

::: langchain_rabbitmq.tools.admin.GetNodeStatsTool

---

## CheckHealthTool

**Tool name:** `rabbitmq_check_health`

Ping the broker and return its health status.

### Input schema

No inputs required.

### Returns

`"Broker is healthy."` or a description of the failure.

::: langchain_rabbitmq.tools.admin.CheckHealthTool

---

## GetConnectionInfoTool

**Tool name:** `rabbitmq_get_connection_info`

Report the number of open AMQP connections and channels.

### Input schema

No inputs required.

### Returns

JSON object with `connections` and `channels` counts.

::: langchain_rabbitmq.tools.admin.GetConnectionInfoTool

---

## CloseConnectionTool

**Tool name:** `rabbitmq_close_connection`

Gracefully close the current AMQP connection and all its channels.

### Input schema

No inputs required.

!!! warning
    After calling this tool the connection is closed. Subsequent operations will
    open a new connection automatically.

::: langchain_rabbitmq.tools.admin.CloseConnectionTool
