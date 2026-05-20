# Configuration

All settings are managed by `RabbitMQSettings`, a Pydantic v2 `BaseSettings` model.
Values are loaded from environment variables with the prefix `RABBITMQ_`.

---

## Environment variables

### Connection

| Variable | Type | Default | Description |
|---|---|---|---|
| `RABBITMQ_URL` | `str \| None` | `None` | Full AMQP URL — takes precedence over individual host/port/user fields if set |
| `RABBITMQ_HOST` | `str` | `localhost` | Broker hostname |
| `RABBITMQ_PORT` | `int` | `5672` | AMQP port |
| `RABBITMQ_VHOST` | `str` | `/` | Virtual host |
| `RABBITMQ_USER` | `str` | `guest` | AMQP username |
| `RABBITMQ_PASSWORD` | `SecretStr` | `guest` | AMQP password (masked in logs) |

### TLS / SSL

| Variable | Type | Default | Description |
|---|---|---|---|
| `RABBITMQ_SSL` | `bool` | `false` | Enable TLS for the AMQP connection |
| `RABBITMQ_SSL_CA_CERTS` | `str \| None` | `None` | Path to CA bundle (PEM) for certificate verification |

### Timeouts

| Variable | Type | Default | Description |
|---|---|---|---|
| `RABBITMQ_CONNECTION_TIMEOUT` | `int` | `10` | Seconds before a connection attempt times out |

### Management API

| Variable | Type | Default | Description |
|---|---|---|---|
| `RABBITMQ_MANAGEMENT_API_URL` | `str \| None` | `None` | Base URL of the Management HTTP API, e.g. `http://localhost:15672` |
| `RABBITMQ_MANAGEMENT_USER` | `str \| None` | `None` | Username for the Management API (falls back to `RABBITMQ_USER`) |
| `RABBITMQ_MANAGEMENT_PASSWORD` | `SecretStr \| None` | `None` | Password for the Management API (falls back to `RABBITMQ_PASSWORD`) |

---

## Programmatic configuration

```python
from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq import RabbitMQToolkit

settings = RabbitMQSettings(
    host="broker.internal",
    port=5671,
    virtual_host="production",
    username="app",
    password="s3cr3t",          # type: ignore[arg-type]  — pydantic SecretStr accepted
    ssl=True,
    ssl_ca_certs="/etc/ssl/ca.pem",
    connection_timeout=30,
    management_api_url="https://broker.internal:15671",
)

toolkit = RabbitMQToolkit(settings=settings)
tools   = toolkit.get_tools()
```

---

## Settings model reference

::: langchain_rabbitmq.config.RabbitMQSettings
    options:
      show_source: true

---

## Validation rules

- Queue and exchange names are validated against the AMQP naming rules (max 255 chars,
  no null bytes)
- `RABBITMQ_PORT` must be in the range 1 – 65535
- `RABBITMQ_CONNECTION_TIMEOUT` must be a positive integer

---

## Secrets in production

Never commit credentials to source control. Use one of:

- **Environment variables** — the default; set `RABBITMQ_*` in your shell or container env
- **`.env` file** — `pydantic-settings` loads `.env` automatically if present; add it to `.gitignore`
- **Secret managers** — inject secrets as env vars from Vault, AWS Secrets Manager, etc.

```bash
# .env (add to .gitignore!)
RABBITMQ_URL=amqp://app:s3cr3t@broker.internal:5672/production
RABBITMQ_MANAGEMENT_API_URL=http://broker.internal:15672
```
