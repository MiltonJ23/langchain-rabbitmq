# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

---

## [0.1.0] - 2024-05-20

### Added

#### Core infrastructure
- `RabbitMQSettings` — Pydantic v2 settings model loading credentials from
  environment variables (`RABBITMQ_*`), with SSL/TLS and Management API support
- Custom exception hierarchy: `RabbitMQToolException` base with concrete subtypes
  `RabbitMQConnectionError`, `RabbitMQChannelError`, `RabbitMQMessageError`,
  `RabbitMQAdminError`
- `_RabbitMQBaseTool` — abstract base class wiring settings, client factories,
  retry logic, and LangChain callback support into every tool

#### AMQP clients
- `RabbitMQClient` — synchronous blocking client (`pika`) with connection pooling,
  retry via `tenacity`, and context-manager lifecycle
- `AsyncRabbitMQClient` — async client (`aio-pika`) with identical interface
- `ManagementAPIClient` / `AsyncManagementAPIClient` — HTTP clients for the
  RabbitMQ Management Plugin REST API

#### Tools (21 total)
**Queue management (6)**
- `DeclareQueueTool` — declare durable/transient queues with full argument support
- `DeleteQueueTool` — delete queues with optional if-empty / if-unused guards
- `PurgeQueueTool` — purge all messages from a queue
- `BindQueueTool` — bind a queue to an exchange
- `UnbindQueueTool` — remove a queue–exchange binding
- `GetQueueInfoTool` — fetch message count, consumer count, and queue state

**Exchange management (3)**
- `DeclareExchangeTool` — declare direct / fanout / topic / headers exchanges
- `DeleteExchangeTool` — delete an exchange
- `BindExchangeTool` — create exchange-to-exchange bindings

**Message operations (5)**
- `PublishMessageTool` — publish with routing key, headers, TTL, and persistence flags
- `ConsumeMessageTool` — pull a single message (basic.get)
- `AckMessageTool` — acknowledge a delivery by tag
- `NackMessageTool` — negative-acknowledge with optional requeue
- `RejectMessageTool` — reject a single delivery

**Admin & monitoring (7)**
- `ListQueuesTool` — list all queues with message and consumer stats
- `ListExchangesTool` — list all exchanges
- `ListBindingsTool` — list all bindings
- `GetNodeStatsTool` — fetch node CPU, memory, and socket metrics
- `CheckHealthTool` — ping the broker and report health status
- `GetConnectionInfoTool` — report open connections and channel count
- `CloseConnectionTool` — gracefully close the AMQP connection

#### Toolkit
- `RabbitMQToolkit` — `BaseToolkit` returning all 21 tools pre-configured with
  shared `RabbitMQSettings`; supports `from_settings()` factory

#### Tests
- 287 unit tests with mocked AMQP/HTTP clients; 93 %+ coverage
- 49 integration tests against a live `rabbitmq:3.12-management` container
  (Testcontainers)
- 26 E2E agent tests using `create_agent` with a deterministic mock LLM

#### CI/CD
- GitHub Actions CI: lint (`ruff`), type-check (`pyrefly` strict), security
  (`bandit`), test matrix across Python 3.9 – 3.12
- Release workflow: trusted PyPI publishing via OIDC on `v*.*.*` tag push

#### Documentation
- Full `README.md` with badges, tools table, configuration reference, and
  compatibility matrix
- `CHANGELOG.md` (this file)
- MkDocs site with Material theme and mkdocstrings API reference
- Three cookbook guides: basic agent, async agent, advanced patterns

[Unreleased]: https://github.com/MiltonJ23/langchain-rabbitmq/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/MiltonJ23/langchain-rabbitmq/releases/tag/v0.1.0
