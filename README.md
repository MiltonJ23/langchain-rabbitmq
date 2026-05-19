# langchain-rabbitmq

> LangChain tools for interacting with RabbitMQ — full documentation coming soon.

## Installation

```bash
pip install langchain-rabbitmq
```

## Quick start

```python
from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.tools import RabbitMQToolkit

settings = RabbitMQSettings()          # reads RABBITMQ_* env vars
toolkit  = RabbitMQToolkit.from_settings(settings)
tools    = toolkit.get_tools()
```

*Full documentation, cookbook notebooks, and API reference will be added in an upcoming release.*
