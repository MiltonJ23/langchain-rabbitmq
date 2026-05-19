"""Shared base class for all langchain-rabbitmq LangChain tools.

Every public tool inherits from :class:`_RabbitMQBaseTool`, which provides:

* A ``settings`` field for broker configuration, injectable at construction.
* Factory helpers :meth:`_make_client` and :meth:`_make_async_client`.
* A uniform :meth:`_safe_run` / :meth:`_safe_arun` wrapper that converts
  every :class:`~langchain_rabbitmq.exceptions.RabbitMQToolException` into
  an agent-friendly string via
  :meth:`~langchain_rabbitmq.exceptions.RabbitMQToolException.to_agent_message`.

Concrete tool classes only need to implement ``_execute`` (sync) and
optionally ``_aexecute`` (async).

Example:
    Defining a custom tool using the base::

        class MyTool(_RabbitMQBaseTool):
            name: str = "my_tool"
            description: str = "Does something with a queue."
            args_schema: Type[BaseModel] = MyInput

            def _execute(self, queue: str, ...) -> str:
                with self._make_client() as client:
                    ...
                    return "done"
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from langchain_core.callbacks import (
    AsyncCallbackManagerForToolRun,
    CallbackManagerForToolRun,
)
from langchain_core.tools import BaseTool
from pydantic import Field

from langchain_rabbitmq.config import RabbitMQSettings
from langchain_rabbitmq.exceptions import RabbitMQToolException
from langchain_rabbitmq.utilities.async_rabbitmq import AsyncRabbitMQClient
from langchain_rabbitmq.utilities.rabbitmq import RabbitMQClient

logger = logging.getLogger(__name__)


class _RabbitMQBaseTool(BaseTool):
    """Abstract base that wires RabbitMQ credentials into LangChain tools.

    Attributes:
        settings: Broker configuration.  Defaults to
            :class:`~langchain_rabbitmq.config.RabbitMQSettings` loaded from
            the process environment (``RABBITMQ_*`` variables).

    Note:
        Subclasses **must** define ``name``, ``description``, and
        ``args_schema``, and implement :meth:`_execute`.
    """

    settings: RabbitMQSettings = Field(default_factory=RabbitMQSettings)

    # BaseTool already sets arbitrary_types_allowed=True in its model_config,
    # so RabbitMQSettings (a BaseSettings subclass) is accepted without
    # additional configuration.

    # ------------------------------------------------------------------
    # Factories
    # ------------------------------------------------------------------

    def _make_client(self) -> RabbitMQClient:
        """Instantiate a synchronous :class:`.RabbitMQClient`.

        The caller is responsible for closing the client (use ``with`` block).

        Returns:
            A new, unconnected :class:`~langchain_rabbitmq.utilities.rabbitmq.RabbitMQClient`.
        """
        return RabbitMQClient(self.settings)

    def _make_async_client(self) -> AsyncRabbitMQClient:
        """Instantiate an asynchronous :class:`.AsyncRabbitMQClient`.

        Returns:
            A new, unconnected
            :class:`~langchain_rabbitmq.utilities.async_rabbitmq.AsyncRabbitMQClient`.
        """
        return AsyncRabbitMQClient(self.settings)

    # ------------------------------------------------------------------
    # Abstract hooks — override in subclasses
    # ------------------------------------------------------------------

    def _execute(self, **kwargs: Any) -> str:
        """Execute the tool logic synchronously.

        Args:
            **kwargs: Validated fields from ``args_schema``.

        Returns:
            Agent-readable result string.

        Raises:
            NotImplementedError: If the subclass has not overridden this method.
        """
        raise NotImplementedError(f"{type(self).__name__} does not implement _execute")

    async def _aexecute(self, **kwargs: Any) -> str:
        """Execute the tool logic asynchronously.

        Defaults to running :meth:`_execute` synchronously.  Override for
        true async behaviour.

        Args:
            **kwargs: Validated fields from ``args_schema``.

        Returns:
            Agent-readable result string.
        """
        # Default: delegate to sync implementation.
        # Subclasses with aio-pika support should override this.
        return self._execute(**kwargs)

    # ------------------------------------------------------------------
    # BaseTool integration
    # ------------------------------------------------------------------

    def _run(
        self,
        *args: Any,
        run_manager: Optional[CallbackManagerForToolRun] = None,
        **kwargs: Any,
    ) -> str:
        """Invoke the tool synchronously, converting exceptions to strings.

        Args:
            *args: Positional arguments forwarded to :meth:`_execute`.
            run_manager: LangChain callback manager.
            **kwargs: Keyword arguments forwarded to :meth:`_execute`.

        Returns:
            Agent-readable result string, or an error string on failure.
        """
        try:
            return self._execute(*args, **kwargs)
        except RabbitMQToolException as exc:
            logger.warning("Tool %s failed: %s", self.name, exc.to_agent_message())
            return exc.to_agent_message()
        except Exception as exc:  # noqa: BLE001
            msg = f"[INTERNAL_ERROR] Unexpected error in {self.name}: {exc}"
            logger.exception("Unexpected error in tool %s", self.name)
            return msg

    async def _arun(
        self,
        *args: Any,
        run_manager: Optional[AsyncCallbackManagerForToolRun] = None,
        **kwargs: Any,
    ) -> str:
        """Invoke the tool asynchronously, converting exceptions to strings.

        Args:
            *args: Positional arguments forwarded to :meth:`_aexecute`.
            run_manager: LangChain async callback manager.
            **kwargs: Keyword arguments forwarded to :meth:`_aexecute`.

        Returns:
            Agent-readable result string, or an error string on failure.
        """
        try:
            return await self._aexecute(*args, **kwargs)
        except RabbitMQToolException as exc:
            logger.warning("Tool %s failed (async): %s", self.name, exc.to_agent_message())
            return exc.to_agent_message()
        except Exception as exc:  # noqa: BLE001
            msg = f"[INTERNAL_ERROR] Unexpected error in {self.name}: {exc}"
            logger.exception("Unexpected error in tool %s (async)", self.name)
            return msg


__all__: list[str] = ["_RabbitMQBaseTool"]
