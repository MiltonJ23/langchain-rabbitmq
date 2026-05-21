"""Connection and runtime configuration for langchain-rabbitmq.

Configuration is managed through :class:`RabbitMQSettings`, a
:class:`pydantic_settings.BaseSettings` subclass.  Every field is overridable
via the corresponding environment variable (prefix ``RABBITMQ_``):

.. code-block:: bash

    export RABBITMQ_HOST=broker.prod.internal
    export RABBITMQ_PORT=5671
    export RABBITMQ_PASSWORD=s3cr3t
    export RABBITMQ_SSL_ENABLED=true
    export RABBITMQ_MANAGEMENT_API_URL=http://broker.prod.internal:15672

Example:
    Loading settings from the environment and building a connection URL::

        from langchain_rabbitmq.config import RabbitMQSettings

        settings = RabbitMQSettings()
        print(settings.amqp_url)
        # amqp://guest:guest@localhost:5672/%2F
"""

from __future__ import annotations

from urllib.parse import quote

from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class RabbitMQSettings(BaseSettings):
    """Runtime configuration for a RabbitMQ broker connection.

    All fields can be supplied via environment variables prefixed with
    ``RABBITMQ_``.  An optional ``.env`` file at the project root is also
    loaded automatically.

    Attributes:
        host: Broker hostname or IP address.
        port: AMQP port.  Use ``5671`` when TLS is enabled.
        virtual_host: RabbitMQ virtual host (default: ``/``).
        username: AMQP authentication username.
        password: AMQP password stored as :class:`~pydantic.SecretStr` so it
            is never printed in logs or ``repr`` output.
        heartbeat: AMQP heartbeat interval in seconds.  Set to ``0`` to
            disable heartbeats (not recommended in production).
        connection_timeout: Seconds to wait when opening a TCP connection.
        channel_timeout: Seconds to wait for channel-level AMQP operations.
        ssl_enabled: Enable TLS for the AMQP connection.
        ssl_ca_certs: Filesystem path to a PEM CA certificate bundle.
        ssl_certfile: Filesystem path to a PEM client certificate.
        ssl_keyfile: Filesystem path to a PEM client private key.
        management_api_url: Base URL of the RabbitMQ Management HTTP API
            (e.g. ``http://localhost:15672``).  Required for admin/monitoring
            tools.
        management_username: Username for the Management API.  Falls back to
            :attr:`username` when not set.
        management_password: Password for the Management API stored as
            :class:`~pydantic.SecretStr`.  Falls back to :attr:`password`
            when not set.
        max_retries: Maximum number of retry attempts for transient errors.
        retry_delay: Seconds to wait between consecutive retry attempts.

    Example:
        Override host and port via environment variables::

            import os
            os.environ["RABBITMQ_HOST"] = "mybroker.example.com"
            os.environ["RABBITMQ_PORT"] = "5671"
            os.environ["RABBITMQ_SSL_ENABLED"] = "true"

            from langchain_rabbitmq.config import RabbitMQSettings
            settings = RabbitMQSettings()
            assert settings.host == "mybroker.example.com"
            assert settings.ssl_enabled is True
    """

    model_config = SettingsConfigDict(
        env_prefix="RABBITMQ_",
        env_file=".env",
        env_file_encoding="utf-8",
        populate_by_name=True,
    )

    # ------------------------------------------------------------------
    # Core connection
    # ------------------------------------------------------------------
    host: str = Field(
        default="localhost",
        description="Broker hostname or IP address",
    )
    port: int = Field(
        default=5672,
        ge=1,
        le=65535,
        description="AMQP port (5672 plain, 5671 TLS)",
    )
    virtual_host: str = Field(
        default="/",
        description="RabbitMQ virtual host",
    )
    username: str = Field(
        default="guest",
        description="AMQP authentication username",
    )
    password: SecretStr = Field(
        default=SecretStr("guest"),
        description="AMQP password (never logged in plain text)",
    )

    # ------------------------------------------------------------------
    # AMQP tuning
    # ------------------------------------------------------------------
    heartbeat: int = Field(
        default=60,
        ge=0,
        description="AMQP heartbeat interval in seconds (0 to disable)",
    )
    connection_timeout: float = Field(
        default=10.0,
        gt=0.0,
        description="TCP connection timeout in seconds",
    )
    channel_timeout: float = Field(
        default=5.0,
        gt=0.0,
        description="Channel operation timeout in seconds",
    )

    # ------------------------------------------------------------------
    # TLS / SSL
    # ------------------------------------------------------------------
    ssl_enabled: bool = Field(
        default=False,
        description="Enable TLS for the AMQP connection",
    )
    ssl_ca_certs: str | None = Field(
        default=None,
        description="Filesystem path to a PEM CA certificate bundle",
    )
    ssl_certfile: str | None = Field(
        default=None,
        description="Filesystem path to a PEM client certificate",
    )
    ssl_keyfile: str | None = Field(
        default=None,
        description="Filesystem path to a PEM client private key",
    )

    # ------------------------------------------------------------------
    # Management HTTP API
    # ------------------------------------------------------------------
    management_api_url: str | None = Field(
        default=None,
        description=("Base URL of the RabbitMQ Management HTTP API (e.g. http://localhost:15672)"),
    )
    management_username: str | None = Field(
        default=None,
        description="Management API username (falls back to ``username``)",
    )
    management_password: SecretStr | None = Field(
        default=None,
        description=("Management API password stored as SecretStr (falls back to ``password``)"),
    )

    # ------------------------------------------------------------------
    # Resilience
    # ------------------------------------------------------------------
    max_retries: int = Field(
        default=3,
        ge=0,
        description="Maximum retry attempts for transient broker errors",
    )
    retry_delay: float = Field(
        default=1.0,
        ge=0.0,
        description="Seconds to wait between retry attempts",
    )

    # ------------------------------------------------------------------
    # Validators
    # ------------------------------------------------------------------

    @field_validator("host")
    @classmethod
    def _host_not_empty(cls, v: str) -> str:
        """Reject blank host values.

        Args:
            v: Raw host string from environment or caller.

        Returns:
            The validated, unchanged host string.

        Raises:
            ValueError: When ``v`` is empty or whitespace-only.
        """
        if not v.strip():
            raise ValueError("host must not be empty or whitespace")
        return v

    @field_validator("virtual_host")
    @classmethod
    def _virtual_host_not_empty(cls, v: str) -> str:
        """Reject blank virtual_host values.

        Args:
            v: Raw virtual_host string from environment or caller.

        Returns:
            The validated, unchanged virtual_host string.

        Raises:
            ValueError: When ``v`` is empty or whitespace-only.
        """
        if not v.strip():
            raise ValueError("virtual_host must not be empty or whitespace")
        return v

    @model_validator(mode="after")
    def _ssl_paths_require_ssl_enabled(self) -> RabbitMQSettings:
        """Guard against supplying SSL certificate paths while TLS is disabled.

        Returns:
            The validated :class:`RabbitMQSettings` instance.

        Raises:
            ValueError: When any of ``ssl_ca_certs``, ``ssl_certfile``, or
                ``ssl_keyfile`` is set while ``ssl_enabled`` is ``False``.
        """
        if not self.ssl_enabled and any([self.ssl_ca_certs, self.ssl_certfile, self.ssl_keyfile]):
            raise ValueError("ssl_ca_certs, ssl_certfile, and ssl_keyfile require ssl_enabled=True")
        return self

    # ------------------------------------------------------------------
    # Computed properties
    # ------------------------------------------------------------------

    @property
    def amqp_url(self) -> str:
        """Construct the AMQP connection URL.

        The virtual host ``/`` is percent-encoded as ``%2F`` per the AMQP
        URI specification (RFC 3986).

        Returns:
            A fully-formed ``amqp://`` or ``amqps://`` URL string with the
            plain-text password embedded.  **Do not log this value.**

        Example:
            >>> s = RabbitMQSettings()
            >>> s.amqp_url
            'amqp://guest:guest@localhost:5672/%2F'
        """
        scheme = "amqps" if self.ssl_enabled else "amqp"
        password = self.password.get_secret_value()
        vhost = quote(self.virtual_host, safe="")
        return f"{scheme}://{self.username}:{password}@{self.host}:{self.port}/{vhost}"

    @property
    def effective_management_username(self) -> str:
        """Management API username, falling back to the AMQP username.

        Returns:
            The resolved management API username string.
        """
        return self.management_username or self.username

    @property
    def effective_management_password(self) -> str:
        """Management API password in plain text, falling back to AMQP password.

        Returns:
            The resolved management API password string.  **Do not log this value.**
        """
        if self.management_password is not None:
            return self.management_password.get_secret_value()
        return self.password.get_secret_value()


__all__: list[str] = ["RabbitMQSettings"]
