"""Unit tests for RabbitMQSettings configuration."""

from __future__ import annotations

import pytest
from pydantic import SecretStr, ValidationError

from langchain_rabbitmq.config import RabbitMQSettings

pytestmark = pytest.mark.unit



class TestDefaults:
    def test_default_host(self) -> None:
        s = RabbitMQSettings()
        assert s.host == "localhost"

    def test_default_port(self) -> None:
        assert RabbitMQSettings().port == 5672

    def test_default_virtual_host(self) -> None:
        assert RabbitMQSettings().virtual_host == "/"

    def test_default_username(self) -> None:
        assert RabbitMQSettings().username == "guest"

    def test_default_password_is_secret_str(self) -> None:
        s = RabbitMQSettings()
        assert isinstance(s.password, SecretStr)
        assert s.password.get_secret_value() == "guest"

    def test_password_not_leaked_in_repr(self) -> None:
        s = RabbitMQSettings()
        assert "guest" not in repr(s.password)

    def test_default_heartbeat(self) -> None:
        assert RabbitMQSettings().heartbeat == 60

    def test_default_ssl_disabled(self) -> None:
        assert RabbitMQSettings().ssl_enabled is False

    def test_default_retries(self) -> None:
        assert RabbitMQSettings().max_retries == 3


class TestValidators:
    def test_empty_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host must not be empty"):
            RabbitMQSettings(host="")

    def test_whitespace_host_rejected(self) -> None:
        with pytest.raises(ValidationError, match="host must not be empty"):
            RabbitMQSettings(host="   ")

    def test_empty_vhost_rejected(self) -> None:
        with pytest.raises(ValidationError, match="virtual_host must not be empty"):
            RabbitMQSettings(virtual_host="")

    def test_port_bounds(self) -> None:
        with pytest.raises(ValidationError):
            RabbitMQSettings(port=0)
        with pytest.raises(ValidationError):
            RabbitMQSettings(port=65536)

    def test_ssl_paths_require_ssl_enabled(self) -> None:
        with pytest.raises(ValidationError, match="ssl_ca_certs"):
            RabbitMQSettings(ssl_enabled=False, ssl_ca_certs="/etc/ca.pem")

    def test_ssl_paths_allowed_when_ssl_enabled(self) -> None:
        # Should not raise
        s = RabbitMQSettings(
            ssl_enabled=True,
            ssl_ca_certs="/etc/ca.pem",
            ssl_certfile="/etc/cert.pem",
            ssl_keyfile="/etc/key.pem",
        )
        assert s.ssl_ca_certs == "/etc/ca.pem"


class TestAmqpUrl:
    def test_default_amqp_url(self) -> None:
        s = RabbitMQSettings()
        assert s.amqp_url == "amqp://guest:guest@localhost:5672/%2F"

    def test_amqps_when_ssl_enabled(self) -> None:
        s = RabbitMQSettings(ssl_enabled=True)
        assert s.amqp_url.startswith("amqps://")

    def test_vhost_slash_encoded(self) -> None:
        s = RabbitMQSettings(virtual_host="/")
        assert "%2F" in s.amqp_url

    def test_custom_vhost_encoded(self) -> None:
        s = RabbitMQSettings(virtual_host="my/vhost")
        # "my%2Fvhost" should appear — both slash and no-slash chars encoded
        assert "my%2Fvhost" in s.amqp_url

    def test_host_and_port_in_url(self) -> None:
        s = RabbitMQSettings(host="broker.example.com", port=5673)
        assert "broker.example.com:5673" in s.amqp_url


class TestManagementCredentials:
    def test_management_username_falls_back_to_amqp(self) -> None:
        s = RabbitMQSettings(username="admin")
        assert s.effective_management_username == "admin"

    def test_management_username_override(self) -> None:
        s = RabbitMQSettings(management_username="mgmt_user")
        assert s.effective_management_username == "mgmt_user"

    def test_management_password_falls_back_to_amqp(self) -> None:
        s = RabbitMQSettings(password=SecretStr("s3cr3t"))  # type: ignore[arg-type]
        assert s.effective_management_password == "s3cr3t"

    def test_management_password_override(self) -> None:
        s = RabbitMQSettings(management_password=SecretStr("mgmt_pass"))  # type: ignore[arg-type]
        assert s.effective_management_password == "mgmt_pass"


class TestEnvVarOverride:
    def test_host_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_HOST", "mybroker.internal")
        s = RabbitMQSettings()
        assert s.host == "mybroker.internal"

    def test_port_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_PORT", "5671")
        s = RabbitMQSettings()
        assert s.port == 5671

    def test_ssl_enabled_from_env(self, monkeypatch: pytest.MonkeyPatch) -> None:
        monkeypatch.setenv("RABBITMQ_SSL_ENABLED", "true")
        s = RabbitMQSettings()
        assert s.ssl_enabled is True
