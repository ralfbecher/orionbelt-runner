"""Tests for HttpObslClient auth wiring + error translation (OBSL >= 2.16)."""
# The version gate is a floor (MINIMUM_OBSL_*) plus a newer-than-tested warning
# (TESTED_OBSL_*); see client.py.

from __future__ import annotations

from collections.abc import Callable

import httpx
import pytest

from orionbelt_runner.client import (
    HttpObslClient,
    ObslAuthError,
    ObslPreflightError,
    ObslSchemaError,
    ObslVersionError,
)
from orionbelt_runner.spec import ObslSpec


def _health(version: str = "2.25.0", auth_mode: str = "none") -> Callable[..., httpx.Response]:
    body = {"status": "ok", "version": version, "auth_mode": auth_mode}

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=body)

    return handler


def _wire(client: HttpObslClient, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    """Swap the client's transport for a mock, preserving its configured headers."""
    inner = client._client
    client._client = httpx.Client(
        base_url=inner.base_url,
        headers=inner.headers,
        transport=httpx.MockTransport(handler),
    )


def test_sends_api_key_in_default_header() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, json={"status": "ok"})

    client = HttpObslClient("http://obsl.test", api_key="obsl_pat_secret")
    _wire(client, handler)
    client.health()

    assert seen["x-api-key"] == "obsl_pat_secret"
    assert "authorization" not in seen


def test_custom_header_name() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, json={"status": "ok"})

    client = HttpObslClient("http://obsl.test", api_key="k", api_key_header="X-Custom-Key")
    _wire(client, handler)
    client.health()

    assert seen["x-custom-key"] == "k"


def test_no_key_sends_no_auth_header() -> None:
    seen: dict[str, str] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen.update(request.headers)
        return httpx.Response(200, json={"status": "ok"})

    client = HttpObslClient("http://obsl.test")
    _wire(client, handler)
    client.health()

    assert "x-api-key" not in seen


def test_401_without_key_raises_auth_error_with_hint() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            401,
            json={"detail": {"code": "auth_required", "message": "API key required"}},
        )

    client = HttpObslClient("http://obsl.test")
    _wire(client, handler)

    with pytest.raises(ObslAuthError) as exc:
        client.health()
    msg = str(exc.value)
    assert "No API key was sent" in msg
    assert "auth_required" in msg


def test_403_with_key_raises_auth_error_naming_header() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            403,
            json={"detail": {"code": "auth_invalid", "message": "Invalid API key"}},
        )

    client = HttpObslClient("http://obsl.test", api_key="wrong", api_key_header="X-API-Key")
    _wire(client, handler)

    with pytest.raises(ObslAuthError) as exc:
        client.health()
    msg = str(exc.value)
    assert "'X-API-Key'" in msg
    assert "rejected" in msg


def test_non_auth_error_is_plain_http_error() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(500, text="boom")

    client = HttpObslClient("http://obsl.test")
    _wire(client, handler)

    with pytest.raises(httpx.HTTPStatusError) as exc:
        client.health()
    assert not isinstance(exc.value, ObslAuthError)


def test_preflight_passes_on_supported_version() -> None:
    client = HttpObslClient("http://obsl.test")
    _wire(client, _health(version="2.25.3"))
    payload = client.check_compatibility()
    assert payload["version"] == "2.25.3"


def test_preflight_rejects_below_floor() -> None:
    client = HttpObslClient("http://obsl.test")
    _wire(client, _health(version="2.15.0"))
    with pytest.raises(ObslVersionError) as exc:
        client.check_compatibility()
    assert "too old" in str(exc.value)


def test_preflight_accepts_version_between_floor_and_tested() -> None:
    client = HttpObslClient("http://obsl.test")
    _wire(client, _health(version="2.23.1"))
    assert client.check_compatibility()["version"] == "2.23.1"


def test_preflight_accepts_newer_than_tested_version() -> None:
    """A newer server proceeds. The exact-equality gate this replaced blocked
    any model written against a newer authoring surface until the runner cut a
    release of its own."""
    client = HttpObslClient("http://obsl.test")
    _wire(client, _health(version="2.26.0"))
    assert client.check_compatibility()["version"] == "2.26.0"


def test_422_schema_error_lists_offending_fields() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            422,
            json={
                "detail": {
                    "message": "Request failed JSON Schema validation.",
                    "errors": [
                        {
                            "code": "additional_properties",
                            "message": "order_by not permitted",
                            "path": "(root)",
                        },
                    ],
                }
            },
        )

    client = HttpObslClient("http://obsl.test")
    _wire(client, handler)

    with pytest.raises(ObslSchemaError) as exc:
        client.health()
    msg = str(exc.value)
    assert "JSON Schema validation" in msg
    assert "order_by not permitted" in msg
    assert "camelCase" in msg


def test_preflight_tolerates_missing_version() -> None:
    client = HttpObslClient("http://obsl.test")

    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"status": "ok", "auth_mode": "none"})

    _wire(client, handler)
    client.check_compatibility()  # no raise


def test_preflight_requires_key_when_server_enforces_auth() -> None:
    client = HttpObslClient("http://obsl.test")
    _wire(client, _health(auth_mode="api_key"))
    with pytest.raises(ObslPreflightError) as exc:
        client.check_compatibility()
    assert "AUTH_MODE=api_key" in str(exc.value)


def test_preflight_ok_with_key_and_auth_enforced() -> None:
    client = HttpObslClient("http://obsl.test", api_key="obsl_pat_x")
    _wire(client, _health(auth_mode="api_key"))
    client.check_compatibility()  # no raise


def test_spec_api_key_defaults() -> None:
    assert ObslSpec(api_key="k").api_key == "k"
    assert ObslSpec().api_key is None
    assert ObslSpec().api_key_header == "X-API-Key"
