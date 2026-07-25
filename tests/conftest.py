"""Shared fixtures — chiefly the OBSL stub (see :mod:`tests.obsl_stub`)."""

from __future__ import annotations

from collections.abc import Iterator

import httpx
import pytest

from orionbelt_runner.client import HttpObslClient
from tests.obsl_stub import StubObsl, serve


@pytest.fixture
def obsl_stub() -> StubObsl:
    """A stub OBSL answering 2.23-shaped payloads. Configure fields per test."""
    return StubObsl()


@pytest.fixture
def stub_client(obsl_stub: StubObsl) -> Iterator[HttpObslClient]:
    """A real ``HttpObslClient`` wired to the stub through a mock transport.

    Exercises the full client — params, headers, content-type negotiation,
    response parsing — without a socket.
    """
    client = HttpObslClient("http://obsl.test")
    inner = client._client
    client._client = httpx.Client(
        base_url=inner.base_url, headers=inner.headers, transport=obsl_stub.transport()
    )
    try:
        yield client
    finally:
        client.close()


@pytest.fixture
def stub_server(obsl_stub: StubObsl) -> Iterator[tuple[str, StubObsl]]:
    """The stub on a loopback port, for CLI-level end-to-end tests."""
    with serve(obsl_stub) as (base_url, stub):
        yield base_url, stub
