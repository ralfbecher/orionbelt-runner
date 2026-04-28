"""OBSL client interface and HTTP implementation.

The runner talks to OBSL through a small ``ObslClient`` protocol. Today there
is one implementation (HTTP). The protocol is the seam — tests can drop in a
fake, and an in-process implementation can be added later without touching the
runner, report, or CLI code.
"""

from __future__ import annotations

from typing import Any, Protocol

import httpx
from pydantic import BaseModel, Field


class ExecuteResult(BaseModel):
    """Rows + metadata returned from POST /v1/query/execute (or shortcut)."""

    sql: str
    dialect: str
    columns: list[str] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int | None = None
    warnings: list[str] = Field(default_factory=list)


class CompileResult(BaseModel):
    """SQL + metadata returned from POST /v1/query/sql (or shortcut)."""

    sql: str
    dialect: str
    warnings: list[str] = Field(default_factory=list)
    sql_valid: bool = True


class ObslClient(Protocol):
    """Minimal subset of the OBSL REST surface the runner depends on."""

    def health(self) -> dict[str, Any]: ...

    def settings(self) -> dict[str, Any]: ...

    def compile(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
    ) -> CompileResult: ...

    def execute(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
    ) -> ExecuteResult: ...


class HttpObslClient:
    """OBSL client over the REST API.

    Defaults assume single-model mode (``MODEL_FILE`` set on the OBSL server)
    and uses the top-level shortcut endpoints (``/v1/query/{sql,execute}``).
    Pass ``model_id`` to target a specific model on a multi-model deployment.
    """

    def __init__(
        self,
        base_url: str = "http://localhost:8080",
        *,
        api_token: str | None = None,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        headers: dict[str, str] = {"User-Agent": "orionbelt-runner/0.1"}
        if api_token:
            headers["Authorization"] = f"Bearer {api_token}"
        self._client = httpx.Client(
            base_url=self._base_url,
            timeout=timeout_seconds,
            headers=headers,
        )

    def __enter__(self) -> HttpObslClient:
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()

    def close(self) -> None:
        self._client.close()

    # -- public protocol methods -------------------------------------------

    def health(self) -> dict[str, Any]:
        r = self._client.get("/health")
        r.raise_for_status()
        return r.json()  # type: ignore[no-any-return]

    def settings(self) -> dict[str, Any]:
        r = self._client.get("/v1/settings")
        r.raise_for_status()
        return r.json()  # type: ignore[no-any-return]

    def compile(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
    ) -> CompileResult:
        path = self._query_path("sql", model_id)
        body = self._build_body(query, dialect, model_id)
        r = self._client.post(path, json=body)
        r.raise_for_status()
        return CompileResult.model_validate(r.json())

    def execute(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
    ) -> ExecuteResult:
        path = self._query_path("execute", model_id)
        body = self._build_body(query, dialect, model_id)
        r = self._client.post(path, json=body)
        r.raise_for_status()
        return ExecuteResult.model_validate(r.json())

    # -- helpers -----------------------------------------------------------

    def _query_path(self, kind: str, model_id: str | None) -> str:
        # Single-model mode → top-level shortcuts; otherwise a session must
        # already exist server-side and the model_id keys into it (handled by
        # the body, not the path).
        return f"/v1/query/{kind}"

    @staticmethod
    def _build_body(query: dict[str, Any], dialect: str, model_id: str | None) -> dict[str, Any]:
        body: dict[str, Any] = {"query": query, "dialect": dialect}
        if model_id is not None:
            body["model_id"] = model_id
        return body
