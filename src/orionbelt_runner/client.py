"""OBSL client interface and HTTP implementation.

The runner talks to OBSL through a small ``ObslClient`` protocol. Today there
is one implementation (HTTP). The protocol is the seam — tests can drop in a
fake, and an in-process implementation can be added later without touching the
runner, report, or CLI code.
"""

from __future__ import annotations

from typing import Any, Protocol

import httpx
from pydantic import BaseModel, ConfigDict, Field, field_validator


class ColumnMetadata(BaseModel):
    """Per-column metadata returned by OBSL alongside rows."""

    model_config = ConfigDict(extra="ignore")

    name: str
    type: str = "string"  # "string" | "number" | "datetime" | "binary"
    format: str | None = None


class ExecuteResult(BaseModel):
    """Rows + metadata returned from POST /v1/query/execute (or shortcut)."""

    model_config = ConfigDict(extra="ignore")

    sql: str
    dialect: str
    columns: list[ColumnMetadata] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    warnings: list[str] = Field(default_factory=list)

    @field_validator("columns", mode="before")
    @classmethod
    def _wrap_string_columns(cls, value: Any) -> Any:
        # Tolerate legacy/test inputs that pass plain column-name strings.
        if isinstance(value, list):
            return [{"name": v} if isinstance(v, str) else v for v in value]
        return value


class CompileResult(BaseModel):
    """SQL + metadata returned from POST /v1/query/sql (or shortcut)."""

    sql: str
    dialect: str
    warnings: list[str] = Field(default_factory=list)
    sql_valid: bool = True


class SessionInfo(BaseModel):
    """Subset of POST /v1/sessions response the runner needs."""

    model_config = ConfigDict(extra="ignore")

    session_id: str


class ModelLoadResult(BaseModel):
    """Subset of POST /v1/sessions/{id}/models response the runner needs."""

    model_config = ConfigDict(extra="ignore")

    model_id: str
    data_objects: int = 0
    dimensions: int = 0
    measures: int = 0
    metrics: int = 0
    warnings: list[str] = Field(default_factory=list)


class ObslClient(Protocol):
    """Minimal subset of the OBSL REST surface the runner depends on."""

    def health(self) -> dict[str, Any]: ...

    def settings(self) -> dict[str, Any]: ...

    def create_session(self, *, metadata: dict[str, str] | None = None) -> SessionInfo: ...

    def load_model(
        self,
        session_id: str,
        *,
        model_yaml: str,
        extends: list[str] | None = None,
    ) -> ModelLoadResult: ...

    def close_session(self, session_id: str) -> None: ...

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
        session_id: str | None = None,
        format_values: bool = True,
        locale: str | None = None,
        timezone: str | None = None,
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

    def create_session(self, *, metadata: dict[str, str] | None = None) -> SessionInfo:
        r = self._client.post("/v1/sessions", json={"metadata": metadata or {}})
        r.raise_for_status()
        return SessionInfo.model_validate(r.json())

    def load_model(
        self,
        session_id: str,
        *,
        model_yaml: str,
        extends: list[str] | None = None,
    ) -> ModelLoadResult:
        body: dict[str, Any] = {"model_yaml": model_yaml}
        if extends:
            body["extends"] = extends
        r = self._client.post(f"/v1/sessions/{session_id}/models", json=body)
        r.raise_for_status()
        return ModelLoadResult.model_validate(r.json())

    def close_session(self, session_id: str) -> None:
        r = self._client.delete(f"/v1/sessions/{session_id}")
        # 204 on success, 404 if already gone — both are fine for cleanup.
        if r.status_code not in (204, 404):
            r.raise_for_status()

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
        session_id: str | None = None,
        format_values: bool = True,
        locale: str | None = None,
        timezone: str | None = None,
    ) -> ExecuteResult:
        if session_id is not None:
            if model_id is None:
                raise ValueError("model_id is required when session_id is set")
            path = f"/v1/sessions/{session_id}/query/execute"
            body: dict[str, Any] = {"model_id": model_id, "query": query, "dialect": dialect}
        else:
            path = self._query_path("execute", model_id)
            body = self._build_body(query, dialect, model_id)
        params: dict[str, str] = {"format_values": "true" if format_values else "false"}
        if locale is not None:
            params["locale"] = locale
        if timezone is not None:
            params["timezone"] = timezone
        r = self._client.post(path, json=body, params=params)
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
