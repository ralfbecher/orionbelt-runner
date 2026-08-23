"""OBSL client interface and HTTP implementation.

The runner talks to OBSL through a small ``ObslClient`` protocol. Today there
is one implementation (HTTP). The protocol is the seam — tests can drop in a
fake, and an in-process implementation can be added later without touching the
runner, report, or CLI code.
"""

from __future__ import annotations

import gzip
import json
import re
from dataclasses import dataclass
from typing import Any, Protocol

import httpx
import structlog
from pydantic import BaseModel, ConfigDict, Field, field_validator

from orionbelt_runner import __version__
from orionbelt_runner.arrow_support import require_pyarrow

log = structlog.get_logger("orionbelt_runner.client")

# OBSL >= 2.12 reads the API key from this header by default (configurable
# server-side via API_KEY_HEADER; Authorization: Bearer is also accepted as a
# fallback). Sending it is safe against pre-auth servers — they ignore it.
DEFAULT_API_KEY_HEADER = "X-API-Key"

# Media type of OBSL's ``?format=arrow`` result frame (OBSL >= 2.21). Sent as
# Accept too — the server negotiates on either the parameter or the header.
ARROW_RESULT_MEDIA_TYPE = "application/vnd.orionbelt.result+arrow"

# The OBSL release this runner is developed and tested against: a *floor*, not
# an exact pairing. The runner calls a small, stable slice of the API (unified
# auth, /health, the session + query endpoints, and the JSON-Schema ingestion
# boundary added in 2.16), and OBSL's minor releases have been additive over
# it — most of what a minor ships is authoring surface the *model* uses, which
# the runner only forwards.
#
# So a newer server warns and proceeds rather than failing: the alternative
# pinned the runner to one minor and blocked every model written against a
# newer authoring surface until the runner cut a release of its own, with
# --skip-preflight as the only way through, which also disabled the auth check.
# An older server is still a hard error, because that direction really can be
# missing an endpoint the runner calls.
#
# Raise this floor when the runner starts *depending* on something new, not
# every time OBSL ships a minor.
MINIMUM_OBSL_MAJOR = 2
MINIMUM_OBSL_MINOR = 16

# Newest OBSL minor this runner has been exercised against. Purely
# informational — it decides the wording of the "newer than tested" warning,
# never whether a run proceeds.
TESTED_OBSL_MAJOR = 2
TESTED_OBSL_MINOR = 25

_SEMVER_RE = re.compile(r"\s*v?(\d+)\.(\d+)(?:\.(\d+))?")


class ObslAuthError(httpx.HTTPStatusError):
    """OBSL rejected the request with 401 (key missing) or 403 (key invalid).

    A dedicated subclass so callers can distinguish an auth failure from any
    other HTTP error. The message carries OBSL's structured ``{code, message}``
    detail plus a hint about how to supply the key.
    """


class ObslSchemaError(httpx.HTTPStatusError):
    """OBSL rejected the request body with 422 at its JSON-Schema boundary.

    OBSL >= 2.16 validates model-load and query payloads against the published
    JSON Schemas at ingestion and returns ``422`` with a structured
    ``{detail: {message, errors: [{code, message, path}]}}`` body. A dedicated
    subclass surfaces those per-field errors so a malformed OBML model or query
    in the spec is diagnosable without reading the raw response.
    """


class ObslPreflightError(RuntimeError):
    """A startup compatibility check failed before any query ran."""


class ObslVersionError(ObslPreflightError):
    """The OBSL server version is outside the line this runner supports."""


def _parse_semver(value: str | None) -> tuple[int, int, int] | None:
    """Parse a leading ``[v]MAJOR.MINOR[.PATCH]`` into a tuple, or None."""
    if not value:
        return None
    m = _SEMVER_RE.match(value)
    if not m:
        return None
    return int(m.group(1)), int(m.group(2)), int(m.group(3) or 0)


def _decode_arrow_frame(body: bytes) -> ArrowResult:
    """Decode OBSL's ``?format=arrow`` result frame.

    Layout (OBSL >= 2.21)::

        [u32 big-endian json_len][JSON envelope utf-8][gzip'd Arrow IPC stream]

    The envelope is assembled fresh per request (so timing and the ``cached``
    flag are accurate even on a server cache hit) while the Arrow sub-part
    carries its own gzip and may be shipped verbatim from cache. The table is
    returned as-is — server-side types (``decimal128``, ``timestamp``,
    ``int64``) reach the Parquet / Arrow writers without a JSON round trip.
    """
    pa = require_pyarrow("Arrow result transport")
    if len(body) < 4:
        raise ValueError(f"Arrow result frame is truncated ({len(body)} bytes)")
    meta_len = int.from_bytes(body[:4], "big")
    end = 4 + meta_len
    if len(body) < end:
        raise ValueError(
            f"Arrow result frame declares a {meta_len}-byte envelope "
            f"but carries only {len(body) - 4}"
        )
    meta = ExecuteResult.model_validate(json.loads(body[4:end].decode("utf-8")))
    table = pa.ipc.open_stream(gzip.decompress(body[end:])).read_all()
    return ArrowResult(meta=meta, table=table)


def _flatten_warning(item: Any) -> str:
    """Render one OBSL warning as a line of text.

    OBSL returns structured warnings — ``{code, severity, message, path, hint,
    context}`` — on execute, compile, and model-load responses. The runner logs
    warnings as plain lines (run log, structlog), so they're flattened to
    ``"code: message (hint: …)"`` on the way in. Plain strings pass through, so
    older servers and test fixtures keep working.
    """
    if isinstance(item, str):
        return item
    if isinstance(item, dict):
        code = item.get("code")
        message = item.get("message") or item.get("detail") or ""
        hint = item.get("hint")
        text = f"{code}: {message}" if code and message else str(code or message or item)
        return f"{text} (hint: {hint})" if hint else text
    return str(item)


class ObslResponse(BaseModel):
    """Base for OBSL response models that can carry structured warnings."""

    model_config = ConfigDict(extra="ignore")

    warnings: list[str] = Field(default_factory=list)

    @field_validator("warnings", mode="before")
    @classmethod
    def _flatten_warnings(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [_flatten_warning(w) for w in value]
        return value


class ColumnMetadata(BaseModel):
    """Per-column metadata returned by OBSL alongside rows."""

    model_config = ConfigDict(extra="ignore")

    name: str
    # "string" | "number" | "datetime" | "binary" | "decimal(p, s)" — OBSL
    # reports governed DECIMAL columns with their precision and scale.
    type: str = "string"
    format: str | None = None


class ResolvedInfo(BaseModel):
    """Mirrors OBSL's ResolvedInfoResponse — what was resolved during compilation."""

    model_config = ConfigDict(extra="ignore")

    fact_tables: list[str] = Field(default_factory=list)
    dimensions: list[str] = Field(default_factory=list)
    measures: list[str] = Field(default_factory=list)


class ExplainJoin(BaseModel):
    """Explanation of a single join step (mirrors OBSL ExplainJoinResponse)."""

    model_config = ConfigDict(extra="ignore")

    from_object: str
    to_object: str
    join_columns: list[str] = Field(default_factory=list)
    reason: str


class ExplainCflLeg(BaseModel):
    """Explanation of a single CFL leg (mirrors OBSL ExplainCflLegResponse)."""

    model_config = ConfigDict(extra="ignore")

    measure_source: str
    common_root: str
    reason: str
    measures: list[str] = Field(default_factory=list)
    joins: list[str] = Field(default_factory=list)


class ExplainPlan(BaseModel):
    """Full query plan explanation (mirrors OBSL ExplainPlanResponse).

    Captured into the run log alongside the compiled SQL so a reader can
    see *why* OBSL picked a given plan, not just *what* it ran.
    """

    model_config = ConfigDict(extra="ignore")

    planner: str
    planner_reason: str
    base_object: str
    base_object_reason: str
    joins: list[ExplainJoin] = Field(default_factory=list)
    where_filter_count: int = 0
    having_filter_count: int = 0
    has_totals: bool = False
    cfl_legs: list[ExplainCflLeg] = Field(default_factory=list)


class ExecuteResult(ObslResponse):
    """Rows + metadata returned from POST /v1/query/execute (or shortcut).

    Also models the JSON envelope of the ``format=arrow`` frame, which carries
    every field here except ``rows`` (those ride in the Arrow sub-part).
    """

    sql: str
    dialect: str
    columns: list[ColumnMetadata] = Field(default_factory=list)
    rows: list[list[Any]] = Field(default_factory=list)
    row_count: int = 0
    execution_time_ms: float = 0.0
    timezone: str | None = None
    sql_valid: bool = True
    resolved: ResolvedInfo = Field(default_factory=ResolvedInfo)
    explain: ExplainPlan | None = None

    @field_validator("columns", mode="before")
    @classmethod
    def _wrap_string_columns(cls, value: Any) -> Any:
        # Tolerate legacy/test inputs that pass plain column-name strings.
        if isinstance(value, list):
            return [{"name": v} if isinstance(v, str) else v for v in value]
        return value


@dataclass(frozen=True)
class ArrowResult:
    """Outcome of an Arrow-transport execute.

    ``meta`` is the JSON envelope — every ``ExecuteResult`` field except the
    rows (sql, columns, timing, warnings, resolved, explain, row_count), so
    the run log is written from it unchanged.

    ``table`` is the ``pyarrow.Table`` carrying the typed rows, or ``None``
    when the server answered in JSON instead (a deployment that doesn't know
    ``format=arrow``). In that fallback the rows ride in ``meta.rows`` and the
    exporter infers types from them, so callers handle one shape either way —
    use ``exports.to_arrow_table()`` rather than reading ``table`` directly.
    """

    meta: ExecuteResult
    table: Any | None = None

    @property
    def from_arrow_transport(self) -> bool:
        """True when the server sent typed Arrow rather than JSON."""
        return self.table is not None


class CompileResult(ObslResponse):
    """SQL + metadata returned from POST /v1/query/sql (or shortcut)."""

    sql: str
    dialect: str
    sql_valid: bool = True
    resolved: ResolvedInfo = Field(default_factory=ResolvedInfo)
    explain: ExplainPlan | None = None


class SessionInfo(BaseModel):
    """Subset of POST /v1/sessions response the runner needs."""

    model_config = ConfigDict(extra="ignore")

    session_id: str


class ModelLoadResult(ObslResponse):
    """Subset of POST /v1/sessions/{id}/models response the runner needs."""

    model_id: str
    data_objects: int = 0
    dimensions: int = 0
    measures: int = 0
    metrics: int = 0


class MeasureSummary(BaseModel):
    """Subset of MeasureDetail the preflight check needs."""

    model_config = ConfigDict(extra="ignore", populate_by_name=True)

    name: str
    format: str | None = None
    data_type: str | None = Field(default=None, alias="dataType")
    result_type: str | None = Field(default=None, alias="result_type")
    description: str | None = None


class ObslClient(Protocol):
    """Minimal subset of the OBSL REST surface the runner depends on."""

    def health(self) -> dict[str, Any]: ...

    def settings(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]: ...

    def create_session(self, *, metadata: dict[str, str] | None = None) -> SessionInfo: ...

    def load_model(
        self,
        session_id: str,
        *,
        model_yaml: str,
        extends: list[str] | None = None,
    ) -> ModelLoadResult: ...

    def close_session(self, session_id: str) -> None: ...

    def list_measures(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> list[MeasureSummary]: ...

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

    def execute_arrow(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
        session_id: str | None = None,
        timezone: str | None = None,
    ) -> ArrowResult: ...


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
        api_key: str | None = None,
        api_key_header: str = DEFAULT_API_KEY_HEADER,
        timeout_seconds: float = 30.0,
    ) -> None:
        self._base_url = base_url.rstrip("/")
        self._api_key_header = api_key_header or DEFAULT_API_KEY_HEADER
        self._has_key = bool(api_key)
        headers: dict[str, str] = {"User-Agent": f"orionbelt-runner/{__version__}"}
        if api_key:
            headers[self._api_key_header] = api_key
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

    # -- startup compatibility check ---------------------------------------

    def check_compatibility(self) -> dict[str, Any]:
        """Check the server before a run and fail fast with a clear message.

        Calls the unauthenticated ``/health`` endpoint (which reports the OBSL
        release version and the active auth mode) and then verifies:

        * the server version is at or above the supported floor (a newer
          server warns and proceeds; an older one is an error), and
        * a key is configured when the server enforces ``AUTH_MODE=api_key``.

        Returns the ``/health`` payload. Raises :class:`ObslVersionError` or
        :class:`ObslPreflightError` on a failed check; transport failures
        (server unreachable) propagate as ``httpx`` errors.
        """
        payload = self.health()
        self._check_version(payload.get("version"))
        self._check_auth_mode(payload.get("auth_mode"))
        return payload

    def _check_version(self, version: Any) -> None:
        reported = version if isinstance(version, str) else None
        parsed = _parse_semver(reported)
        if parsed is None:
            # Don't brick the run on a missing / unparseable version string —
            # warn and proceed (older servers may not report one).
            log.warning("obsl_version_unparsed", reported=reported)
            return
        major, minor, _patch = parsed
        if (major, minor) < (MINIMUM_OBSL_MAJOR, MINIMUM_OBSL_MINOR):
            floor = f"{MINIMUM_OBSL_MAJOR}.{MINIMUM_OBSL_MINOR}"
            raise ObslVersionError(
                f"OBSL server reports version {reported!r}, but orionbelt-runner "
                f"{__version__} requires OBSL {floor} or newer: too old — "
                f"upgrade OBSL."
            )
        if (major, minor) > (TESTED_OBSL_MAJOR, TESTED_OBSL_MINOR):
            # Not an error. The runner uses a stable slice of the API and a
            # newer server is expected to serve it; say so and carry on, so a
            # model written against a newer authoring surface is not blocked
            # behind a runner release.
            log.warning(
                "obsl_version_newer_than_tested",
                reported=reported,
                tested=f"{TESTED_OBSL_MAJOR}.{TESTED_OBSL_MINOR}.x",
                runner=__version__,
            )

    def _check_auth_mode(self, auth_mode: Any) -> None:
        if auth_mode == "api_key" and not self._has_key:
            raise ObslPreflightError(
                "OBSL has AUTH_MODE=api_key but no API key is configured. "
                "Set obsl.api_key in the spec or OBSL_API_KEY in the environment."
            )

    # -- public protocol methods -------------------------------------------

    def health(self) -> dict[str, Any]:
        r = self._client.get("/health")
        self._raise_for_status(r)
        return r.json()  # type: ignore[no-any-return]

    def settings(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        params: dict[str, str] = {}
        if session_id is not None:
            params["session_id"] = session_id
        if model_id is not None:
            params["model_id"] = model_id
        r = self._client.get("/v1/settings", params=params or None)
        self._raise_for_status(r)
        return r.json()  # type: ignore[no-any-return]

    def create_session(self, *, metadata: dict[str, str] | None = None) -> SessionInfo:
        r = self._client.post("/v1/sessions", json={"metadata": metadata or {}})
        self._raise_for_status(r)
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
        self._raise_for_status(r)
        return ModelLoadResult.model_validate(r.json())

    def close_session(self, session_id: str) -> None:
        r = self._client.delete(f"/v1/sessions/{session_id}")
        # 204 on success, 404 if already gone — both are fine for cleanup.
        if r.status_code not in (204, 404):
            self._raise_for_status(r)

    def list_measures(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> list[MeasureSummary]:
        if session_id is not None and model_id is not None:
            path = f"/v1/sessions/{session_id}/models/{model_id}/measures"
        else:
            # Single-model / auto-resolve mode.
            path = "/v1/measures"
        r = self._client.get(path)
        self._raise_for_status(r)
        data = r.json()
        return [MeasureSummary.model_validate(m) for m in data]

    def compile(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
    ) -> CompileResult:
        # Shortcut /v1/query/sql expects the query body fields at the top
        # level and `dialect` as a query parameter.
        r = self._client.post("/v1/query/sql", json=dict(query), params={"dialect": dialect})
        self._raise_for_status(r)
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
        params: dict[str, str] = {"format_values": "true" if format_values else "false"}
        if locale is not None:
            params["locale"] = locale
        if timezone is not None:
            params["timezone"] = timezone
        path, body = self._execute_target(
            query, dialect=dialect, model_id=model_id, session_id=session_id
        )
        if session_id is None:
            params["dialect"] = dialect
        r = self._client.post(path, json=body, params=params)
        self._raise_for_status(r)
        return ExecuteResult.model_validate(r.json())

    def execute_arrow(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
        session_id: str | None = None,
        timezone: str | None = None,
    ) -> ArrowResult:
        """Execute a query asking OBSL for the Arrow result frame.

        ``?format=arrow`` makes OBSL answer with
        ``application/vnd.orionbelt.result+arrow``::

            [u32 big-endian json_len][JSON envelope utf-8][gzip'd Arrow IPC]

        The Arrow sub-part holds typed, locale-neutral rows straight from the
        server — real ``decimal128`` / ``timestamp`` / ``int64``, no
        client-side inference over JSON scalars — which is exactly what the
        Parquet and Arrow exports want. ``format_values`` is deliberately
        absent: this transport is raw by definition.

        Servers that don't know the parameter simply answer in JSON; that's
        detected by content type and returned as an :class:`ArrowResult` with
        ``table=None``, rows in ``meta``, and a warning. No second request.
        """
        params: dict[str, str] = {"format": "arrow", "format_values": "false"}
        if timezone is not None:
            params["timezone"] = timezone
        path, body = self._execute_target(
            query, dialect=dialect, model_id=model_id, session_id=session_id
        )
        if session_id is None:
            params["dialect"] = dialect
        r = self._client.post(
            path, json=body, params=params, headers={"Accept": ARROW_RESULT_MEDIA_TYPE}
        )
        self._raise_for_status(r)

        content_type = r.headers.get("content-type", "")
        if ARROW_RESULT_MEDIA_TYPE not in content_type:
            log.warning(
                "obsl_arrow_unsupported",
                content_type=content_type,
                hint=(
                    "server ignored ?format=arrow — falling back to JSON rows with "
                    "client-side type inference. Exact DECIMAL types need OBSL >= 2.21."
                ),
            )
            return ArrowResult(meta=ExecuteResult.model_validate(r.json()))
        return _decode_arrow_frame(r.content)

    def _execute_target(
        self,
        query: dict[str, Any],
        *,
        dialect: str,
        model_id: str | None,
        session_id: str | None,
    ) -> tuple[str, dict[str, Any]]:
        """Resolve (path, body) for an execute call, session or shortcut."""
        if session_id is not None:
            # Session endpoint: wrapped {model_id, query, dialect} body.
            if model_id is None:
                raise ValueError("model_id is required when session_id is set")
            return (
                f"/v1/sessions/{session_id}/query/execute",
                {"model_id": model_id, "query": query, "dialect": dialect},
            )
        # Shortcut endpoint: query body fields spread at top level, dialect as
        # a query parameter (added by the caller).
        return "/v1/query/execute", dict(query)

    # -- helpers -----------------------------------------------------------

    def _raise_for_status(self, r: httpx.Response) -> None:
        if r.is_success:
            return
        # 401/403 mean OBSL auth (AUTH_MODE=api_key) rejected us. Translate
        # into a dedicated error with a concrete next step — these are the
        # failures an operator hits first when pointing the runner at a
        # secured OBSL >= 2.12 deployment.
        if r.status_code in (401, 403):
            raise self._auth_error(r)
        # 422 from OBSL >= 2.16 means the request body failed JSON-Schema
        # validation at the ingestion boundary (a malformed OBML model loaded
        # by the runner, or a query body in the spec). Translate the structured
        # per-field errors into a readable message.
        if r.status_code == 422:
            raise self._schema_error(r)
        # OBSL returns structured error detail (FastAPI default). Surface it
        # in the exception message so failures are diagnosable from logs.
        body = r.text
        if len(body) > 500:
            body = body[:500] + "…"
        raise httpx.HTTPStatusError(
            f"{r.status_code} {r.reason_phrase} from {r.request.method} {r.request.url}: {body}",
            request=r.request,
            response=r,
        )

    def _schema_error(self, r: httpx.Response) -> ObslSchemaError:
        """Build an :class:`ObslSchemaError` from a 422 JSON-Schema rejection.

        OBSL >= 2.16 returns ``{detail: {message, errors: [{code, message,
        path}]}}``. FastAPI's own request validation uses a different shape
        (``detail`` is a list); both are handled, falling back to the raw body.
        """
        message: str | None = None
        items: list[str] = []
        try:
            detail = r.json().get("detail")
        except (ValueError, AttributeError):
            detail = None

        if isinstance(detail, dict):
            message = detail.get("message")
            errors = detail.get("errors")
            if isinstance(errors, list):
                for e in errors:
                    if not isinstance(e, dict):
                        continue
                    path = e.get("path") or "(root)"
                    items.append(f"{path}: {e.get('message') or e.get('code') or 'invalid'}")
        elif isinstance(detail, list):
            # FastAPI request-validation shape: [{loc, msg, ...}, ...].
            for e in detail:
                if not isinstance(e, dict):
                    continue
                loc = ".".join(str(p) for p in e.get("loc", []) if p != "body") or "(root)"
                items.append(f"{loc}: {e.get('msg') or 'invalid'}")
        elif isinstance(detail, str):
            message = detail

        headline = message or "request body failed JSON-Schema validation"
        body = "; ".join(items) if items else (r.text[:300] if r.text else "no detail")
        return ObslSchemaError(
            f"{r.status_code} {r.reason_phrase} from {r.request.method} "
            f"{r.request.url}: OBSL rejected the request ({headline}: {body}). "
            "Check the OBML model / query bodies in the spec against the OBSL "
            "JSON Schema — OBSL >= 2.16 enforces it (camelCase keys, no string "
            "version, lowercase enum values).",
            request=r.request,
            response=r,
        )

    def _auth_error(self, r: httpx.Response) -> ObslAuthError:
        """Build an :class:`ObslAuthError` from a 401/403 response."""
        code: str | None = None
        message: str | None = None
        try:
            detail = r.json().get("detail")
        except (ValueError, AttributeError):
            detail = None
        if isinstance(detail, dict):
            code = detail.get("code")
            message = detail.get("message")
        elif isinstance(detail, str):
            message = detail

        if not self._has_key:
            hint = (
                "No API key was sent. OBSL requires authentication — set "
                "obsl.api_key in the spec or OBSL_API_KEY in the environment."
            )
        else:
            hint = (
                f"The API key sent in the {self._api_key_header!r} header was "
                "rejected. Check it matches one of the server's API_KEYS."
            )
        server = message or (r.text[:200] if r.text else "no detail")
        label = code or ("auth required" if r.status_code == 401 else "forbidden")
        return ObslAuthError(
            f"{r.status_code} {r.reason_phrase} from {r.request.method} "
            f"{r.request.url}: OBSL rejected the request ({label}: {server}). {hint}",
            request=r.request,
            response=r,
        )
