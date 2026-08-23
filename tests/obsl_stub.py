"""A stub OBSL server that answers with real 2.25-shaped payloads.

Every bug shipped in 0.8.0 was *contract drift*: the runner's models described
OBSL's responses slightly wrong (structured warnings modelled as strings,
``decimal(p, s)`` column types unhandled), and hand-written test fixtures
encoded the same misunderstanding, so nothing failed until a real server
answered. This module exists to make that class of bug testable.

The payloads here mirror OBSL 2.25's response models — ``QueryExecuteResponse``,
``StructuredWarning``, ``ColumnMetadata``, ``ExplainPlanResponse`` in the
server's ``api/schemas.py``. Keep them that way: when they drift, the runner
should fail here rather than in production.
:mod:`tests.test_obsl_contract` cross-checks the field names against a sibling
OBSL checkout when one is present.

Two ways to drive it:

* :meth:`StubObsl.transport` — an ``httpx.MockTransport`` for wiring straight
  into a client. No sockets, no threads; use this by default.
* :func:`serve` — a real HTTP server on a loopback port, for exercising the
  CLI end to end.
"""

from __future__ import annotations

import gzip
import json
import threading
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, field
from decimal import Decimal
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

import httpx
import pyarrow as pa

from orionbelt_runner.client import ARROW_RESULT_MEDIA_TYPE

# What the runner pins to; the stub reports it from /health and /v1/settings.
DEFAULT_OBSL_VERSION = "2.25.1"

# A governed DECIMAL measure alongside a string dimension and a timestamp —
# the shape that regressed in 0.8.0. OBSL reports DECIMAL columns as
# "decimal(p, s)" and sends their cells as exact strings in JSON.
DEFAULT_COLUMNS: list[dict[str, Any]] = [
    {"name": "Nation", "type": "string", "format": None},
    {"name": "Revenue", "type": "decimal(18, 2)", "format": "#,##0.00"},
    {"name": "As Of", "type": "datetime", "format": None},
]
RAW_ROWS: list[list[Any]] = [
    ["GERMANY", "5000.50", "2026-04-29T00:00:00"],
    ["UNITED STATES", "7345.25", "2026-04-30T00:00:00"],
]
FORMATTED_ROWS: list[list[Any]] = [
    ["GERMANY", "5.000,50", "29.04.2026"],
    ["UNITED STATES", "7.345,25", "30.04.2026"],
]

# OBSL's StructuredWarning — the shape that failed every warning-carrying query
# before 0.8.1.
DEFAULT_WARNINGS: list[dict[str, Any]] = [
    {
        "code": "sampled_result",
        "severity": "warning",
        "message": "Result was sampled to the configured row limit",
        "path": "$.limit",
        "hint": "raise LIMIT or narrow the query",
        "context": {"limit": 1000},
    }
]

EXPLAIN: dict[str, Any] = {
    "planner": "cfl",
    "planner_reason": "measures span multiple fact tables",
    "base_object": "Orders",
    "base_object_reason": "lowest common grain",
    "joins": [
        {
            "from_object": "Orders",
            "to_object": "Nations",
            "join_columns": ["nation_key"],
            "reason": "declared relationship",
        }
    ],
    "where_filter_count": 1,
    "having_filter_count": 0,
    "has_totals": False,
    "cfl_legs": [
        {
            "measure_source": "LineItems",
            "common_root": "Orders",
            "reason": "fan-out guard",
            "measures": ["Revenue"],
            "joins": ["Orders→LineItems"],
        }
    ],
}


@dataclass
class StubObsl:
    """Canned OBSL responses, configurable per test.

    ``arrow_transport=False`` simulates a deployment that ignores
    ``?format=arrow`` and answers JSON — the fallback the runner must handle in
    a single request.
    """

    version: str = DEFAULT_OBSL_VERSION
    auth_mode: str = "none"
    arrow_transport: bool = True
    warnings: list[dict[str, Any]] = field(default_factory=lambda: list(DEFAULT_WARNINGS))
    columns: list[dict[str, Any]] = field(default_factory=lambda: list(DEFAULT_COLUMNS))
    raw_rows: list[list[Any]] = field(default_factory=lambda: [list(r) for r in RAW_ROWS])
    formatted_rows: list[list[Any]] = field(
        default_factory=lambda: [list(r) for r in FORMATTED_ROWS]
    )
    timezone: str = "Europe/Berlin"
    # Every request the stub served, for asserting how the runner called it.
    requests: list[httpx.Request] = field(default_factory=list)

    # -- payloads ----------------------------------------------------------

    def health(self) -> dict[str, Any]:
        return {"status": "ok", "version": self.version, "auth_mode": self.auth_mode}

    def settings(self) -> dict[str, Any]:
        return {
            "version": self.version,
            "api_version": "v1",
            "timezone": {
                "effective": self.timezone,
                "database": self.timezone,
                "utc": "2026-04-29T10:00:00Z",
                "now": "2026-04-29T12:00:00+02:00",
            },
        }

    def measures(self) -> list[dict[str, Any]]:
        """``GET /v1/measures`` — a bare array of ``MeasureDetail``, not an object."""
        return [
            {
                "name": "Revenue",
                "result_type": "decimal",
                "aggregation": "sum",
                "expression": None,
                "columns": [{"data_object": "LineItems", "column": "extended_price"}],
                "distinct": False,
                "total": False,
                "description": "Sum of extended price",
                "format": "#,##0.00",
                "dataType": "decimal",
                "owner": "finance",
                "synonyms": ["Sales"],
            }
        ]

    def execute_json(self, *, format_values: bool) -> dict[str, Any]:
        """A ``QueryExecuteResponse``, including fields the runner doesn't model."""
        return {
            "sql": "SELECT n.name, SUM(l.revenue) FROM orders o JOIN …",
            "dialect": "postgres",
            "columns": list(self.columns),
            "rows": self.formatted_rows if format_values else self.raw_rows,
            "row_count": len(self.raw_rows),
            "execution_time_ms": 42.7,
            "timezone": self.timezone,
            "resolved": {
                "fact_tables": ["LineItems"],
                "dimensions": ["Nation"],
                "measures": ["Revenue"],
            },
            "warnings": list(self.warnings),
            "sql_valid": True,
            "explain": EXPLAIN,
            # Server-side fields the runner ignores — they must not break it.
            "physical_tables": ["TPCH.PUBLIC.ORDERS", "TPCH.PUBLIC.NATION"],
            "cached": False,
            "cached_at": None,
            "ttl_seconds": 300,
        }

    def arrow_table(self) -> pa.Table:
        """The typed table OBSL puts in the Arrow sub-part."""
        return pa.table(
            {
                "Nation": pa.array([r[0] for r in self.raw_rows], pa.string()),
                "Revenue": pa.array([Decimal(r[1]) for r in self.raw_rows], pa.decimal128(18, 2)),
                "As Of": pa.array([0, 86_400_000_000][: len(self.raw_rows)], pa.timestamp("us")),
            }
        )

    def execute_arrow_frame(self) -> bytes:
        """``[u32 big-endian json_len][JSON envelope utf-8][gzip'd Arrow IPC]``."""
        envelope = {k: v for k, v in self.execute_json(format_values=False).items() if k != "rows"}
        meta = json.dumps(envelope).encode("utf-8")
        sink = pa.BufferOutputStream()
        with pa.ipc.new_stream(sink, self.arrow_table().schema) as writer:
            writer.write_table(self.arrow_table())
        return len(meta).to_bytes(4, "big") + meta + gzip.compress(sink.getvalue().to_pybytes())

    # -- routing -----------------------------------------------------------

    def handle(self, request: httpx.Request) -> httpx.Response:
        """Route one request. Shared by the mock transport and the real server."""
        self.requests.append(request)
        path, params = request.url.path, request.url.params

        if request.method == "GET":
            if path.startswith("/health"):
                return httpx.Response(200, json=self.health())
            if path.endswith("/settings"):
                return httpx.Response(200, json=self.settings())
            if path.endswith("/measures"):
                return httpx.Response(200, json=self.measures())
            return httpx.Response(404, json={"detail": f"no stub route for GET {path}"})

        if request.method == "DELETE":
            return httpx.Response(200, json={"deleted": True})

        if request.method == "POST":
            if path.endswith("/models"):
                return httpx.Response(
                    200,
                    json={
                        "model_id": "model-1",
                        "data_objects": 3,
                        "dimensions": 2,
                        "measures": 1,
                        "metrics": 0,
                        "warnings": [
                            {"code": "unused_join", "message": "Join 'Suppliers' is unused"}
                        ],
                    },
                )
            if path.startswith("/v1/sessions") and "query" not in path:
                return httpx.Response(
                    200, json={"session_id": "sess-1", "created_at": "2026-04-29"}
                )
            if path.endswith("/query/execute"):
                return self._execute(params)
            if path.endswith("/query/sql"):
                payload = self.execute_json(format_values=False)
                return httpx.Response(
                    200, json={k: payload[k] for k in ("sql", "dialect", "warnings", "sql_valid")}
                )

        return httpx.Response(404, json={"detail": f"no stub route for {request.method} {path}"})

    def _execute(self, params: httpx.QueryParams) -> httpx.Response:
        wants_arrow = params.get("format") == "arrow"
        if wants_arrow and self.arrow_transport:
            return httpx.Response(
                200,
                content=self.execute_arrow_frame(),
                headers={"content-type": ARROW_RESULT_MEDIA_TYPE},
            )
        # Either JSON was asked for, or this deployment doesn't know the
        # parameter and answers JSON regardless — the fallback path.
        return httpx.Response(
            200, json=self.execute_json(format_values=params.get("format_values") == "true")
        )

    def transport(self) -> httpx.MockTransport:
        """An httpx transport serving this stub — no sockets involved."""
        return httpx.MockTransport(self.handle)


@contextmanager
def serve(stub: StubObsl | None = None) -> Iterator[tuple[str, StubObsl]]:
    """Run ``stub`` on a loopback port; yield ``(base_url, stub)``.

    For tests that need a real endpoint — driving the CLI, or proving the HTTP
    layer end to end. Everything else should use :meth:`StubObsl.transport`.
    """
    stub = stub or StubObsl()

    class Handler(BaseHTTPRequestHandler):
        protocol_version = "HTTP/1.1"

        def log_message(self, *args: Any) -> None:  # keep pytest output clean
            pass

        def _dispatch(self, method: str) -> None:
            length = int(self.headers.get("Content-Length") or 0)
            body = self.rfile.read(length) if length else b""
            request = httpx.Request(
                method,
                f"http://127.0.0.1{self.path}",
                content=body,
                headers=dict(self.headers),
            )
            response = stub.handle(request)
            payload = response.content
            self.send_response(response.status_code)
            self.send_header(
                "Content-Type", response.headers.get("content-type", "application/json")
            )
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def do_GET(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._dispatch("GET")

        def do_POST(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._dispatch("POST")

        def do_DELETE(self) -> None:  # noqa: N802 - BaseHTTPRequestHandler API
            self._dispatch("DELETE")

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}", stub
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)
