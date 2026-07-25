"""Tests for the Arrow result transport (`?format=arrow`) and warning parsing."""

from __future__ import annotations

import gzip
import json
from collections.abc import Callable
from decimal import Decimal
from typing import Any

import httpx
import pyarrow as pa

from orionbelt_runner.client import (
    ARROW_RESULT_MEDIA_TYPE,
    ExecuteResult,
    HttpObslClient,
    ModelLoadResult,
    _decode_arrow_frame,
)


def _wire(client: HttpObslClient, handler: Callable[[httpx.Request], httpx.Response]) -> None:
    """Swap the client's transport for a mock, preserving its configured headers."""
    inner = client._client
    client._client = httpx.Client(
        base_url=inner.base_url,
        headers=inner.headers,
        transport=httpx.MockTransport(handler),
    )


def _table() -> pa.Table:
    """A table shaped like OBSL's typed output, decimals included."""
    return pa.table(
        {
            "Nation": pa.array(["GERMANY", "UNITED STATES"]),
            "Revenue": pa.array([Decimal("5000.50"), Decimal("7345.25")], pa.decimal128(18, 2)),
            "As Of": pa.array([0, 86_400_000_000], pa.timestamp("us")),
        }
    )


def _frame(table: pa.Table, envelope: dict[str, Any] | None = None) -> bytes:
    """Build OBSL's [u32 len][json][gzip'd arrow ipc] result frame."""
    meta = envelope or {
        "sql": "SELECT nation, revenue FROM …",
        "dialect": "postgres",
        "columns": [
            {"name": "Nation", "type": "string"},
            {"name": "Revenue", "type": "decimal(18, 2)", "format": "#,##0.00"},
            {"name": "As Of", "type": "datetime"},
        ],
        "row_count": table.num_rows,
        "execution_time_ms": 12.5,
        "timezone": "Europe/Berlin",
        "cached": False,
        "physical_tables": ["TPCH.PUBLIC.ORDERS"],
    }
    sink = pa.BufferOutputStream()
    with pa.ipc.new_stream(sink, table.schema) as writer:
        writer.write_table(table)
    meta_bytes = json.dumps(meta).encode("utf-8")
    return (
        len(meta_bytes).to_bytes(4, "big")
        + meta_bytes
        + gzip.compress(sink.getvalue().to_pybytes())
    )


# -- frame decoding --------------------------------------------------------


def test_decode_arrow_frame_returns_typed_table_and_envelope() -> None:
    result = _decode_arrow_frame(_frame(_table()))

    assert result.from_arrow_transport
    assert result.table is not None
    # Server-side types survive verbatim — no JSON round trip, no inference.
    assert result.table.schema.field("Revenue").type == pa.decimal128(18, 2)
    assert result.table.column("Revenue").to_pylist() == [Decimal("5000.50"), Decimal("7345.25")]
    # The envelope is a full ExecuteResult minus rows.
    assert result.meta.sql.startswith("SELECT")
    assert result.meta.row_count == 2
    assert result.meta.timezone == "Europe/Berlin"
    assert result.meta.rows == []


def test_decode_arrow_frame_rejects_a_truncated_frame() -> None:
    body = _frame(_table())
    for broken, why in ((body[:3], "too short"), (body[:20], "envelope cut off")):
        try:
            _decode_arrow_frame(broken)
        except ValueError:
            continue
        except Exception as exc:  # pragma: no cover - only on a regression
            raise AssertionError(f"{why}: wrong error {type(exc).__name__}") from exc
        raise AssertionError(f"{why}: no error raised")


# -- transport negotiation -------------------------------------------------


def test_execute_arrow_requests_the_frame_and_decodes_it() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["url"] = str(request.url)
        seen["accept"] = request.headers.get("accept")
        return httpx.Response(
            200,
            content=_frame(_table()),
            headers={"content-type": ARROW_RESULT_MEDIA_TYPE},
        )

    client = HttpObslClient("http://obsl")
    _wire(client, handler)
    result = client.execute_arrow({"select": {"dimensions": ["Nation"]}}, timezone="Europe/Berlin")

    assert "format=arrow" in seen["url"]
    assert "format_values=false" in seen["url"]  # the transport is raw by definition
    assert "timezone=Europe%2FBerlin" in seen["url"]
    assert seen["accept"] == ARROW_RESULT_MEDIA_TYPE
    assert result.table is not None
    assert result.table.num_rows == 2


def test_execute_arrow_falls_back_when_the_server_answers_json() -> None:
    """An older / unaware deployment ignores ?format=arrow — one request, no crash."""
    calls = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal calls
        calls += 1
        return httpx.Response(
            200,
            json={
                "sql": "SELECT 1",
                "dialect": "postgres",
                "columns": [{"name": "Revenue", "type": "decimal(18, 2)"}],
                "rows": [["5000.50"]],
                "row_count": 1,
            },
        )

    client = HttpObslClient("http://obsl")
    _wire(client, handler)
    result = client.execute_arrow({"select": {"measures": ["Revenue"]}})

    assert calls == 1  # no retry against the JSON endpoint
    assert not result.from_arrow_transport
    assert result.table is None
    assert result.meta.rows == [["5000.50"]]


def test_execute_arrow_uses_the_session_endpoint_when_given_a_session() -> None:
    seen: dict[str, Any] = {}

    def handler(request: httpx.Request) -> httpx.Response:
        seen["path"] = request.url.path
        seen["body"] = json.loads(request.content)
        return httpx.Response(
            200, content=_frame(_table()), headers={"content-type": ARROW_RESULT_MEDIA_TYPE}
        )

    client = HttpObslClient("http://obsl")
    _wire(client, handler)
    client.execute_arrow({"select": {}}, session_id="s1", model_id="m1", dialect="duckdb")

    assert seen["path"] == "/v1/sessions/s1/query/execute"
    assert seen["body"] == {"model_id": "m1", "query": {"select": {}}, "dialect": "duckdb"}


# -- structured warnings ---------------------------------------------------


def test_structured_warnings_are_flattened_not_rejected() -> None:
    """OBSL sends {code, message, hint, …}; the runner logs warnings as lines."""
    result = ExecuteResult.model_validate(
        {
            "sql": "SELECT 1",
            "dialect": "postgres",
            "columns": [{"name": "a"}],
            "rows": [[1]],
            "row_count": 1,
            "warnings": [
                {
                    "code": "deprecated_measure",
                    "severity": "warning",
                    "message": "Measure X is deprecated",
                    "hint": "use Y",
                },
                {"code": "no_hint", "message": "plain"},
                "already a string",
            ],
        }
    )

    assert result.warnings == [
        "deprecated_measure: Measure X is deprecated (hint: use Y)",
        "no_hint: plain",
        "already a string",
    ]


def test_model_load_warnings_are_flattened_too() -> None:
    loaded = ModelLoadResult.model_validate(
        {"model_id": "m1", "warnings": [{"code": "unused_join", "message": "Join J is unused"}]}
    )
    assert loaded.warnings == ["unused_join: Join J is unused"]
