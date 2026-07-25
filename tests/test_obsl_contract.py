"""Contract tests: the runner against 2.23-shaped OBSL responses.

These cover the failure mode that hand-written fixtures can't — the runner's
models disagreeing with what OBSL actually sends. Each test here would have
failed before 0.8.1 on payloads that unit tests happily accepted.
"""

from __future__ import annotations

import re
from decimal import Decimal
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from typer.testing import CliRunner

from orionbelt_runner.cli import app
from orionbelt_runner.client import HttpObslClient
from orionbelt_runner.exports import to_arrow_table
from orionbelt_runner.runner import Runner
from orionbelt_runner.spec import ExportTarget, ObslSpec, QuerySpec, ReportSpec, RunSpec
from tests.obsl_stub import EXPLAIN, StubObsl

OBSL_CLONE = Path(__file__).resolve().parents[2] / "orionbelt-semantic-layer"


# -- the responses the runner must survive ---------------------------------


def test_execute_parses_a_full_response(stub_client: HttpObslClient) -> None:
    """Structured warnings, explain plan, and unmodelled fields all together."""
    result = stub_client.execute({"select": {"measures": ["Revenue"]}})

    assert result.row_count == 2
    assert result.timezone == "Europe/Berlin"
    # Structured warnings flatten instead of failing the whole query.
    assert result.warnings == [
        "sampled_result: Result was sampled to the configured row limit "
        "(hint: raise LIMIT or narrow the query)"
    ]
    assert result.explain is not None
    assert result.explain.planner == "cfl"
    assert result.explain.joins[0].to_object == "Nations"
    assert result.explain.cfl_legs[0].measures == ["Revenue"]
    assert result.resolved.fact_tables == ["LineItems"]
    # The default call is formatted, so cells are locale display strings…
    assert result.columns[1].type == "decimal(18, 2)"
    assert result.rows[0][1] == "5.000,50"
    # …while a raw call carries DECIMAL as an exact string, per OBSL issue #136.
    raw = stub_client.execute({"select": {"measures": ["Revenue"]}}, format_values=False)
    assert raw.rows[0][1] == "5000.50"


def test_model_load_parses_structured_warnings(stub_client: HttpObslClient) -> None:
    loaded = stub_client.load_model("sess-1", model_yaml="version: 1")
    assert loaded.model_id == "model-1"
    assert loaded.warnings == ["unused_join: Join 'Suppliers' is unused"]


def test_preflight_accepts_the_supported_line(stub_client: HttpObslClient) -> None:
    assert stub_client.check_compatibility()["version"] == "2.23.1"


def test_execute_arrow_gets_the_servers_own_types(stub_client: HttpObslClient) -> None:
    arrow = stub_client.execute_arrow({"select": {"measures": ["Revenue"]}})

    assert arrow.from_arrow_transport
    table = to_arrow_table(arrow)
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)
    assert table.column("Revenue").to_pylist() == [Decimal("5000.50"), Decimal("7345.25")]
    # The envelope still carries everything but the rows.
    assert arrow.meta.explain is not None
    assert arrow.meta.row_count == 2
    assert arrow.meta.rows == []


def test_arrow_fallback_still_types_decimals(obsl_stub: StubObsl) -> None:
    """A server that ignores ?format=arrow: one request, inference, still typed."""
    obsl_stub.arrow_transport = False
    client = HttpObslClient("http://obsl.test")
    client._client.close()
    import httpx

    client._client = httpx.Client(base_url="http://obsl.test", transport=obsl_stub.transport())

    arrow = client.execute_arrow({"select": {"measures": ["Revenue"]}})

    assert not arrow.from_arrow_transport
    assert len(obsl_stub.requests) == 1  # no retry against the JSON endpoint
    table = to_arrow_table(arrow)
    # Inference recovers the decimal from OBSL's column hint, not from the value.
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)
    assert table.column("Revenue").to_pylist() == [Decimal("5000.50"), Decimal("7345.25")]


# -- whole runs against the stub -------------------------------------------


def _spec(tmp_path: Path, base_url: str, **kwargs: object) -> RunSpec:
    return RunSpec(
        name="Contract",
        obsl=ObslSpec(base_url=base_url, locale="de", timezone="Europe/Berlin"),
        queries=[
            QuerySpec(
                name="revenue_by_nation",
                # Matches the stub's canned columns, so the section renders a table.
                query={"select": {"dimensions": ["Nation"], "measures": ["Revenue"]}},
            )
        ],
        report=ReportSpec(output=str(tmp_path / "report-{date}.md"), title="Contract — {date}"),
        **kwargs,  # type: ignore[arg-type]
    )


def test_run_against_the_stub_writes_report_and_typed_parquet(
    tmp_path: Path, stub_client: HttpObslClient, obsl_stub: StubObsl
) -> None:
    spec = _spec(
        tmp_path,
        "http://obsl.test",
        exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))],
    )

    result = Runner(stub_client).run(spec)

    assert result.fully_delivered, result.export_errors
    assert result.report_path is not None
    # The report renders OBSL's formatted cells…
    assert "5.000,50" in result.report_path.read_text(encoding="utf-8")
    # …while Parquet carries the server's exact decimal.
    table = pq.read_table(tmp_path / "out" / "revenue_by_nation.parquet")
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)
    assert table.column("Revenue").to_pylist() == [Decimal("5000.50"), Decimal("7345.25")]
    # The warning that used to fail the query reaches the run log.
    assert result.runlog_path is not None
    assert "sampled_result" in result.runlog_path.read_text(encoding="utf-8")


def test_run_survives_a_server_without_the_arrow_transport(
    tmp_path: Path, stub_client: HttpObslClient, obsl_stub: StubObsl
) -> None:
    obsl_stub.arrow_transport = False
    spec = _spec(
        tmp_path,
        "http://obsl.test",
        exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))],
    )

    result = Runner(stub_client).run(spec)

    assert result.fully_delivered, result.export_errors
    table = pq.read_table(tmp_path / "out" / "revenue_by_nation.parquet")
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)


def test_cli_runs_end_to_end_against_a_real_socket(
    tmp_path: Path, stub_server: tuple[str, StubObsl]
) -> None:
    """The whole binary: preflight, session, query, report, export, run log."""
    base_url, stub = stub_server
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(
        f"""
name: Contract CLI
obsl:
  base_url: {base_url}
  locale: de
queries:
  - name: revenue_by_nation
    query:
      select:
        dimensions: [Nation]
        measures: [Revenue]
exports:
  - format: parquet
    uri: out/
report:
  output: report.md
  title: Contract CLI
""",
        encoding="utf-8",
    )

    result = CliRunner().invoke(app, ["run", str(spec_path), "--output-dir", str(tmp_path)])

    assert result.exit_code == 0, result.output
    assert "Report written" in result.output
    assert (tmp_path / "report.md").exists()
    table = pq.read_table(tmp_path / "out" / "revenue_by_nation.parquet")
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)
    # Preflight really ran against the socket.
    assert any(r.url.path.startswith("/health") for r in stub.requests)


# -- drift detection against a sibling OBSL checkout -----------------------


def _obsl_wire_fields(class_name: str) -> set[str]:
    """Names an OBSL response model accepts *on the wire*, read from its source.

    Both the declared field names and any pydantic ``alias`` — OBSL sends e.g.
    ``dataType`` for a field declared ``data_type``, and the wire name is what
    the stub has to match.

    Parsed rather than imported: OBSL isn't a dependency of this repo, and the
    clone only exists on a developer machine.
    """
    source = (OBSL_CLONE / "src/orionbelt/api/schemas.py").read_text(encoding="utf-8")
    body = re.search(rf"^class {class_name}\(BaseModel\):\n(.*?)(?=^class )", source, re.S | re.M)
    assert body is not None, f"{class_name} not found in the OBSL clone"
    declared = set(re.findall(r"^    (\w+):", body.group(1), re.M))
    aliases = set(re.findall(r'alias="([^"]+)"', body.group(1)))
    return declared | aliases


@pytest.mark.skipif(
    not (OBSL_CLONE / "src/orionbelt/api/schemas.py").exists(),
    reason="OBSL checkout not present next to this repo",
)
@pytest.mark.parametrize(
    ("stub_payload", "obsl_model"),
    [
        ("execute", "QueryExecuteResponse"),
        ("column", "ColumnMetadata"),
        ("warning", "StructuredWarning"),
        ("explain", "ExplainPlanResponse"),
        ("measure", "MeasureDetail"),
    ],
)
def test_stub_payloads_match_the_obsl_schemas(stub_payload: str, obsl_model: str) -> None:
    """The stub must keep sending what OBSL sends.

    Skipped in CI (no OBSL checkout), but on a dev machine it catches the exact
    drift that shipped 0.8.0: a field whose shape changed server-side while the
    runner's fixtures kept the old one. A stub field OBSL doesn't declare is the
    error; OBSL growing new fields is fine, since the runner ignores extras.
    """
    stub = StubObsl()
    sent: set[str] = {
        "execute": set(stub.execute_json(format_values=False)),
        "column": set(stub.columns[0]),
        "warning": set(stub.warnings[0]),
        "explain": set(EXPLAIN),
        "measure": set(stub.measures()[0]),
    }[stub_payload]

    declared = _obsl_wire_fields(obsl_model)
    unknown = sent - declared
    assert not unknown, (
        f"stub sends {sorted(unknown)} which OBSL's {obsl_model} doesn't declare — "
        "the stub has drifted from the server contract"
    )
