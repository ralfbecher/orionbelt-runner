"""Runner-level tests for `exports:` targets and export-only (`no_report`) runs."""

from __future__ import annotations

from decimal import Decimal
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from orionbelt_runner.client import ArrowResult, ExecuteResult, ObslClient
from orionbelt_runner.exports import MissingArrowDependencyError
from orionbelt_runner.runner import Runner
from orionbelt_runner.spec import ExportTarget, ObslSpec, QuerySpec, ReportSpec, RunSpec
from tests.test_runner import FakeObslClient

FORMATTED = ExecuteResult(
    sql="SELECT 1",
    dialect="postgres",
    columns=[
        {"name": "Country", "type": "string"},
        {"name": "Revenue", "type": "number"},
        {"name": "As Of", "type": "datetime"},
    ],
    # What OBSL returns with format_values=true: locale-formatted strings.
    rows=[["DE", "5.000,50 €", "29.04.2026"], ["US", "7.345,25 €", "30.04.2026"]],
    row_count=2,
)

RAW = ExecuteResult(
    sql="SELECT 1",
    dialect="postgres",
    columns=[
        {"name": "Country", "type": "string"},
        {"name": "Revenue", "type": "number"},
        {"name": "As Of", "type": "datetime"},
    ],
    rows=[["DE", 5000.5, "2026-04-29T00:00:00"], ["US", 7345.25, "2026-04-30T00:00:00"]],
    row_count=2,
)


TYPED_TABLE = pa.table(
    {
        "Country": pa.array(["DE", "US"]),
        # What the Arrow transport delivers for a governed DECIMAL measure —
        # the exact type, which no amount of JSON inference can recover.
        "Revenue": pa.array([Decimal("5000.50"), Decimal("7345.25")], pa.decimal128(18, 2)),
        "As Of": pa.array([0, 86_400_000_000], pa.timestamp("us")),
    }
)


class DualFakeClient(FakeObslClient):
    """Mirrors OBSL across both transports.

    ``execute`` yields locale-formatted display strings (what the report
    renders); ``execute_arrow`` yields the typed Arrow table (what Parquet /
    Arrow exports write). ``arrow_transport=False`` simulates a deployment that
    ignores ``?format=arrow`` and answers JSON, exercising the inference
    fallback.
    """

    def __init__(self, *, raw_error: Exception | None = None, arrow_transport: bool = True) -> None:
        super().__init__({"default": FORMATTED})
        self._raw_error = raw_error
        self._arrow_transport = arrow_transport

    def execute(self, query: dict[str, Any], **kwargs: Any) -> ExecuteResult:
        super().execute(query, **kwargs)
        if kwargs.get("format_values", True):
            return FORMATTED
        if self._raw_error is not None:
            raise self._raw_error
        return RAW

    def execute_arrow(self, query: dict[str, Any], **kwargs: Any) -> ArrowResult:
        # Record it the way the JSON path records a raw call, so tests can
        # assert the execute pattern per query in one list.
        self.calls.append({**kwargs, "query": query, "format_values": False, "transport": "arrow"})
        if self._raw_error is not None:
            raise self._raw_error
        if not self._arrow_transport:
            return ArrowResult(meta=RAW)
        return ArrowResult(meta=RAW.model_copy(update={"rows": []}), table=TYPED_TABLE)


def _as_protocol(client: FakeObslClient) -> ObslClient:
    return client  # type: ignore[return-value]


def _spec(
    tmp_path: Path,
    *,
    exports: list[ExportTarget],
    no_report: bool = False,
    report: ReportSpec | None = None,
) -> RunSpec:
    if report is None and not no_report:
        report = ReportSpec(output=str(tmp_path / "report-{date}.md"), title="Smoke")
    return RunSpec(
        name="Smoke",
        obsl=ObslSpec(base_url="http://unused"),
        queries=[QuerySpec(name="by_country", query={"select": {"dimensions": ["Country"]}})],
        report=report,
        no_report=no_report,
        exports=exports,
    )


# -- parquet / arrow targets ----------------------------------------------


def test_parquet_target_writes_the_servers_typed_table(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))])

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.succeeded
    assert result.export_locations == [str(tmp_path / "out")]
    written = tmp_path / "out" / "by_country.parquet"
    assert written.exists()

    table = pq.read_table(written)
    # Straight from the Arrow transport: the server's own types, including the
    # exact DECIMAL that JSON inference could only have guessed as string.
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)
    assert table.schema.field("As Of").type == pa.timestamp("us")
    assert table.column("Revenue").to_pylist() == [Decimal("5000.50"), Decimal("7345.25")]

    # One formatted execute (report) + one Arrow execute (export) per query.
    assert [c["format_values"] for c in fake.calls] == [True, False]
    assert fake.calls[1]["transport"] == "arrow"


def test_parquet_falls_back_to_inference_when_the_server_lacks_arrow(tmp_path: Path) -> None:
    """A deployment that ignores ?format=arrow still gets typed columns."""
    fake = DualFakeClient(arrow_transport=False)
    spec = _spec(tmp_path, exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))])

    Runner(_as_protocol(fake)).run(spec)

    table = pq.read_table(tmp_path / "out" / "by_country.parquet")
    # Inferred from JSON scalars: float rather than decimal, but still typed.
    assert table.schema.field("Revenue").type == pa.float64()
    assert table.schema.field("As Of").type == pa.timestamp("us")


def test_arrow_target_writes_ipc_files(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[ExportTarget(format="arrow", uri=str(tmp_path / "out"), compression="zstd")],
    )

    result = Runner(_as_protocol(fake)).run(spec)

    written = tmp_path / "out" / "by_country.arrow"
    assert result.succeeded
    with pa.ipc.open_file(written) as reader:
        assert reader.read_all().column("Country").to_pylist() == ["DE", "US"]


def test_tsv_target_uses_formatted_values_and_skips_the_raw_execute(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="tsv", uri=str(tmp_path / "out"))])

    Runner(_as_protocol(fake)).run(spec)

    body = (tmp_path / "out" / "by_country.tsv").read_text(encoding="utf-8")
    assert "5.000,50 €" in body  # mirrors the report
    assert [c["format_values"] for c in fake.calls] == [True]


def test_uri_placeholders_are_resolved(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[ExportTarget(format="tsv", uri=str(tmp_path / "{name}" / "{date}"))],
    )

    result = Runner(_as_protocol(fake)).run(spec)

    assert len(result.export_locations) == 1
    location = Path(result.export_locations[0])
    assert location.parent.name == "Smoke"
    assert location.name.count("-") == 2  # YYYY-MM-DD
    assert (location / "by_country.tsv").exists()


def test_relative_export_uri_is_rebased_under_output_dir(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[ExportTarget(format="tsv", uri="data/exports")],
        report=ReportSpec(output="report-{date}.md", title="Smoke"),
    )

    result = Runner(_as_protocol(fake)).run(spec, output_dir=tmp_path)

    assert (tmp_path / "data/exports/by_country.tsv").exists()
    assert result.export_locations == [str(tmp_path / "data/exports")]


def test_multiple_targets_all_written(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[
            ExportTarget(format="parquet", uri=str(tmp_path / "warehouse")),
            ExportTarget(format="tsv", uri=str(tmp_path / "share")),
        ],
    )

    result = Runner(_as_protocol(fake)).run(spec)

    assert (tmp_path / "warehouse/by_country.parquet").exists()
    assert (tmp_path / "share/by_country.tsv").exists()
    assert len(result.export_locations) == 2
    # Only one raw re-execute even though two targets are configured.
    assert [c["format_values"] for c in fake.calls] == [True, False]


# -- failure handling ------------------------------------------------------


def test_failed_raw_execute_skips_the_query_but_not_the_run(tmp_path: Path) -> None:
    """A parquet export must never invent a schema — better a missing file."""
    fake = DualFakeClient(raw_error=RuntimeError("boom"))
    spec = _spec(tmp_path, exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))])

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.succeeded  # queries ran; the report is valid
    assert result.report_path is not None and result.report_path.exists()
    assert not (tmp_path / "out" / "by_country.parquet").exists()
    assert result.export_locations == []
    # …but the run did not deliver what the spec asked for, and says so.
    assert not result.fully_delivered
    assert "by_country" in result.export_errors[0]


def test_unreachable_target_does_not_discard_the_report(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="tsv", uri="gs://not-supported/prefix")])

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.succeeded
    assert result.report_path is not None and result.report_path.exists()
    assert result.export_locations == []
    assert not result.fully_delivered
    assert "gs://not-supported/prefix" in result.export_errors[0]


def test_export_only_run_that_wrote_nothing_is_not_a_clean_run(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    """The Docker-without-pyarrow shape: exit code must not say 'all good'."""
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[ExportTarget(format="parquet", uri="s3://bucket/prefix")],
        no_report=True,
    )

    def explode(*_args: object, **_kwargs: object) -> None:
        raise MissingArrowDependencyError("S3 exports need pyarrow")

    monkeypatch.setattr("orionbelt_runner.runner.open_sink", explode)
    result = Runner(_as_protocol(fake)).run(spec, output_dir=tmp_path)

    assert result.succeeded  # every query ran…
    assert not result.fully_delivered  # …but nothing landed
    assert result.export_locations == []
    assert "need pyarrow" in result.export_errors[0]


def test_successful_run_is_fully_delivered(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))])

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.fully_delivered
    assert result.export_errors == []


def test_exports_are_skipped_when_a_query_failed(tmp_path: Path) -> None:
    class FailingClient(DualFakeClient):
        def execute(self, query: dict[str, Any], **kwargs: Any) -> ExecuteResult:
            raise RuntimeError("nope")

    spec = _spec(tmp_path, exports=[ExportTarget(format="tsv", uri=str(tmp_path / "out"))])
    result = Runner(_as_protocol(FailingClient())).run(spec)

    assert not result.succeeded
    assert not (tmp_path / "out").exists()


# -- how many times each query is executed ---------------------------------


def test_export_only_typed_targets_execute_each_query_once(tmp_path: Path) -> None:
    """Nothing consumes formatted rows here, so don't pay for them."""
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[
            ExportTarget(format="parquet", uri=str(tmp_path / "p")),
            ExportTarget(format="arrow", uri=str(tmp_path / "a")),
        ],
        no_report=True,
    )

    result = Runner(_as_protocol(fake)).run(spec, output_dir=tmp_path)

    assert [c["format_values"] for c in fake.calls] == [False]
    # The Arrow transport is raw by definition and takes no locale at all.
    assert fake.calls[0]["transport"] == "arrow"
    assert "locale" not in fake.calls[0]
    assert result.fully_delivered
    table = pq.read_table(tmp_path / "p" / "by_country.parquet")
    assert table.schema.field("Revenue").type == pa.decimal128(18, 2)


def test_export_only_with_a_tsv_target_still_needs_both_runs(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[
            ExportTarget(format="parquet", uri=str(tmp_path / "p")),
            ExportTarget(format="tsv", uri=str(tmp_path / "t")),
        ],
        no_report=True,
    )

    Runner(_as_protocol(fake)).run(spec, output_dir=tmp_path)

    assert [c["format_values"] for c in fake.calls] == [True, False]
    assert "5.000,50 €" in (tmp_path / "t" / "by_country.tsv").read_text(encoding="utf-8")
    assert pq.read_table(tmp_path / "p" / "by_country.parquet").column("Revenue").to_pylist() == [
        Decimal("5000.50"),
        Decimal("7345.25"),
    ]


def test_report_run_keeps_using_formatted_values(tmp_path: Path) -> None:
    """The report must never silently switch to raw cells."""
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="parquet", uri=str(tmp_path / "p"))])

    result = Runner(_as_protocol(fake)).run(spec)

    assert [c["format_values"] for c in fake.calls] == [True, False]
    assert result.report_path is not None
    assert "5.000,50 €" in result.report_path.read_text(encoding="utf-8")


def test_run_without_exports_executes_once_formatted(tmp_path: Path) -> None:
    fake = DualFakeClient()
    result = Runner(_as_protocol(fake)).run(_spec(tmp_path, exports=[]))

    assert [c["format_values"] for c in fake.calls] == [True]
    assert result.fully_delivered


def test_export_only_without_targets_still_executes_and_logs(tmp_path: Path) -> None:
    """Degenerate spec: no report, no exports. Keep the formatted default."""
    fake = DualFakeClient()
    result = Runner(_as_protocol(fake)).run(
        _spec(tmp_path, exports=[], no_report=True), output_dir=tmp_path
    )

    assert [c["format_values"] for c in fake.calls] == [True]
    assert result.runlog_path is not None and result.runlog_path.exists()


# -- export-only runs ------------------------------------------------------


def test_no_report_skips_rendering_but_writes_exports_and_runlog(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))],
        no_report=True,
    )

    result = Runner(_as_protocol(fake)).run(spec, output_dir=tmp_path)

    assert result.succeeded
    assert result.report_path is None
    assert (tmp_path / "out" / "by_country.parquet").exists()
    # Runlog still lands — the audit trail is the point of an export-only run.
    assert result.runlog_path is not None
    assert result.runlog_path.exists()
    assert result.runlog_path.name.startswith("Smoke-")
    assert result.runlog_path.name.endswith(".run.yaml")
    assert list(tmp_path.glob("*.md")) == []


def test_no_report_with_a_report_block_keeps_the_runlog_location(tmp_path: Path) -> None:
    """Toggling reporting off must not move the runlog out of its folder."""
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[ExportTarget(format="tsv", uri=str(tmp_path / "out"))],
        no_report=True,
        report=ReportSpec(output=str(tmp_path / "reports" / "report-{date}.md"), title="Smoke"),
    )

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.report_path is None
    assert result.runlog_path is not None
    assert result.runlog_path.parent == tmp_path / "reports"
    assert not (tmp_path / "reports" / "report-2026-04-29.md").exists()


def test_no_report_ignores_the_sibling_tsv_shortcut(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(
        tmp_path,
        exports=[],
        no_report=True,
        report=ReportSpec(
            output=str(tmp_path / "report-{date}.md"), title="Smoke", export_results=True
        ),
    )

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.succeeded
    assert result.exports_dir is None


def test_spec_without_report_or_no_report_is_rejected() -> None:
    with pytest.raises(ValidationError, match="no_report"):
        RunSpec(
            name="Smoke",
            queries=[QuerySpec(name="q", query={"select": {"dimensions": ["Country"]}})],
        )
