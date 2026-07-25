"""Runner-level tests for `exports:` targets and export-only (`no_report`) runs."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq
import pytest
from pydantic import ValidationError

from orionbelt_runner.client import ExecuteResult, ObslClient
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


class DualFakeClient(FakeObslClient):
    """Returns formatted or raw rows depending on ``format_values``.

    Mirrors OBSL: the same query yields display strings with
    ``format_values=true`` and native values with ``format_values=false``.
    """

    def __init__(self, *, raw_error: Exception | None = None) -> None:
        super().__init__({"default": FORMATTED})
        self._raw_error = raw_error

    def execute(self, query: dict[str, Any], **kwargs: Any) -> ExecuteResult:
        super().execute(query, **kwargs)
        if kwargs.get("format_values", True):
            return FORMATTED
        if self._raw_error is not None:
            raise self._raw_error
        return RAW


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


def test_parquet_target_writes_typed_files_from_a_raw_re_execute(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="parquet", uri=str(tmp_path / "out"))])

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.succeeded
    assert result.export_locations == [str(tmp_path / "out")]
    written = tmp_path / "out" / "by_country.parquet"
    assert written.exists()

    table = pq.read_table(written)
    # Native types, not the report's locale-formatted strings.
    assert table.schema.field("Revenue").type == pa.float64()
    assert table.schema.field("As Of").type == pa.timestamp("us")
    assert table.column("Revenue").to_pylist() == [5000.5, 7345.25]

    # One formatted execute (report) + one raw execute (export) per query.
    assert [c["format_values"] for c in fake.calls] == [True, False]


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

    assert result.succeeded
    assert result.report_path is not None and result.report_path.exists()
    assert not (tmp_path / "out" / "by_country.parquet").exists()
    assert result.export_locations == []


def test_unreachable_target_does_not_discard_the_report(tmp_path: Path) -> None:
    fake = DualFakeClient()
    spec = _spec(tmp_path, exports=[ExportTarget(format="tsv", uri="gs://not-supported/prefix")])

    result = Runner(_as_protocol(fake)).run(spec)

    assert result.succeeded
    assert result.report_path is not None and result.report_path.exists()
    assert result.export_locations == []


def test_exports_are_skipped_when_a_query_failed(tmp_path: Path) -> None:
    class FailingClient(DualFakeClient):
        def execute(self, query: dict[str, Any], **kwargs: Any) -> ExecuteResult:
            raise RuntimeError("nope")

    spec = _spec(tmp_path, exports=[ExportTarget(format="tsv", uri=str(tmp_path / "out"))])
    result = Runner(_as_protocol(FailingClient())).run(spec)

    assert not result.succeeded
    assert not (tmp_path / "out").exists()


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
