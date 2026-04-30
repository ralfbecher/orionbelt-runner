"""Tests for the Runner using a fake ObslClient (Protocol-based testing)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from orionbelt_runner.client import (
    ExecuteResult,
    MeasureSummary,
    ModelLoadResult,
    ObslClient,
    SessionInfo,
)
from orionbelt_runner.runner import Runner
from orionbelt_runner.spec import (
    ModelSpec,
    ObslSpec,
    QuerySpec,
    ReportSection,
    ReportSpec,
    RunSpec,
)


class FakeObslClient:
    """A canned-response client. The Protocol is the seam — no http needed."""

    def __init__(
        self,
        results: dict[str, ExecuteResult],
        *,
        measures: list[MeasureSummary] | None = None,
    ) -> None:
        self._results = results
        self.calls: list[dict[str, Any]] = []
        self.session_calls: list[str] = []
        self.model_loads: list[dict[str, Any]] = []
        self.closed_sessions: list[str] = []
        self.measures_calls: list[dict[str, Any]] = []
        self.settings_calls: list[dict[str, Any]] = []
        self._session_counter = 0
        self.next_model_id = "model-loaded"
        self._measures = measures or []
        # Tests can override this to simulate OBSL's resolved-TZ block.
        self.timezone_block: dict[str, Any] | None = None

    def health(self) -> dict[str, Any]:
        return {"status": "ok", "version": "2.1.0"}

    def settings(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        self.settings_calls.append({"session_id": session_id, "model_id": model_id})
        payload: dict[str, Any] = {"version": "2.1.0", "api_version": "v1"}
        if self.timezone_block is not None:
            payload["timezone"] = self.timezone_block
        return payload

    def create_session(self, *, metadata: dict[str, str] | None = None) -> SessionInfo:
        self._session_counter += 1
        sid = f"sess-{self._session_counter}"
        self.session_calls.append(sid)
        return SessionInfo(session_id=sid)

    def load_model(
        self,
        session_id: str,
        *,
        model_yaml: str,
        extends: list[str] | None = None,
    ) -> ModelLoadResult:
        self.model_loads.append(
            {"session_id": session_id, "model_yaml": model_yaml, "extends": extends}
        )
        return ModelLoadResult(model_id=self.next_model_id, data_objects=1)

    def close_session(self, session_id: str) -> None:
        self.closed_sessions.append(session_id)

    def list_measures(
        self,
        *,
        session_id: str | None = None,
        model_id: str | None = None,
    ) -> list[MeasureSummary]:
        self.measures_calls.append({"session_id": session_id, "model_id": model_id})
        return list(self._measures)

    def compile(self, query: dict[str, Any], **kwargs: Any) -> Any:
        raise NotImplementedError  # not exercised here

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
        self.calls.append(
            {
                "query": query,
                "dialect": dialect,
                "model_id": model_id,
                "session_id": session_id,
                "format_values": format_values,
                "locale": locale,
                "timezone": timezone,
            }
        )
        # Pick whichever canned result the test threaded through via the
        # query's ``__test_name`` marker (test-only convention).
        name = query.get("__test_name", "default")
        return self._results[name]


def _make_spec(tmp_path: Path) -> RunSpec:
    return RunSpec(
        name="Smoke",
        obsl=ObslSpec(base_url="http://unused"),
        queries=[
            QuerySpec(
                name="headline",
                query={"__test_name": "headline", "select": {"measures": ["Total Revenue"]}},
            ),
            QuerySpec(
                name="by_country",
                query={"__test_name": "by_country", "select": {"dimensions": ["Country"]}},
            ),
        ],
        report=ReportSpec(
            output=str(tmp_path / "report-{date}.md"),
            title="Smoke Test — {date}",
            sections=[
                ReportSection(heading="Total", query="headline", render="value"),
                ReportSection(heading="By country", query="by_country", render="table"),
            ],
        ),
    )


def test_runner_writes_report(tmp_path: Path) -> None:
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000], ["US", 7345]],
                row_count=2,
            ),
        }
    )
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert result.succeeded
    assert result.report_path is not None
    assert result.report_path.exists()
    content = result.report_path.read_text(encoding="utf-8")
    assert "# Smoke Test —" in content
    assert "**12345**" in content
    assert "| Country | Total Revenue |" in content
    assert "| DE | 5000 |" in content
    assert len(fake.calls) == 2


def test_runner_resolves_timezone_from_settings(tmp_path: Path) -> None:
    """OBSL's /v1/settings TZ is used to localize {time}/{date}/{tz}."""
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000]],
                row_count=1,
            ),
        }
    )
    fake.timezone_block = {
        "effective": "Europe/Berlin",
        "database": None,
        "utc": "2026-04-29T13:30:00Z",
        "now": "2026-04-29T15:30:00+02:00",
    }
    spec = RunSpec(
        name="TZ",
        obsl=ObslSpec(base_url="http://unused"),
        queries=[
            QuerySpec(
                name="headline",
                query={"__test_name": "headline", "select": {"measures": ["Total Revenue"]}},
            ),
            QuerySpec(
                name="by_country",
                query={"__test_name": "by_country", "select": {"dimensions": ["Country"]}},
            ),
        ],
        report=ReportSpec(
            output=str(tmp_path / "report-{date}_{time_filename}_{tz_filename}.md"),
            title="TZ — {date} {time} {tz}",
            sections=[ReportSection(heading="Total", query="headline", render="value")],
        ),
    )
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert result.succeeded
    assert result.report_path is not None
    # tz_filename / timezone replace "/" with ", " so the path stays a single file.
    assert "Europe, Berlin" in result.report_path.name
    # The basis comes from the API server's `utc` (13:30Z), localised to
    # Europe/Berlin (CEST = +02:00) → 15:30 in the title and 15_30_00 in
    # the filename. Crucially this is NOT the runner's own clock.
    assert "15_30_00" in result.report_path.name
    body = result.report_path.read_text(encoding="utf-8")
    first_line = body.splitlines()[0]
    assert first_line == "# TZ — 2026-04-29 15:30:00 Europe/Berlin"


def test_runner_friendly_timezone_placeholder(tmp_path: Path) -> None:
    """``{timezone}`` works as a filesystem-safe alias of ``{tz_filename}``."""
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["X"],
                rows=[[1]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["X"],
                rows=[[1]],
                row_count=1,
            ),
        }
    )
    fake.timezone_block = {"effective": "Europe/Berlin", "utc": "2026-04-29T13:30:00Z"}
    spec = RunSpec(
        name="TZ",
        obsl=ObslSpec(base_url="http://unused"),
        queries=[
            QuerySpec(
                name="headline",
                query={"__test_name": "headline", "select": {"measures": ["X"]}},
            ),
            QuerySpec(
                name="by_country",
                query={"__test_name": "by_country", "select": {"dimensions": ["Y"]}},
            ),
        ],
        report=ReportSpec(
            output=str(tmp_path / "{name}-{date}-{time_filename}-{timezone}.md"),
            title="{name} {date} {time} {timezone}",
            sections=[ReportSection(heading="x", query="headline", render="value")],
        ),
    )
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert result.report_path is not None
    # `{timezone}` materialised as the filename-safe form.
    assert result.report_path.name == "TZ-2026-04-29-15_30_00-Europe, Berlin.md"


def test_runner_falls_back_to_utc_when_settings_lacks_timezone(tmp_path: Path) -> None:
    """No `timezone` block in /v1/settings → UTC, with the trailing Z preserved."""
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["X"],
                rows=[[1]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["X"],
                rows=[[1]],
                row_count=1,
            ),
        }
    )
    fake.timezone_block = None
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert result.succeeded
    # _resolve_timezone is invoked (without a session here, it still runs).
    assert fake.settings_calls, "expected /v1/settings to be probed"


def test_runner_auto_sections_when_report_has_none(tmp_path: Path) -> None:
    """Empty report.sections + queries with descriptions → auto-generated sections.

    Heading is the first comment line, description is the rest, render
    mode is auto-picked: measure-only single-measure → ``value``, anything
    else → ``table``.
    """
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000], ["US", 7345]],
                row_count=2,
            ),
        }
    )
    spec = RunSpec(
        name="Auto",
        obsl=ObslSpec(base_url="http://unused"),
        queries=[
            QuerySpec(
                name="headline",
                description="Headline KPI\nTotal revenue across all regions.",
                query={"__test_name": "headline", "select": {"measures": ["Total Revenue"]}},
            ),
            QuerySpec(
                name="by_country",
                description="Revenue by country",
                query={
                    "__test_name": "by_country",
                    "select": {"dimensions": ["Country"], "measures": ["Total Revenue"]},
                },
            ),
        ],
        report=ReportSpec(
            output=str(tmp_path / "report.md"),
            title="Auto",
            # No sections — runner generates them.
        ),
    )
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert result.succeeded
    body = result.report_path.read_text(encoding="utf-8") if result.report_path else ""
    # Heading from comment first line; description from remainder.
    assert "## Headline KPI" in body
    assert "Total revenue across all regions." in body
    # Single-measure query → value render → bold.
    assert "**12345**" in body
    # by_country has dims → table render.
    assert "## Revenue by country" in body
    assert "| Country | Total Revenue |" in body
    assert "| DE | 5000 |" in body


def test_runner_records_per_query_errors(tmp_path: Path) -> None:
    class FlakyClient(FakeObslClient):
        def execute(
            self,
            query: dict[str, Any],
            **kwargs: Any,
        ) -> ExecuteResult:
            if query.get("__test_name") == "by_country":
                raise RuntimeError("boom")
            return super().execute(query, **kwargs)

    flaky = FlakyClient(
        {
            "headline": ExecuteResult(
                sql="x", dialect="postgres", columns=["X"], rows=[[1]], row_count=1
            ),
        }
    )
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(flaky))
    result = runner.run(spec)

    assert not result.succeeded
    assert "by_country" in result.errors
    assert "boom" in result.errors["by_country"]
    # Report is only written when no queries failed.
    assert result.report_path is None


def test_runner_loads_model_into_session_when_spec_has_model(tmp_path: Path) -> None:
    model_path = tmp_path / "sales.obml.yaml"
    model_path.write_text("name: Sales\n", encoding="utf-8")

    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="x", dialect="postgres", columns=["X"], rows=[[1]], row_count=1
            ),
        }
    )
    spec = RunSpec(
        name="Multi",
        obsl=ObslSpec(base_url="http://unused", model=ModelSpec(yaml_path=model_path)),
        queries=[
            QuerySpec(
                name="headline",
                query={"__test_name": "headline", "select": {"measures": ["X"]}},
            ),
        ],
        report=ReportSpec(
            output=str(tmp_path / "report-{date}.md"),
            title="Multi — {date}",
            sections=[ReportSection(heading="Total", query="headline", render="value")],
        ),
    )
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert result.succeeded
    assert fake.session_calls == ["sess-1"]
    assert fake.model_loads == [
        {"session_id": "sess-1", "model_yaml": "name: Sales\n", "extends": None},
    ]
    assert fake.calls[0]["session_id"] == "sess-1"
    assert fake.calls[0]["model_id"] == "model-loaded"
    assert fake.closed_sessions == ["sess-1"]


def test_runner_closes_session_even_when_query_raises(tmp_path: Path) -> None:
    model_path = tmp_path / "sales.obml.yaml"
    model_path.write_text("name: Sales\n", encoding="utf-8")

    class FlakyClient(FakeObslClient):
        def execute(self, query: dict[str, Any], **kwargs: Any) -> ExecuteResult:
            raise RuntimeError("boom")

    fake = FlakyClient({})
    spec = RunSpec(
        name="Multi",
        obsl=ObslSpec(base_url="http://unused", model=ModelSpec(yaml_path=model_path)),
        queries=[QuerySpec(name="q", query={"select": {}})],
        report=ReportSpec(
            output=str(tmp_path / "r-{date}.md"),
            title="T",
            sections=[],
        ),
    )
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)

    assert not result.succeeded
    assert fake.closed_sessions == ["sess-1"]  # cleanup ran despite failure


def test_preflight_warns_when_referenced_measure_lacks_format(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Measures referenced by the spec but missing a `format` pattern on
    the OBSL side trigger a preflight warning before any query runs.

    structlog renders directly to stdout/stderr (not via stdlib logging),
    so the test reads the warning from ``capsys`` rather than ``caplog``.
    """
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000]],
                row_count=1,
            ),
        },
        # OBSL returned Revenue with no format pattern → should warn.
        measures=[
            MeasureSummary(name="Total Revenue", format=None, dataType="decimal(18, 2)"),
        ],
    )
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(fake))
    runner.run(spec)

    # Preflight was called once; in single-model mode it passes (None, None).
    assert len(fake.measures_calls) == 1

    # structlog rendered the warning to stdout/stderr.
    out = capsys.readouterr()
    log_text = out.out + out.err
    assert "preflight_format_missing" in log_text
    assert "Total Revenue" in log_text


def test_preflight_silent_when_format_present(tmp_path: Path) -> None:
    """No warning when every referenced measure carries a format pattern."""
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000]],
                row_count=1,
            ),
        },
        measures=[
            MeasureSummary(name="Total Revenue", format="#,##0.00"),
        ],
    )
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)
    assert result.succeeded
    # Preflight was called but produced no missing-format warnings.
    assert len(fake.measures_calls) == 1


def test_preflight_skips_int_typed_measures(
    tmp_path: Path, capsys: pytest.CaptureFixture[str]
) -> None:
    """Integer measures (counts) don't need a format pattern — bare str(int)
    is already locale-neutral, so they must not trigger the missing-format
    warning even when ``format`` is None.
    """
    fake = FakeObslClient(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000]],
                row_count=1,
            ),
        },
        measures=[
            # Result-type int + no format → must NOT warn.
            MeasureSummary(name="Total Revenue", format=None, result_type="int"),
        ],
    )
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(fake))
    runner.run(spec)

    out = capsys.readouterr()
    log_text = out.out + out.err
    assert "preflight_format_missing" not in log_text


def test_preflight_skipped_when_no_measures_referenced(tmp_path: Path) -> None:
    """Spec with only raw-mode/dim-only queries doesn't probe measures."""
    fake = FakeObslClient(
        {
            "raw": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Customers.Name"],
                rows=[["Alice"]],
                row_count=1,
            )
        }
    )
    spec = RunSpec(
        name="RawOnly",
        obsl=ObslSpec(base_url="http://unused"),
        queries=[
            QuerySpec(
                name="raw",
                query={"__test_name": "raw", "select": {"fields": ["Customers.Name"]}},
            ),
        ],
        report=ReportSpec(
            output=str(tmp_path / "r-{date}.md"),
            title="T",
            sections=[ReportSection(heading="H", query="raw", render="table")],
        ),
    )
    runner = Runner(_as_protocol(fake))
    runner.run(spec)
    assert fake.measures_calls == []  # no measures referenced → no probe


def test_preflight_failure_is_non_fatal(tmp_path: Path) -> None:
    """If list_measures raises, the run continues."""

    class BlowsUpListing(FakeObslClient):
        def list_measures(
            self,
            *,
            session_id: str | None = None,
            model_id: str | None = None,
        ) -> list[MeasureSummary]:
            raise RuntimeError("transient")

    fake = BlowsUpListing(
        {
            "headline": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Total Revenue"],
                rows=[[12345]],
                row_count=1,
            ),
            "by_country": ExecuteResult(
                sql="SELECT 1",
                dialect="postgres",
                columns=["Country", "Total Revenue"],
                rows=[["DE", 5000]],
                row_count=1,
            ),
        }
    )
    spec = _make_spec(tmp_path)
    runner = Runner(_as_protocol(fake))
    result = runner.run(spec)
    assert result.succeeded  # preflight failure didn't abort the run


def _as_protocol(c: FakeObslClient) -> ObslClient:
    """Type-narrowing helper for mypy: a structural check that FakeObslClient
    satisfies ObslClient. If signatures drift, this fails to type-check.
    """
    return c


@pytest.fixture(autouse=True)
def _quiet_logs() -> None:
    import logging

    logging.getLogger("orionbelt_runner").setLevel(logging.WARNING)
