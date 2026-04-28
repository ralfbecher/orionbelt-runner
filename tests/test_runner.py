"""Tests for the Runner using a fake ObslClient (Protocol-based testing)."""

from __future__ import annotations

from pathlib import Path
from typing import Any

import pytest

from orionbelt_runner.client import (
    ExecuteResult,
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

    def __init__(self, results: dict[str, ExecuteResult]) -> None:
        self._results = results
        self.calls: list[dict[str, Any]] = []
        self.session_calls: list[str] = []
        self.model_loads: list[dict[str, Any]] = []
        self.closed_sessions: list[str] = []
        self._session_counter = 0
        self.next_model_id = "model-loaded"

    def health(self) -> dict[str, Any]:
        return {"status": "ok", "version": "2.1.0"}

    def settings(self) -> dict[str, Any]:
        return {"version": "2.1.0", "api_version": "v1"}

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

    def compile(self, query: dict[str, Any], **kwargs: Any) -> Any:
        raise NotImplementedError  # not exercised here

    def execute(
        self,
        query: dict[str, Any],
        *,
        dialect: str = "postgres",
        model_id: str | None = None,
        session_id: str | None = None,
    ) -> ExecuteResult:
        self.calls.append(
            {"query": query, "dialect": dialect, "model_id": model_id, "session_id": session_id}
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


def test_runner_records_per_query_errors(tmp_path: Path) -> None:
    class FlakyClient(FakeObslClient):
        def execute(
            self,
            query: dict[str, Any],
            *,
            dialect: str = "postgres",
            model_id: str | None = None,
            session_id: str | None = None,
        ) -> ExecuteResult:
            if query.get("__test_name") == "by_country":
                raise RuntimeError("boom")
            return super().execute(query, dialect=dialect, model_id=model_id, session_id=session_id)

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


def _as_protocol(c: FakeObslClient) -> ObslClient:
    """Type-narrowing helper for mypy: a structural check that FakeObslClient
    satisfies ObslClient. If signatures drift, this fails to type-check.
    """
    return c


@pytest.fixture(autouse=True)
def _quiet_logs() -> None:
    import logging

    logging.getLogger("orionbelt_runner").setLevel(logging.WARNING)
