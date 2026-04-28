"""Core runner: spec in, ExecuteResult-per-query + rendered report out."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path

import structlog

from orionbelt_runner.client import ExecuteResult, ObslClient
from orionbelt_runner.report import render_markdown
from orionbelt_runner.spec import RunSpec

log = structlog.get_logger("orionbelt_runner")


@dataclass
class RunResult:
    """Outcome of a single run."""

    spec_name: str
    started_at: datetime
    finished_at: datetime
    results: dict[str, ExecuteResult] = field(default_factory=dict)
    report_path: Path | None = None
    errors: dict[str, str] = field(default_factory=dict)

    @property
    def succeeded(self) -> bool:
        return not self.errors


class Runner:
    """Executes a RunSpec end-to-end: query → render → write."""

    def __init__(self, client: ObslClient) -> None:
        self._client = client

    def run(self, spec: RunSpec, *, output_dir: Path | None = None) -> RunResult:
        started_at = datetime.now(tz=UTC)
        log.info("run_start", spec=spec.name, query_count=len(spec.queries))

        results: dict[str, ExecuteResult] = {}
        errors: dict[str, str] = {}
        for q in spec.queries:
            try:
                results[q.name] = self._client.execute(
                    q.query,
                    dialect=q.dialect,
                    model_id=spec.obsl.model_id,
                )
                log.info("query_done", name=q.name, rows=len(results[q.name].rows))
            except Exception as exc:  # noqa: BLE001 — surface anything the client raises
                msg = f"{type(exc).__name__}: {exc}"
                errors[q.name] = msg
                log.error("query_failed", name=q.name, error=msg)

        finished_at = datetime.now(tz=UTC)

        report_path: Path | None = None
        if results and not errors:
            report_path = self._render_report(spec, results, started_at, output_dir)
            log.info("report_written", path=str(report_path))

        return RunResult(
            spec_name=spec.name,
            started_at=started_at,
            finished_at=finished_at,
            results=results,
            report_path=report_path,
            errors=errors,
        )

    def _render_report(
        self,
        spec: RunSpec,
        results: dict[str, ExecuteResult],
        started_at: datetime,
        output_dir: Path | None,
    ) -> Path:
        ctx = {
            "name": spec.name,
            "date": started_at.strftime("%Y-%m-%d"),
            "datetime": started_at.strftime("%Y-%m-%dT%H-%M-%SZ"),
        }
        body = render_markdown(spec.report, results, context=ctx)
        out_path = Path(spec.report.output.format(**ctx))
        if output_dir is not None and not out_path.is_absolute():
            out_path = output_dir / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(body, encoding="utf-8")
        return out_path
