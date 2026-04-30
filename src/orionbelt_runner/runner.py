"""Core runner: spec in, ExecuteResult-per-query + rendered report out."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import structlog

from orionbelt_runner.client import ExecuteResult, ObslClient
from orionbelt_runner.report import render_markdown
from orionbelt_runner.spec import ModelSpec, QuerySpec, ReportSection, RunSpec

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

        session_id: str | None = None
        model_id: str | None = spec.obsl.model_id
        results: dict[str, ExecuteResult] = {}
        errors: dict[str, str] = {}
        report_basis: datetime = started_at
        tz_name = "UTC"

        try:
            if spec.obsl.model is not None:
                session_id, model_id = self._load_session_model(spec.obsl.model)

            report_basis, tz_name = self._resolve_report_clock(
                session_id=session_id, model_id=model_id, fallback=started_at
            )

            self._preflight_format_patterns(spec.queries, session_id=session_id, model_id=model_id)

            for q in spec.queries:
                try:
                    results[q.name] = self._client.execute(
                        q.query,
                        dialect=q.dialect or "postgres",
                        model_id=model_id,
                        session_id=session_id,
                        format_values=True,
                        locale=spec.obsl.locale,
                        timezone=spec.obsl.timezone,
                    )
                    log.info("query_done", name=q.name, rows=len(results[q.name].rows))
                except Exception as exc:  # noqa: BLE001 — surface anything the client raises
                    msg = f"{type(exc).__name__}: {exc}"
                    errors[q.name] = msg
                    log.error("query_failed", name=q.name, error=msg)
        finally:
            if session_id is not None:
                try:
                    self._client.close_session(session_id)
                    log.info("session_closed", session_id=session_id)
                except Exception as exc:  # noqa: BLE001
                    log.warning("session_close_failed", session_id=session_id, error=str(exc))

        finished_at = datetime.now(tz=UTC)

        report_path: Path | None = None
        if results and not errors:
            report_path = self._render_report(
                spec, results, report_basis, output_dir, tz_name=tz_name
            )
            log.info("report_written", path=str(report_path))

        return RunResult(
            spec_name=spec.name,
            started_at=started_at,
            finished_at=finished_at,
            results=results,
            report_path=report_path,
            errors=errors,
        )

    def _preflight_format_patterns(
        self,
        queries: list[QuerySpec],
        *,
        session_id: str | None,
        model_id: str | None,
    ) -> None:
        """Warn when measures referenced by the spec lack a ``format`` pattern.

        Without ``format`` on the OBSL measure, ``format_values=true`` cannot
        produce locale-aware display strings — the cell falls through to a
        bare ``str(value)``. The runner sends ``format_values=true`` on every
        query, so a missing pattern silently degrades the rendered report.
        Surfacing it here turns a stealth bug ("why is my report ugly?")
        into a visible warning.

        Failure to call ``list_measures`` is non-fatal; the run continues.
        """
        referenced: set[str] = set()
        for q in queries:
            select = q.query.get("select") if isinstance(q.query, dict) else None
            if isinstance(select, dict):
                for name in select.get("measures", []) or []:
                    if isinstance(name, str):
                        referenced.add(name)
        if not referenced:
            return

        try:
            measures = self._client.list_measures(session_id=session_id, model_id=model_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("preflight_list_measures_failed", error=f"{type(exc).__name__}: {exc}")
            return

        by_name = {m.name: m for m in measures}
        missing_format: list[str] = []
        for name in sorted(referenced):
            m = by_name.get(name)
            if m is None:
                # Unknown measures will fail at execute time with a clearer
                # error — don't pile on a warning here.
                continue
            # Integer-typed measures (counts, distinct counts) don't need a
            # format pattern — bare str(int) is already locale-neutral and
            # matches user expectations. Only flag float / decimal measures.
            rt = (m.result_type or m.data_type or "").lower()
            if rt in {"int", "integer", "bigint", "smallint"}:
                continue
            if not m.format:
                missing_format.append(name)

        if missing_format:
            log.warning(
                "preflight_format_missing",
                measures=missing_format,
                hint=(
                    "format_values=true cannot apply locale-aware formatting to these "
                    "measures. Add `format: '#,##0.00'` (or similar) to each measure "
                    "in the OBML model."
                ),
            )

    def _load_session_model(self, model_spec: ModelSpec) -> tuple[str, str]:
        session = self._client.create_session()
        log.info("session_created", session_id=session.session_id)
        yaml_text = model_spec.yaml_path.read_text(encoding="utf-8")
        extends_yaml = [p.read_text(encoding="utf-8") for p in model_spec.extends]
        loaded = self._client.load_model(
            session.session_id,
            model_yaml=yaml_text,
            extends=extends_yaml or None,
        )
        log.info(
            "model_loaded",
            session_id=session.session_id,
            model_id=loaded.model_id,
            data_objects=loaded.data_objects,
        )
        return session.session_id, loaded.model_id

    def _resolve_report_clock(
        self,
        *,
        session_id: str | None,
        model_id: str | None,
        fallback: datetime,
    ) -> tuple[datetime, str]:
        """Ask OBSL for the report's timestamp basis and IANA TZ.

        Calls ``GET /v1/settings`` and reads the ``timezone`` block:

        * ``effective`` (or ``database``) → the IANA TZ to display in.
        * ``utc`` (or ``now``) → the API server's current instant. We
          prefer ``utc`` because it's unambiguous; ``now`` carries an
          offset that we still parse correctly.

        The server-side instant is the right report timestamp: a runner
        on a different host or with clock drift would otherwise label its
        own wall clock with the database's TZ, which is misleading.

        Falls back to ``fallback`` (the runner's clock) and UTC on any
        failure or when the response doesn't carry the relevant fields.
        """
        try:
            payload = self._client.settings(session_id=session_id, model_id=model_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("settings_lookup_failed", error=f"{type(exc).__name__}: {exc}")
            return fallback, "UTC"

        tz_name = "UTC"
        instant: datetime | None = None

        tz_block = payload.get("timezone")
        if isinstance(tz_block, dict):
            for key in ("effective", "database"):
                value = tz_block.get(key)
                if isinstance(value, str) and value:
                    tz_name = value
                    break

            for key in ("utc", "now"):
                raw = tz_block.get(key)
                if not isinstance(raw, str) or not raw:
                    continue
                try:
                    # Python's fromisoformat accepts offsets like +02:00 in
                    # 3.11+, but rejects the trailing "Z". Normalise first.
                    instant = datetime.fromisoformat(raw.replace("Z", "+00:00"))
                except ValueError:
                    log.warning("settings_now_unparseable", field=key, value=raw)
                    continue
                if instant.tzinfo is None:
                    instant = instant.replace(tzinfo=UTC)
                break

        return instant if instant is not None else fallback, tz_name

    def _render_report(
        self,
        spec: RunSpec,
        results: dict[str, ExecuteResult],
        started_at: datetime,
        output_dir: Path | None,
        *,
        tz_name: str = "UTC",
    ) -> Path:
        try:
            tz = ZoneInfo(tz_name)
        except ZoneInfoNotFoundError:
            log.warning("unknown_timezone", tz=tz_name, fallback="UTC")
            tz_name = "UTC"
            tz = ZoneInfo("UTC")
        local_dt = started_at.astimezone(tz)
        is_utc = tz_name.upper() == "UTC"
        datetime_str = local_dt.strftime("%Y-%m-%dT%H-%M-%S") + ("Z" if is_utc else "")
        tz_filename = tz_name.replace("/", ", ")
        ctx = {
            "name": spec.name,
            "date": local_dt.strftime("%Y-%m-%d"),
            "datetime": datetime_str,
            "time": local_dt.strftime("%H:%M:%S"),
            "time_filename": local_dt.strftime("%H_%M_%S"),
            "tz": tz_name,
            "tz_filename": tz_filename,
            "timezone": tz_filename,  # alias of tz_filename — friendlier name
        }
        report_spec = spec.report
        if not report_spec.sections:
            auto = _auto_sections(spec.queries)
            if auto:
                report_spec = report_spec.model_copy(update={"sections": auto})
        body = render_markdown(report_spec, results, context=ctx)
        out_path = Path(spec.report.output.format(**ctx))
        if output_dir is not None and not out_path.is_absolute():
            out_path = output_dir / out_path
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(body, encoding="utf-8")
        return out_path


def _auto_sections(queries: list[QuerySpec]) -> list[ReportSection]:
    """Build one section per query when the spec didn't list any.

    Heading and description come from the query's ``description`` field
    (the leading ``# …`` block of the query file): the first non-empty
    line becomes the heading, the rest the description. Falls back to
    the query name when the file has no leading comment. Render mode is
    inferred from the query body — measure-only queries become ``value``,
    everything else stays ``table``.
    """
    sections: list[ReportSection] = []
    for q in queries:
        heading, description = _split_heading(q.description) if q.description else (q.name, None)
        sections.append(
            ReportSection(
                heading=heading,
                query=q.name,
                description=description,
                render=_auto_render_mode(q.query),
            )
        )
    return sections


def _split_heading(comment: str) -> tuple[str, str | None]:
    """Take a multi-line comment block and split it into (heading, body)."""
    lines = comment.splitlines()
    heading = ""
    body_start = 0
    for i, line in enumerate(lines):
        if line.strip():
            heading = line.strip()
            body_start = i + 1
            break
    while body_start < len(lines) and not lines[body_start].strip():
        body_start += 1
    body = "\n".join(lines[body_start:]).rstrip() or None
    return heading or comment, body


def _auto_render_mode(query: dict[str, Any]) -> str:
    """Pick ``value`` for measure-only queries, ``table`` for everything else.

    A measure-only query has ``measures`` set and exactly zero dimensions /
    fields. That shape returns a single row with one numeric cell, which
    reads better as a single bold number than a one-row table.
    """
    select = query.get("select") if isinstance(query, dict) else None
    if not isinstance(select, dict):
        return "table"
    measures = select.get("measures") or []
    dimensions = select.get("dimensions") or []
    fields = select.get("fields") or []
    if measures and not dimensions and not fields and len(measures) == 1:
        return "value"
    return "table"
