"""Core runner: spec in, ExecuteResult-per-query + rendered report out."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from datetime import UTC, datetime
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import structlog

from orionbelt_runner import __version__
from orionbelt_runner.client import ExecuteResult, ObslClient
from orionbelt_runner.exports import (
    FORMAT_EXTENSIONS,
    render_export,
    render_tsv,
    safe_export_filename,
)
from orionbelt_runner.report import render_html, render_markdown, render_pdf
from orionbelt_runner.runlog import ObslMeta, QueryLogEntry, RunLog, render_runlog
from orionbelt_runner.sinks import open_sink
from orionbelt_runner.spec import ExportTarget, ModelSpec, QuerySpec, ReportSection, RunSpec

log = structlog.get_logger("orionbelt_runner")

# Runlog location for export-only specs that dropped the `report:` block —
# there's no report path to mirror. `_runlog_path_from_report` appends
# `.run.yaml`, so this template carries no extension of its own.
DEFAULT_RUNLOG_OUTPUT = "{name}-{datetime}"


@dataclass
class RunResult:
    """Outcome of a single run."""

    spec_name: str
    started_at: datetime
    finished_at: datetime
    results: dict[str, ExecuteResult] = field(default_factory=dict)
    report_path: Path | None = None
    runlog_path: Path | None = None
    exports_dir: Path | None = None
    # One entry per configured ``exports:`` target that wrote at least one
    # file — the resolved folder path or ``s3://bucket/prefix``.
    export_locations: list[str] = field(default_factory=list)
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
        log.info(
            "run_start",
            spec=spec.name,
            runner_version=__version__,
            query_count=len(spec.queries),
        )

        session_id: str | None = None
        model_id: str | None = spec.obsl.model_id
        results: dict[str, ExecuteResult] = {}
        # Parquet / Arrow are typed formats, so they need the unformatted
        # result. Fetched with a second execute per query (format_values=false)
        # only when such a target is configured — the report keeps using the
        # formatted run so its cells stay OBSL-authoritative.
        needs_raw = any(t.needs_raw_values for t in spec.exports)
        raw_results: dict[str, ExecuteResult] = {}
        errors: dict[str, str] = {}
        query_entries: list[QueryLogEntry] = []
        report_basis: datetime = started_at
        tz_name = "UTC"
        settings_payload: dict[str, Any] = {}

        try:
            if spec.obsl.model is not None:
                session_id, model_id = self._load_session_model(spec.obsl.model)

            report_basis, tz_name, settings_payload = self._resolve_report_clock(
                session_id=session_id, model_id=model_id, fallback=started_at
            )

            self._preflight_format_patterns(spec.queries, session_id=session_id, model_id=model_id)

            for q in spec.queries:
                q_started = datetime.now(tz=UTC)
                t0 = time.monotonic_ns()
                dialect = q.dialect or "postgres"
                try:
                    result = self._client.execute(
                        q.query,
                        dialect=dialect,
                        model_id=model_id,
                        session_id=session_id,
                        format_values=True,
                        locale=spec.obsl.locale,
                        timezone=spec.obsl.timezone,
                    )
                    duration_ms = (time.monotonic_ns() - t0) / 1e6
                    results[q.name] = result
                    query_entries.append(
                        QueryLogEntry(
                            name=q.name,
                            dialect=dialect,
                            started_at=q_started,
                            duration_ms=duration_ms,
                            result=result,
                        )
                    )
                    log.info("query_done", name=q.name, rows=len(result.rows))
                    if needs_raw:
                        self._fetch_raw(
                            q,
                            spec,
                            dialect=dialect,
                            model_id=model_id,
                            session_id=session_id,
                            into=raw_results,
                        )
                except Exception as exc:  # noqa: BLE001 — surface anything the client raises
                    duration_ms = (time.monotonic_ns() - t0) / 1e6
                    msg = f"{type(exc).__name__}: {exc}"
                    errors[q.name] = msg
                    query_entries.append(
                        QueryLogEntry(
                            name=q.name,
                            dialect=dialect,
                            started_at=q_started,
                            duration_ms=duration_ms,
                            error=msg,
                        )
                    )
                    log.error("query_failed", name=q.name, error=msg)
        finally:
            if session_id is not None:
                try:
                    self._client.close_session(session_id)
                    log.info("session_closed", session_id=session_id)
                except Exception as exc:  # noqa: BLE001
                    log.warning("session_close_failed", session_id=session_id, error=str(exc))

        finished_at = datetime.now(tz=UTC)

        duration_ms = (finished_at - started_at).total_seconds() * 1000

        report_path: Path | None = None
        exports_dir: Path | None = None
        export_locations: list[str] = []
        if results and not errors:
            if spec.renders_report:
                report_path = self._render_report(
                    spec,
                    results,
                    report_basis,
                    output_dir,
                    tz_name=tz_name,
                    duration_ms=duration_ms,
                )
                log.info("report_written", path=str(report_path))
            # The sibling-TSV shortcut is defined relative to the report file,
            # so it only applies when a report was rendered. Export-only runs
            # use `exports:` targets instead.
            if report_path is not None and spec.report is not None and spec.report.export_results:
                exports_dir = self._write_exports(results, report_path)
                if exports_dir is not None:
                    log.info(
                        "exports_written",
                        dir=str(exports_dir),
                        files=len(results),
                    )
            if spec.exports:
                ctx = _build_template_context(
                    spec.name, report_basis, tz_name, duration_ms=duration_ms
                )
                export_locations = self._write_export_targets(
                    spec.exports,
                    results,
                    raw_results,
                    ctx=ctx,
                    output_dir=output_dir,
                )

        runlog_path = self._write_runlog(
            spec=spec,
            started_at=started_at,
            finished_at=finished_at,
            report_basis=report_basis,
            tz_name=tz_name,
            output_dir=output_dir,
            session_id=session_id,
            model_id=model_id,
            settings_payload=settings_payload,
            query_entries=query_entries,
            errors=errors,
            report_path=report_path,
        )
        if runlog_path is not None:
            log.info("runlog_written", path=str(runlog_path))

        return RunResult(
            spec_name=spec.name,
            started_at=started_at,
            finished_at=finished_at,
            results=results,
            report_path=report_path,
            runlog_path=runlog_path,
            exports_dir=exports_dir,
            export_locations=export_locations,
            errors=errors,
        )

    def _fetch_raw(
        self,
        q: QuerySpec,
        spec: RunSpec,
        *,
        dialect: str,
        model_id: str | None,
        session_id: str | None,
        into: dict[str, ExecuteResult],
    ) -> None:
        """Re-execute ``q`` with ``format_values=false`` for typed exports.

        Timezone is still forwarded so timestamp cells come back in the
        report's zone; locale is irrelevant without value formatting.

        A failure here is not fatal — the report and the formatted exports are
        already safe. The query is simply skipped by the parquet / arrow
        targets, which is louder (a missing file) than the alternative of
        silently falling back to string-typed columns and changing the schema
        downstream consumers rely on.
        """
        try:
            into[q.name] = self._client.execute(
                q.query,
                dialect=dialect,
                model_id=model_id,
                session_id=session_id,
                format_values=False,
                timezone=spec.obsl.timezone,
            )
        except Exception as exc:  # noqa: BLE001 — exports must not fail the run
            log.warning(
                "raw_execute_failed",
                name=q.name,
                error=f"{type(exc).__name__}: {exc}",
                hint="query will be skipped by parquet / arrow export targets",
            )

    def _write_export_targets(
        self,
        targets: list[ExportTarget],
        results: dict[str, ExecuteResult],
        raw_results: dict[str, ExecuteResult],
        *,
        ctx: dict[str, Any],
        output_dir: Path | None,
    ) -> list[str]:
        """Write every configured ``exports:`` target; return their locations.

        One file per query per target. Typed formats read from ``raw_results``
        (the ``format_values=false`` run), TSV from the formatted ``results``
        so it mirrors the report.

        Like the sibling-TSV shortcut, a failing target is logged and skipped —
        an unreachable bucket must not discard a report that already rendered.
        """
        locations: list[str] = []
        for target in targets:
            source = raw_results if target.needs_raw_values else results
            try:
                uri = target.uri.format(**ctx)
                sink = open_sink(
                    uri,
                    endpoint_override=target.endpoint_override,
                    region=target.region,
                    base_dir=output_dir,
                )
                extension = FORMAT_EXTENSIONS[target.format]
                written = 0
                for name, result in source.items():
                    body = render_export(result, fmt=target.format, compression=target.compression)
                    sink.write(safe_export_filename(name, extension=extension), body)
                    written += 1
            except Exception as exc:  # noqa: BLE001 — never let exports mask the real run
                log.warning(
                    "export_target_failed",
                    uri=target.uri,
                    format=target.format,
                    error=f"{type(exc).__name__}: {exc}",
                )
                continue

            skipped = sorted(set(results) - set(source))
            if skipped:
                log.warning("export_queries_skipped", uri=sink.location, queries=skipped)
            if written:
                locations.append(sink.location)
            log.info(
                "export_target_written",
                location=sink.location,
                format=target.format,
                files=written,
            )
        return locations

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
    ) -> tuple[datetime, str, dict[str, Any]]:
        """Ask OBSL for the report's timestamp basis, IANA TZ, and full payload.

        Calls ``GET /v1/settings`` and reads the ``timezone`` block:

        * ``effective`` (or ``database``) → the IANA TZ to display in.
        * ``utc`` (or ``now``) → the API server's current instant. We
          prefer ``utc`` because it's unambiguous; ``now`` carries an
          offset that we still parse correctly.

        The server-side instant is the right report timestamp: a runner
        on a different host or with clock drift would otherwise label its
        own wall clock with the database's TZ, which is misleading.

        Returns the full settings dict alongside the resolved clock so
        callers (e.g. the run-log writer) can pull ``version`` /
        ``api_version`` from the same call without a second round trip.

        Falls back to ``fallback`` (the runner's clock) and UTC on any
        failure or when the response doesn't carry the relevant fields.
        """
        try:
            payload = self._client.settings(session_id=session_id, model_id=model_id)
        except Exception as exc:  # noqa: BLE001
            log.warning("settings_lookup_failed", error=f"{type(exc).__name__}: {exc}")
            return fallback, "UTC", {}

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

        return instant if instant is not None else fallback, tz_name, payload

    def _render_report(
        self,
        spec: RunSpec,
        results: dict[str, ExecuteResult],
        started_at: datetime,
        output_dir: Path | None,
        *,
        tz_name: str = "UTC",
        duration_ms: float | None = None,
    ) -> Path:
        ctx = _build_template_context(spec.name, started_at, tz_name, duration_ms=duration_ms)
        report_spec = spec.report
        if report_spec is None:  # pragma: no cover - guarded by spec.renders_report
            raise ValueError("cannot render a report: spec has no `report:` block")
        if not report_spec.sections:
            auto = _auto_sections(spec.queries)
            if auto:
                report_spec = report_spec.model_copy(update={"sections": auto})
        out_path = _resolve_output_path(report_spec.output, ctx, output_dir)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        if report_spec.format == "pdf":
            pdf_bytes = render_pdf(report_spec, results, context=ctx)
            out_path.write_bytes(pdf_bytes)
        elif report_spec.format == "html":
            out_path.write_text(render_html(report_spec, results, context=ctx), encoding="utf-8")
        else:
            out_path.write_text(
                render_markdown(report_spec, results, context=ctx), encoding="utf-8"
            )
        return out_path

    def _write_exports(
        self,
        results: dict[str, ExecuteResult],
        report_path: Path,
    ) -> Path | None:
        """Write each query's rows as TSV under ``<report-stem>_exports/``.

        One file per query, named after the query (sanitised to safe path
        chars). Returns the exports directory path on success, ``None`` if
        the write failed — exports are a convenience artefact, never fatal.
        """
        exports_dir = report_path.with_name(f"{report_path.stem}_exports")
        try:
            exports_dir.mkdir(parents=True, exist_ok=True)
            for name, result in results.items():
                file_path = exports_dir / safe_export_filename(name)
                file_path.write_text(render_tsv(result), encoding="utf-8")
            return exports_dir
        except Exception as exc:  # noqa: BLE001 — never let exports mask the real run
            log.warning("exports_write_failed", error=f"{type(exc).__name__}: {exc}")
            return None

    def _write_runlog(
        self,
        *,
        spec: RunSpec,
        started_at: datetime,
        finished_at: datetime,
        report_basis: datetime,
        tz_name: str,
        output_dir: Path | None,
        session_id: str | None,
        model_id: str | None,
        settings_payload: dict[str, Any],
        query_entries: list[QueryLogEntry],
        errors: dict[str, str],
        report_path: Path | None,
    ) -> Path | None:
        """Write the YAML run log sidecar.

        Always attempted, even when queries failed — that's exactly when
        the log is most useful. Path mirrors the report path stem with
        ``.md`` swapped to ``.run.yaml`` (or appended when the spec uses
        a non-markdown output template). Export-only specs that dropped the
        ``report:`` block fall back to ``{name}-{datetime}.run.yaml``,
        rebased under ``--output-dir`` like any other relative path.

        Failure to write is logged but not fatal: the runner has already
        produced everything else it can.
        """
        try:
            ctx = _build_template_context(spec.name, report_basis, tz_name)
            template = spec.report.output if spec.report is not None else DEFAULT_RUNLOG_OUTPUT
            runlog_path = _runlog_path_from_report(_resolve_output_path(template, ctx, output_dir))

            run_log = RunLog(
                spec=spec.name,
                description=spec.description,
                started_at=started_at,
                finished_at=finished_at,
                obsl=ObslMeta(
                    base_url=spec.obsl.base_url,
                    version=_str_or_none(settings_payload.get("version")),
                    api_version=_str_or_none(settings_payload.get("api_version")),
                    session_id=session_id,
                    model_id=model_id,
                    locale=spec.obsl.locale,
                    timezone=spec.obsl.timezone,
                ),
                queries=list(query_entries),
                errors=dict(errors),
                report_path=str(report_path) if report_path is not None else None,
            )
            body = render_runlog(run_log)
            runlog_path.parent.mkdir(parents=True, exist_ok=True)
            runlog_path.write_text(body, encoding="utf-8")
            return runlog_path
        except Exception as exc:  # noqa: BLE001 — never let logging mask the real run outcome
            log.warning("runlog_write_failed", error=f"{type(exc).__name__}: {exc}")
            return None


def _build_template_context(
    spec_name: str, basis: datetime, tz_name: str, *, duration_ms: float | None = None
) -> dict[str, Any]:
    """Build the ``str.format`` context shared by report + runlog paths.

    Single source of truth so the runlog lands next to the report even
    when the report template uses ``{date}`` / ``{time_filename}`` /
    ``{tz_filename}`` placeholders. The TZ resolution mirrors what
    ``_render_report`` did before this was extracted.

    ``duration_ms`` (the wall-clock run duration) is exposed as the
    ``{duration_ms}`` placeholder — rounded to a whole millisecond — for
    intro / footer templates. It's ``None`` for path-resolution callers
    (the run-log writer) that don't have a duration to report.
    """
    try:
        tz = ZoneInfo(tz_name)
    except ZoneInfoNotFoundError:
        log.warning("unknown_timezone", tz=tz_name, fallback="UTC")
        tz_name = "UTC"
        tz = ZoneInfo("UTC")
    local_dt = basis.astimezone(tz)
    is_utc = tz_name.upper() == "UTC"
    datetime_str = local_dt.strftime("%Y-%m-%dT%H-%M-%S") + ("Z" if is_utc else "")
    tz_filename = tz_name.replace("/", ", ")
    return {
        "name": spec_name,
        "date": local_dt.strftime("%Y-%m-%d"),
        "datetime": datetime_str,
        "time": local_dt.strftime("%H:%M:%S"),
        "time_filename": local_dt.strftime("%H_%M_%S"),
        "tz": tz_name,
        "tz_filename": tz_filename,
        "timezone": tz_filename,  # alias of tz_filename — friendlier name
        "runner_version": __version__,
        "duration_ms": round(duration_ms) if duration_ms is not None else None,
    }


def _resolve_output_path(template: str, ctx: dict[str, Any], output_dir: Path | None) -> Path:
    """Format the report-output template and apply ``--output-dir`` rebasing."""
    out_path = Path(template.format(**ctx))
    if output_dir is not None and not out_path.is_absolute():
        out_path = output_dir / out_path
    return out_path


def _runlog_path_from_report(report_path: Path) -> Path:
    """Derive the runlog sidecar path from the report path.

    ``foo.md`` / ``foo.html`` / ``foo.pdf`` → ``foo.run.yaml``. For other
    templates we append ``.run.yaml`` so the runlog is always uniquely
    named — never colliding with the report itself.
    """
    if report_path.suffix.lower() in {".md", ".html", ".htm", ".pdf"}:
        return report_path.with_suffix(".run.yaml")
    return report_path.with_name(report_path.name + ".run.yaml")


def _str_or_none(value: Any) -> str | None:
    """Coerce a ``/v1/settings`` field into a plain ``str | None`` for the runlog."""
    if value is None:
        return None
    return str(value)


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
