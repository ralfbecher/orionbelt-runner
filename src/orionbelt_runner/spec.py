"""Run-spec models — the YAML format the runner consumes.

A run spec is a self-describing YAML document combining:

* OBSL connection details
* A list of named queries (any valid OBML query body)
* A report config (output path, sections referencing queries)
* Zero or more data-export targets (folder or S3 bucket)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator
from ruamel.yaml import YAML

# Compression codecs each export format accepts. "default" leaves the choice
# to the writer (snappy for parquet, uncompressed for arrow/tsv).
_CODECS: dict[str, set[str]] = {
    "parquet": {"default", "none", "snappy", "gzip", "brotli", "lz4", "zstd"},
    "arrow": {"default", "none", "lz4", "zstd"},
    "tsv": {"default", "none"},
}


class ModelSpec(BaseModel):
    """Model the runner loads into a fresh OBSL session at run start.

    When set, the runner switches from single-model shortcut endpoints to a
    session-scoped flow: create session → load model → run queries → delete
    session. Paths resolve relative to the spec file's directory.
    """

    yaml_path: Path
    extends: list[Path] = Field(default_factory=list)


class ObslSpec(BaseModel):
    """OBSL endpoint configuration.

    ``locale`` and ``timezone`` are forwarded as query params on every
    /query/execute call so OBSL renders numeric and timestamp cells with
    locale-aware formatting (matching the Gradio UI). When omitted the
    server falls back to ``DEFAULT_LOCALE`` and the model's default timezone.
    """

    base_url: str = "http://localhost:8080"
    model_id: str | None = None
    # API key for OBSL >= 2.12 (AUTH_MODE=api_key). Sent in the header named
    # by ``api_key_header`` (default X-API-Key).
    api_key: str | None = None
    api_key_header: str = "X-API-Key"
    timeout_seconds: float = 30.0
    model: ModelSpec | None = None
    locale: str | None = None  # BCP-47, e.g. "de", "en-US"
    timezone: str | None = None  # IANA TZ, e.g. "Europe/Berlin"


class QuerySpec(BaseModel):
    """A single named query — passed through to OBSL as-is.

    ``dialect`` is left optional so the loader can distinguish "user did not
    specify" (None) from an explicit per-query value. ``load_spec`` fills any
    None entries with the spec-level default, so by the time the runner sees
    a QuerySpec the field is always a string.
    """

    name: str
    description: str | None = None
    dialect: str | None = None
    query: dict[str, Any]


class ReportSection(BaseModel):
    """A markdown section bound to one of the spec's queries."""

    heading: str
    query: str  # references QuerySpec.name
    description: str | None = None
    render: Literal["table", "value", "list"] = "table"
    # When render="value": column index or name to project (default: first numeric).
    value_column: str | int | None = None
    # When render="list": column index or name to project (default: first column).
    list_column: str | int | None = None


class ReportSpec(BaseModel):
    """Report config. Markdown / HTML / PDF — chart formats land later.

    ``format: pdf`` reuses the HTML pipeline (HTML body → WeasyPrint → PDF)
    and requires the optional ``pdf`` extra: ``uv sync --extra pdf``.

    ``output`` / ``title`` / ``intro`` all run through ``str.format`` against
    the same placeholder set:

    * ``{name}``           — spec name.
    * ``{date}``           — ``YYYY-MM-DD`` in the resolved TZ.
    * ``{time}``           — ``HH:MM:SS`` in the resolved TZ (colons are
      filesystem-unsafe on Windows; use ``{time_filename}`` for paths).
    * ``{time_filename}``  — ``HH_MM_SS`` (filesystem-safe).
    * ``{datetime}``       — ``YYYY-MM-DDTHH-MM-SS`` plus ``Z`` only when
      the TZ is UTC (filesystem-safe everywhere).
    * ``{tz}``             — IANA name, e.g. ``Europe/Berlin``.
    * ``{timezone}``       — same as ``{tz}`` but with ``/`` replaced by
      ``, `` so the value is safe to drop into a path.
    * ``{tz_filename}``    — alias of ``{timezone}`` (kept for back-compat).

    The instant comes from OBSL's ``GET /v1/settings`` (``timezone.utc``)
    when the runner can reach a session-loaded model; falls back to the
    runner's own UTC clock otherwise.

    ``footer`` (optional, rendered below the last section) accepts the
    same placeholders plus result-derived counters — both snake_case and
    camelCase forms are exposed:

    * ``{number_of_queries}`` / ``{numberOfQueries}``     — queries that ran.
    * ``{number_of_sections}`` / ``{numberOfSections}``   — sections rendered.
    * ``{number_of_rows}`` / ``{numberOfRows}``           — sum of row_count
      across all results.
    """

    format: Literal["markdown", "html", "pdf"] = "markdown"
    # PDF page size + orientation. Ignored for markdown / HTML output.
    # A4 portrait is the default because most reports are headline +
    # a few narrow tables. Use A3 (or landscape) for wide tables with
    # many columns or long values that would otherwise wrap.
    pdf_page_size: Literal["A4", "A3"] = "A4"
    pdf_orientation: Literal["portrait", "landscape"] = "portrait"
    output: str
    title: str
    intro: str | None = None
    footer: str | None = None
    sections: list[ReportSection] = Field(default_factory=list)
    # When true, write each query's result rows as TSV into a sibling
    # ``<report-stem>_exports/`` directory next to the rendered report. One
    # TSV per query; only successful results are exported.
    export_results: bool = False


class ExportTarget(BaseModel):
    """One data-export destination — a folder or an S3 prefix.

    Each target writes one file per query, named after the query (sanitised
    to safe path chars) with the format's extension. Exports are only written
    on a fully successful run.

    ``uri`` accepts the same ``str.format`` placeholders as ``report.output``
    (``{name}``, ``{date}``, ``{datetime}``, ``{time_filename}``, ``{tz}``, …)
    and points at a *directory prefix*:

    * ``./out/{date}/`` or ``/var/data/{name}`` — local folder. Relative paths
      resolve under ``--output-dir`` when that flag is set, exactly like the
      report path.
    * ``s3://bucket/prefix/{date}/`` — S3 or any S3-compatible store.

    ``parquet`` / ``arrow`` need the optional ``arrow`` extra
    (``uv sync --extra arrow``) and carry *native types* — for those formats
    the runner re-executes each query with ``format_values=false`` so numbers
    and timestamps land as ``double`` / ``timestamp`` rather than as
    locale-formatted strings. ``tsv`` mirrors the report's formatted cells and
    needs no extra dependency.
    """

    model_config = ConfigDict(extra="forbid")

    format: Literal["parquet", "arrow", "tsv"] = "parquet"
    uri: str
    compression: Literal["default", "none", "snappy", "gzip", "brotli", "lz4", "zstd"] = "default"
    # S3 only. Credentials always come from the standard AWS chain (env vars,
    # ~/.aws/credentials, instance/task role) — never from the spec file.
    # Set endpoint_override for MinIO / Cloudflare R2 / Ceph; the
    # AWS_ENDPOINT_URL env var is used as a fallback when this is unset.
    endpoint_override: str | None = None
    region: str | None = None

    @model_validator(mode="after")
    def _check_compression(self) -> ExportTarget:
        allowed = _CODECS[self.format]
        if self.compression not in allowed:
            raise ValueError(
                f"compression {self.compression!r} is not valid for format "
                f"{self.format!r} (allowed: {', '.join(sorted(allowed))})"
            )
        return self

    @property
    def needs_raw_values(self) -> bool:
        """True when this target wants natively typed (unformatted) cells."""
        return self.format in {"parquet", "arrow"}


class RunSpec(BaseModel):
    """Top-level run definition.

    Queries can be declared inline under ``queries:`` and/or loaded from a
    folder via ``queries_dir:``. When both are set, dir queries (alpha-sorted
    by relative path) run first, then inline queries in spec order. ``load_spec``
    enforces that at least one query exists overall and that names are unique.

    Set ``no_report: true`` for an **export-only** run — queries execute and
    ``exports:`` targets are written, but no report is rendered. The ``report:``
    block then becomes optional; keep it (it's ignored except as the runlog's
    location) to toggle reporting off without deleting the config.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    dialect: str = "postgres"
    obsl: ObslSpec = Field(default_factory=ObslSpec)
    queries_dir: Path | None = None
    queries: list[QuerySpec] = Field(default_factory=list)
    report: ReportSpec | None = None
    # Export-only run: execute queries and write `exports:` targets, skip
    # rendering (and skip `report.export_results`, which is report-relative).
    no_report: bool = False
    # Data exports to a folder or S3 bucket — independent of the report and of
    # ``report.export_results`` (the sibling-TSV shortcut), which keeps working.
    exports: list[ExportTarget] = Field(default_factory=list)

    @model_validator(mode="after")
    def _check_report_present(self) -> RunSpec:
        if self.report is None and not self.no_report:
            raise ValueError(
                "spec has no `report:` block — add one, or set `no_report: true` "
                "for an export-only run"
            )
        return self

    @property
    def renders_report(self) -> bool:
        """True when this run should render a report."""
        return self.report is not None and not self.no_report


def load_spec(path: Path | str) -> RunSpec:
    """Load and validate a YAML run spec from disk.

    Paths in ``obsl.model`` and ``queries_dir`` resolve relative to the spec
    file so users can keep model + query YAML next to the run spec. Queries
    found under ``queries_dir`` are prepended to ``spec.queries`` (dir-first,
    inline-after); duplicate names raise.
    """
    yaml = YAML(typ="safe")
    spec_path = Path(path)
    raw = yaml.load(spec_path.read_text(encoding="utf-8"))
    if raw is None:
        raise ValueError(f"Empty or invalid YAML at {path}")
    spec = RunSpec.model_validate(raw)
    base = spec_path.resolve().parent

    if spec.obsl.model is not None:
        spec.obsl.model.yaml_path = (base / spec.obsl.model.yaml_path).resolve()
        spec.obsl.model.extends = [(base / p).resolve() for p in spec.obsl.model.extends]

    if spec.queries_dir is not None:
        queries_dir = (base / spec.queries_dir).resolve()
        spec.queries = _load_queries_from_dir(queries_dir) + spec.queries

    if not spec.queries:
        raise ValueError(
            f"Spec at {path} defines no queries (queries: empty and queries_dir absent or empty)"
        )

    for q in spec.queries:
        if q.dialect is None:
            q.dialect = spec.dialect

    seen: set[str] = set()
    for q in spec.queries:
        if q.name in seen:
            raise ValueError(f"Duplicate query name in spec: {q.name!r}")
        seen.add(q.name)

    return spec


def _extract_leading_comment(text: str) -> str | None:
    """Return the leading ``# …`` block of a YAML file as plain text.

    Walks lines from the top, collecting comment bodies (with the leading
    ``# `` stripped) and stopping at the first non-comment, non-blank line.
    Blank lines inside the comment block are preserved so callers can split
    a heading from a body. Returns ``None`` when there's no leading
    comment.
    """
    collected: list[str] = []
    started = False
    for line in text.splitlines():
        stripped = line.strip()
        if stripped.startswith("#"):
            content = stripped.lstrip("#").lstrip()
            collected.append(content)
            started = True
        elif stripped == "":
            if started:
                collected.append("")
            # else: skip leading blank lines
        else:
            break
    while collected and not collected[-1]:
        collected.pop()
    return "\n".join(collected) if collected else None


def _load_queries_from_dir(dir_path: Path) -> list[QuerySpec]:
    """Recursively load *.yaml / *.yml from ``dir_path`` as QuerySpec objects.

    Files are sorted alpha by their path relative to ``dir_path``. Empty
    files are skipped silently.

    Two file shapes are accepted:

    * **Wrapped** — a full QuerySpec with a top-level ``query:`` key (and
      optionally ``name:``, ``description:``, ``dialect:``). Missing ``name``
      defaults to the filename stem.
    * **Bare body** — the OBML query body itself at the top level (``select:``,
      ``where:``, etc.). The whole mapping is taken as the query body, the
      filename stem becomes the name, and ``dialect`` defaults to ``postgres``.

    Detection rule: presence of a top-level ``query:`` key → wrapped, else
    → bare body. To customise name/dialect for a bare-body file, wrap it.

    The file's leading ``# …`` comment block (if any) is captured as the
    QuerySpec's ``description`` when no explicit ``description:`` is set
    in the wrapped form. The runner uses this to derive auto-section
    headings + descriptions when the spec doesn't list explicit sections.
    """
    if not dir_path.is_dir():
        raise ValueError(f"queries_dir is not a directory: {dir_path}")

    yaml = YAML(typ="safe")
    files = sorted(
        [*dir_path.rglob("*.yaml"), *dir_path.rglob("*.yml")],
        key=lambda p: p.relative_to(dir_path).as_posix(),
    )
    out: list[QuerySpec] = []
    for f in files:
        text = f.read_text(encoding="utf-8")
        raw = yaml.load(text)
        if raw is None:
            continue
        if not isinstance(raw, dict):
            raise ValueError(f"Query file must be a YAML mapping: {f}")
        comment = _extract_leading_comment(text)
        if "query" in raw:
            if "name" not in raw:
                raw["name"] = f.stem
            spec = QuerySpec.model_validate(raw)
            if spec.description is None and comment:
                spec.description = comment
            out.append(spec)
        else:
            out.append(QuerySpec(name=f.stem, description=comment, query=raw))
    return out
