"""Run-spec models — the YAML format the runner consumes.

A run spec is a self-describing YAML document combining:

* OBSL connection details
* A list of named queries (any valid OBML query body)
* A report config (output path, sections referencing queries)
"""

from __future__ import annotations

from pathlib import Path
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field
from ruamel.yaml import YAML


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
    api_token: str | None = None
    timeout_seconds: float = 30.0
    model: ModelSpec | None = None
    locale: str | None = None  # BCP-47, e.g. "de", "en-US"
    timezone: str | None = None  # IANA TZ, e.g. "Europe/Berlin"


class QuerySpec(BaseModel):
    """A single named query — passed through to OBSL as-is."""

    name: str
    description: str | None = None
    dialect: str = "postgres"
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
    """Markdown report config. PDF / chart formats land later."""

    format: Literal["markdown"] = "markdown"
    output: str  # supports {date}, {datetime}, {name} placeholders
    title: str
    intro: str | None = None
    sections: list[ReportSection] = Field(default_factory=list)


class RunSpec(BaseModel):
    """Top-level run definition.

    Queries can be declared inline under ``queries:`` and/or loaded from a
    folder via ``queries_dir:``. When both are set, dir queries (alpha-sorted
    by relative path) run first, then inline queries in spec order. ``load_spec``
    enforces that at least one query exists overall and that names are unique.
    """

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    obsl: ObslSpec = Field(default_factory=ObslSpec)
    queries_dir: Path | None = None
    queries: list[QuerySpec] = Field(default_factory=list)
    report: ReportSpec


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

    seen: set[str] = set()
    for q in spec.queries:
        if q.name in seen:
            raise ValueError(f"Duplicate query name in spec: {q.name!r}")
        seen.add(q.name)

    return spec


def _load_queries_from_dir(dir_path: Path) -> list[QuerySpec]:
    """Recursively load *.yaml / *.yml from ``dir_path`` as QuerySpec objects.

    Files are sorted alpha by their path relative to ``dir_path``. A missing
    ``name:`` defaults to the filename stem (wysiwyg — no normalization).
    Empty files are skipped silently.
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
        raw = yaml.load(f.read_text(encoding="utf-8"))
        if raw is None:
            continue
        if not isinstance(raw, dict):
            raise ValueError(f"Query file must be a YAML mapping: {f}")
        if "name" not in raw:
            raw["name"] = f.stem
        out.append(QuerySpec.model_validate(raw))
    return out
