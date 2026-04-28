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
    """OBSL endpoint configuration."""

    base_url: str = "http://localhost:8080"
    model_id: str | None = None
    api_token: str | None = None
    timeout_seconds: float = 30.0
    model: ModelSpec | None = None


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
    """Top-level run definition."""

    model_config = ConfigDict(extra="forbid")

    name: str
    description: str | None = None
    obsl: ObslSpec = Field(default_factory=ObslSpec)
    queries: list[QuerySpec]
    report: ReportSpec


def load_spec(path: Path | str) -> RunSpec:
    """Load and validate a YAML run spec from disk.

    Model paths in ``obsl.model`` are resolved relative to the spec file so
    users can keep model YAML next to the run spec.
    """
    yaml = YAML(typ="safe")
    spec_path = Path(path)
    raw = yaml.load(spec_path.read_text(encoding="utf-8"))
    if raw is None:
        raise ValueError(f"Empty or invalid YAML at {path}")
    spec = RunSpec.model_validate(raw)

    if spec.obsl.model is not None:
        base = spec_path.resolve().parent
        spec.obsl.model.yaml_path = (base / spec.obsl.model.yaml_path).resolve()
        spec.obsl.model.extends = [(base / p).resolve() for p in spec.obsl.model.extends]

    return spec
