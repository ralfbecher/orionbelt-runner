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


class ObslSpec(BaseModel):
    """OBSL endpoint configuration."""

    base_url: str = "http://localhost:8080"
    model_id: str | None = None
    api_token: str | None = None
    timeout_seconds: float = 30.0


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
    """Load and validate a YAML run spec from disk."""
    yaml = YAML(typ="safe")
    raw = yaml.load(Path(path).read_text(encoding="utf-8"))
    if raw is None:
        raise ValueError(f"Empty or invalid YAML at {path}")
    return RunSpec.model_validate(raw)
