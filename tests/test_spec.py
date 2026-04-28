"""Tests for spec loading and validation."""

from __future__ import annotations

from pathlib import Path

import pytest
from pydantic import ValidationError

from orionbelt_runner.spec import RunSpec, load_spec


def test_loads_example_spec() -> None:
    path = Path(__file__).resolve().parents[1] / "examples" / "monthly-revenue.yaml"
    spec = load_spec(path)
    assert isinstance(spec, RunSpec)
    assert spec.name == "Monthly Revenue"
    assert len(spec.queries) == 3
    assert {q.name for q in spec.queries} == {
        "total_revenue",
        "revenue_by_country",
        "top_customers_raw",
    }
    assert spec.report.format == "markdown"
    assert "{date}" in spec.report.output


def test_rejects_unknown_top_level_keys() -> None:
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        RunSpec.model_validate(
            {
                "name": "x",
                "queries": [{"name": "q", "query": {}}],
                "report": {"format": "markdown", "output": "/tmp/r.md", "title": "T"},
                "bogus": True,
            }
        )


def test_section_query_is_a_name_reference() -> None:
    spec = RunSpec.model_validate(
        {
            "name": "x",
            "queries": [{"name": "q1", "query": {"select": {"measures": ["M"]}}}],
            "report": {
                "format": "markdown",
                "output": "/tmp/r.md",
                "title": "T",
                "sections": [{"heading": "H", "query": "q1", "render": "value"}],
            },
        }
    )
    assert spec.report.sections[0].query == "q1"
