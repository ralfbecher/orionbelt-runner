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
    # Names track whatever the example currently ships; assert they exist
    # rather than pinning to a particular set so doc-tweaks don't break tests.
    assert len(spec.queries) >= 1
    assert all(isinstance(q.name, str) and q.name for q in spec.queries)
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


# ---- queries_dir loading ---------------------------------------------------


def _write_spec(tmp_path: Path, body: str) -> Path:
    spec_path = tmp_path / "spec.yaml"
    spec_path.write_text(body, encoding="utf-8")
    return spec_path


def _write_query(path: Path, body: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(body, encoding="utf-8")


_BASE_SPEC = """\
name: Multi
queries_dir: ./queries
report:
  format: markdown
  output: out.md
  title: T
"""


def test_queries_dir_loads_files_and_uses_filename_stem(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(
        tmp_path / "queries" / "headline.yaml",
        "dialect: postgres\nquery: { select: { measures: [Total] } }\n",
    )
    spec = load_spec(spec_path)
    assert [q.name for q in spec.queries] == ["headline"]
    assert spec.queries[0].dialect == "postgres"


def test_queries_dir_recursive_alpha_sorted(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(tmp_path / "queries" / "b.yaml", "query: {}\n")
    _write_query(tmp_path / "queries" / "a.yml", "query: {}\n")
    _write_query(tmp_path / "queries" / "sub" / "c.yaml", "query: {}\n")
    spec = load_spec(spec_path)
    # Alpha sort by relative path: "a.yml", "b.yaml", "sub/c.yaml"
    assert [q.name for q in spec.queries] == ["a", "b", "c"]


def test_queries_dir_explicit_name_overrides_filename(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(
        tmp_path / "queries" / "file-stem.yaml",
        "name: explicit_name\nquery: {}\n",
    )
    spec = load_spec(spec_path)
    assert [q.name for q in spec.queries] == ["explicit_name"]


def test_queries_dir_dir_first_inline_after(tmp_path: Path) -> None:
    spec_path = _write_spec(
        tmp_path,
        _BASE_SPEC + "queries:\n  - { name: inline_one, query: {} }\n",
    )
    _write_query(tmp_path / "queries" / "from_dir.yaml", "query: {}\n")
    spec = load_spec(spec_path)
    assert [q.name for q in spec.queries] == ["from_dir", "inline_one"]


def test_queries_dir_duplicate_names_raise(tmp_path: Path) -> None:
    spec_path = _write_spec(
        tmp_path,
        _BASE_SPEC + "queries:\n  - { name: dup, query: {} }\n",
    )
    _write_query(tmp_path / "queries" / "dup.yaml", "query: {}\n")
    with pytest.raises(ValueError, match="Duplicate query name"):
        load_spec(spec_path)


def test_spec_with_no_queries_at_all_raises(tmp_path: Path) -> None:
    spec_path = _write_spec(
        tmp_path,
        "name: Empty\nreport:\n  format: markdown\n  output: o.md\n  title: T\n",
    )
    with pytest.raises(ValueError, match="defines no queries"):
        load_spec(spec_path)


def test_queries_dir_skips_empty_files(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(tmp_path / "queries" / "empty.yaml", "")
    _write_query(tmp_path / "queries" / "real.yaml", "query: {}\n")
    spec = load_spec(spec_path)
    assert [q.name for q in spec.queries] == ["real"]


def test_queries_dir_loads_bare_body_files(tmp_path: Path) -> None:
    """A query file without a top-level `query:` is treated as the body itself."""
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(
        tmp_path / "queries" / "headline.yaml",
        "select:\n  measures:\n    - Total\n",
    )
    spec = load_spec(spec_path)
    assert [q.name for q in spec.queries] == ["headline"]
    assert spec.queries[0].dialect == "postgres"
    assert spec.queries[0].query == {"select": {"measures": ["Total"]}}


def test_spec_level_dialect_fills_bare_body_queries(tmp_path: Path) -> None:
    """Top-level `dialect:` propagates to bare-body queries that don't set their own."""
    spec_path = _write_spec(
        tmp_path,
        "name: Multi\ndialect: snowflake\nqueries_dir: ./queries\n"
        "report:\n  format: markdown\n  output: out.md\n  title: T\n",
    )
    _write_query(
        tmp_path / "queries" / "headline.yaml",
        "select:\n  measures:\n    - Total\n",
    )
    spec = load_spec(spec_path)
    assert spec.queries[0].dialect == "snowflake"


def test_spec_level_dialect_does_not_override_explicit(tmp_path: Path) -> None:
    """A wrapped query with its own `dialect:` keeps its value over the spec default."""
    spec_path = _write_spec(
        tmp_path,
        "name: Multi\ndialect: snowflake\nqueries_dir: ./queries\n"
        "report:\n  format: markdown\n  output: out.md\n  title: T\n",
    )
    _write_query(
        tmp_path / "queries" / "pg.yaml",
        "dialect: postgres\nquery: { select: { measures: [Total] } }\n",
    )
    spec = load_spec(spec_path)
    assert spec.queries[0].dialect == "postgres"


def test_queries_dir_captures_leading_comment_as_description(tmp_path: Path) -> None:
    """The first ``# …`` block of a query file lands on QuerySpec.description."""
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(
        tmp_path / "queries" / "headline.yaml",
        "# Headline KPI\n# Total revenue across all regions.\nselect:\n  measures:\n    - Total\n",
    )
    spec = load_spec(spec_path)
    assert spec.queries[0].name == "headline"
    assert spec.queries[0].description == "Headline KPI\nTotal revenue across all regions."


def test_queries_dir_explicit_description_wins_over_comment(tmp_path: Path) -> None:
    """A wrapped file's explicit ``description:`` is not overwritten by the comment."""
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(
        tmp_path / "queries" / "wrapped.yaml",
        "# leading comment that should not win\n"
        "description: explicit\nquery: { select: { measures: [Total] } }\n",
    )
    spec = load_spec(spec_path)
    assert spec.queries[0].description == "explicit"


def test_queries_dir_mixes_wrapped_and_bare(tmp_path: Path) -> None:
    spec_path = _write_spec(tmp_path, _BASE_SPEC)
    _write_query(
        tmp_path / "queries" / "a_wrapped.yaml",
        "dialect: postgres\nquery: { select: { measures: [Total] } }\n",
    )
    _write_query(
        tmp_path / "queries" / "b_bare.yaml",
        "select:\n  fields:\n    - X.Y\n",
    )
    spec = load_spec(spec_path)
    names = [q.name for q in spec.queries]
    assert names == ["a_wrapped", "b_bare"]
    assert spec.queries[1].query == {"select": {"fields": ["X.Y"]}}
