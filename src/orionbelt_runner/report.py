"""Markdown rendering for ExecuteResult sections."""

from __future__ import annotations

from typing import Any

from orionbelt_runner.client import ExecuteResult
from orionbelt_runner.spec import ReportSection, ReportSpec


def render_markdown(
    spec: ReportSpec,
    results: dict[str, ExecuteResult],
    context: dict[str, str] | None = None,
) -> str:
    """Render a complete markdown report for the given spec + results."""
    ctx = context or {}
    parts: list[str] = []

    title = spec.title.format(**ctx)
    parts.append(f"# {title}\n")
    if spec.intro:
        parts.append(spec.intro.format(**ctx) + "\n")

    for section in spec.sections:
        if section.query not in results:
            parts.append(f"## {section.heading}\n\n_Missing query result: `{section.query}`_\n")
            continue
        parts.append(_render_section(section, results[section.query]))

    return "\n".join(parts).rstrip() + "\n"


def _render_section(section: ReportSection, result: ExecuteResult) -> str:
    lines = [f"## {section.heading}\n"]
    if section.description:
        lines.append(section.description + "\n")

    if section.render == "table":
        lines.append(_render_table(result))
    elif section.render == "value":
        lines.append(_render_value(result, section.value_column))
    elif section.render == "list":
        lines.append(_render_list(result, section.list_column))

    return "\n".join(lines) + "\n"


def _render_table(result: ExecuteResult) -> str:
    if not result.columns:
        return "_No columns returned._"
    if not result.rows:
        return _table_header(result.columns) + "\n_No rows._"
    rows = [_format_row(r) for r in result.rows]
    return _table_header(result.columns) + "\n" + "\n".join(rows)


def _table_header(columns: list[str]) -> str:
    header = "| " + " | ".join(columns) + " |"
    sep = "| " + " | ".join("---" for _ in columns) + " |"
    return f"{header}\n{sep}"


def _format_row(row: list[Any]) -> str:
    return "| " + " | ".join(_format_cell(c) for c in row) + " |"


def _format_cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value).replace("|", "\\|").replace("\n", " ")


def _render_value(result: ExecuteResult, column: str | int | None) -> str:
    if not result.rows:
        return "_No rows._"
    idx = _resolve_column_index(result, column, prefer_numeric=True)
    cell = result.rows[0][idx] if idx is not None else result.rows[0][0]
    return f"**{_format_cell(cell)}**"


def _render_list(result: ExecuteResult, column: str | int | None) -> str:
    if not result.rows:
        return "_No rows._"
    idx = _resolve_column_index(result, column, prefer_numeric=False)
    if idx is None:
        idx = 0
    items = [f"- {_format_cell(row[idx])}" for row in result.rows]
    return "\n".join(items)


def _resolve_column_index(
    result: ExecuteResult, column: str | int | None, *, prefer_numeric: bool
) -> int | None:
    if isinstance(column, int):
        return column
    if isinstance(column, str) and column in result.columns:
        return result.columns.index(column)
    if prefer_numeric and result.rows:
        for i, cell in enumerate(result.rows[0]):
            if isinstance(cell, (int, float)) and not isinstance(cell, bool):
                return i
    return None
