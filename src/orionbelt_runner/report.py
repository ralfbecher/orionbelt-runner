"""Markdown rendering for ExecuteResult sections."""

from __future__ import annotations

import re
from typing import Any

from orionbelt_runner.client import ExecuteResult
from orionbelt_runner.spec import ReportSection, ReportSpec

# Anchored regex spans (e.g. ``^[A-Z]{2}$``) inside a description trip
# markdown's link / inline-code heuristics. Wrap any such span in
# backticks so it renders as inline code instead of distorting the line.
# Conservative: only matches `^…$` chunks with no whitespace and no
# pre-existing backticks, so we never double-wrap.
_ANCHORED_REGEX = re.compile(r"(?<!`)(\^[^\s`]+\$)(?!`)")


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
    lines = [f"## {_safe_description(section.heading)}\n"]
    if section.description:
        lines.append(_safe_description(section.description) + "\n")

    if section.render == "table":
        lines.append(_render_table(result))
    elif section.render == "value":
        lines.append(_render_value(result, section.value_column))
    elif section.render == "list":
        lines.append(_render_list(result, section.list_column))

    return "\n".join(lines) + "\n"


def _safe_description(text: str) -> str:
    """Wrap anchored-regex-looking spans in backticks for markdown safety."""
    return _ANCHORED_REGEX.sub(r"`\1`", text)


def _render_table(result: ExecuteResult) -> str:
    if not result.columns:
        return "_No columns returned._"
    names = [c.name for c in result.columns]
    if not result.rows:
        return _table_header(names) + "\n_No rows._"
    rows = [_format_row(r) for r in result.rows]
    return _table_header(names) + "\n" + "\n".join(rows)


def _table_header(names: list[str]) -> str:
    header = "| " + " | ".join(names) + " |"
    sep = "| " + " | ".join("---" for _ in names) + " |"
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
    if isinstance(column, str):
        for i, c in enumerate(result.columns):
            if c.name == column:
                return i
    if prefer_numeric:
        # Prefer column.type when OBSL provides it (format_values=true makes
        # cells strings, so runtime isinstance checks are unreliable). Fall
        # back to runtime sniffing for legacy callers passing untyped columns.
        for i, c in enumerate(result.columns):
            if c.type == "number":
                return i
        if result.rows:
            for i, cell in enumerate(result.rows[0]):
                if isinstance(cell, (int, float)) and not isinstance(cell, bool):
                    return i
    return None
