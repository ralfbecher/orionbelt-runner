# CLAUDE.md

Guidance for Claude Code working in this repository.

## Project Overview

**OrionBelt Runner** runs OBSL query batches and emits reports. A run is a single YAML spec. Output today is markdown; PDF and chart embedding are planned.

This repo **does not vendor OBSL**. All access goes through the public REST API of [orionbelt-semantic-layer](https://github.com/ralfbecher/orionbelt-semantic-layer) via a small `ObslClient` protocol (`src/orionbelt_runner/client.py`). When OBSL changes, only the HTTP client adapter needs to follow.

## Commands

```bash
uv sync                                                 # install
uv run orionbelt-runner run examples/monthly-revenue.yaml
uv run pytest                                           # tests
uv run ruff check src/ tests/                           # lint
uv run ruff format src/ tests/                          # format
uv run mypy src/                                        # type check
```

## Architecture

```
src/orionbelt_runner/
├── __init__.py    # __version__
├── client.py      # ObslClient protocol + HttpObslClient
├── spec.py        # Pydantic models for the YAML spec + load_spec()
├── runner.py      # Runner — orchestrates query execution + report rendering
├── report.py      # Markdown rendering (table / value / list)
└── cli.py         # Typer CLI: orionbelt-runner run / version
```

## Design rules

- **The Protocol is the seam.** Anything the runner needs from OBSL goes through `ObslClient`. Tests use a fake; a future in-process client lives next to `HttpObslClient` without touching `runner.py` / `report.py` / `cli.py`.
- **Pass query bodies through unchanged.** The runner does not parse or transform OBML queries — it forwards them to OBSL and treats the result as data.
- **Spec is the public contract.** Validate with Pydantic; keep `extra="forbid"` on `RunSpec` so typos surface early.
- **Reports are pure functions.** `render_markdown(spec, results, context)` takes the spec and the materialized rows; no I/O. The `Runner` is the only place that writes files.

## Conventions

- Python 3.12+, `from __future__ import annotations` everywhere
- Pydantic v2 for all I/O models
- Ruff: `["E", "F", "I", "N", "UP", "B", "A", "SIM"]`, line-length 100
- mypy strict mode with `pydantic.mypy` plugin
- structlog for logging — JSON-friendly when piped to a log collector

## OBSL version compatibility

OBSL exposes `version` and `api_version` on `GET /v1/settings`. The HTTP client should call `settings()` once at startup and surface a clear error if the OBSL version is too old (when we start gating on features). For now we depend on OBSL `>= 2.1.0` (raw mode lands there).

## Out of scope (for now)

- Scheduling — drive from cron / systemd / Cloud Scheduler / GitHub Actions
- PDF rendering — landing later, likely WeasyPrint
- Chart generation — landing later, likely via OrionBelt Analytics
- Multi-model session orchestration — supported via `model_id` only

When any of these arrive, keep them behind the same `ObslClient` boundary or add a sibling module — do not couple them into `runner.py` directly.
