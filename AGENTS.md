# AGENTS.md

Guidance for coding agents working in this repository.

## Project Overview

**OrionBelt Runner** runs OBSL query batches and emits reports plus data exports. A run is a single YAML spec. Output today is markdown / HTML / PDF reports and Parquet / Arrow / TSV exports to a folder or S3; chart embedding is planned.

This repo **does not vendor OBSL**. All access goes through the public REST API of [orionbelt-semantic-layer](https://github.com/ralforion/orionbelt-semantic-layer) via a small `ObslClient` protocol (`src/orionbelt_runner/client.py`). When OBSL changes, only the HTTP client adapter needs to follow.

## Commands

```bash
uv sync                                                 # install
uv sync --extra arrow --extra pdf --extra dev           # + parquet/arrow/S3, PDF, test tooling
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
├── runner.py      # Runner — orchestrates query execution + report rendering + exports
├── report.py      # Markdown / HTML / PDF rendering (table / value / list)
├── runlog.py      # YAML run-log sidecar rendering
├── exports.py     # Per-query export bodies: TSV / Parquet / Arrow IPC (pure)
├── sinks.py       # Where export bytes land: LocalSink (folder) / S3Sink (pyarrow.fs)
└── cli.py         # Typer CLI: orionbelt-runner run / version
```

## Testing

```
tests/
├── obsl_stub.py           # StubObsl — OBSL 2.25-shaped responses (the contract fixture)
├── conftest.py            # obsl_stub / stub_client / stub_server fixtures
├── test_obsl_contract.py  # the runner against those responses, end to end
└── test_*.py              # unit tests, mostly against a hand-rolled ObslClient fake
```

**Anything about how OBSL answers belongs in `tests/obsl_stub.py`.** Every bug shipped in 0.8.0 was contract drift — the runner's models describing OBSL's responses slightly wrong, with hand-written fixtures encoding the same misunderstanding, so nothing failed until a real server answered. Pick the fixture that matches the layer:

- `stub_client` — a real `HttpObslClient` on a mock transport. No sockets. Default choice; covers params, headers, content-type negotiation, parsing.
- `stub_server` — the stub on a loopback port, for CLI-level runs.
- `obsl_stub` — the payload object itself; set `arrow_transport=False`, `warnings=[…]`, `columns=[…]` per test, and read `.requests` to assert how the runner called it.

When OBSL's contract changes, update `obsl_stub.py` **first** and let the failures show you what to fix. `test_stub_payloads_match_the_obsl_schemas` cross-checks the stub's field names (including pydantic aliases — OBSL sends `dataType` for `data_type`) against a sibling `../orionbelt-semantic-layer` checkout; it skips when the clone isn't there, so it guards a dev machine, not CI.

## Design rules

- **The Protocol is the seam.** Anything the runner needs from OBSL goes through `ObslClient`. Tests use a fake; a future in-process client lives next to `HttpObslClient` without touching `runner.py` / `report.py` / `cli.py`.
- **Pass query bodies through unchanged.** The runner does not parse or transform OBML queries — it forwards them to OBSL and treats the result as data.
- **Spec is the public contract.** Validate with Pydantic; keep `extra="forbid"` on `RunSpec` so typos surface early.
- **Reports are pure functions.** `render_markdown(spec, results, context)` takes the spec and the materialized rows; no I/O. The `Runner` is the only place that writes files.
- **Exports render to bytes, sinks own the write.** `render_export(result, fmt=…)` is pure; a `Sink` (folder or S3) is the only thing that touches a destination. New destinations (GCS, Azure) become new `Sink` implementations in `sinks.py` — `runner.py` must not learn about them.
- **Typed exports use the Arrow transport, and only pay for what's consumed.** Parquet / Arrow read via `execute_arrow()` (`?format=arrow`), whose table is written through untouched — never re-infer types the server already resolved. `build_arrow_table()` is the JSON fallback only. Note OBSL sends governed DECIMAL as *exact strings* in JSON, so the `decimal(p, s)` column hint must be honoured there; the report and TSV need the formatted run. `runner.run()` derives `needs_formatted` / `needs_raw` from the spec and executes each query once or twice accordingly — an export-only spec with only typed targets runs raw-only. Never format values client-side to fill this gap.
- **Exports and the run log never *destroy* a run, but they do report.** A failing target or raw re-execute is logged and skipped — a rendered report must survive an unreachable bucket — and lands in `RunResult.export_errors`, which the CLI turns into a non-zero exit. `succeeded` = every query ran; `fully_delivered` = that plus every export landed. Never write a typed export from formatted strings as a fallback: skip the query instead of silently changing the downstream schema.
- **Optional deps stay optional.** pyarrow (Parquet / Arrow / S3) and WeasyPrint (PDF) are imported inside the function that needs them, and the missing-dependency error names the extra to install. Core markdown / HTML / TSV runs must work on a bare `uv sync`.

## Conventions

- Python 3.12+, `from __future__ import annotations` everywhere
- Pydantic v2 for all I/O models
- Ruff: `["E", "F", "I", "N", "UP", "B", "A", "SIM"]`, line-length 100
- mypy strict mode with `pydantic.mypy` plugin
- structlog for logging — JSON-friendly when piped to a log collector

## OBSL version compatibility

Each runner minor line declares the OBSL minor series it supports: **0.9.x ↔ OBSL 2.25.x**. (The two version numbers don't advance together — a runner minor can ship features of its own against an unchanged OBSL line, as 0.8.0 did.) `HttpObslClient.preflight()` calls the unauthenticated `GET /health` (which returns the OBSL release `version` and the active `auth_mode`) before any query and raises `ObslVersionError` if the server is outside the supported line, or `ObslPreflightError` if the server enforces `AUTH_MODE=api_key` but no key was configured. The CLI runs preflight automatically (skippable with `--skip-preflight`). The pin lives in `client.py` as `SUPPORTED_OBSL_MAJOR` / `SUPPORTED_OBSL_MINOR` — bump them whenever the runner adopts a new OBSL minor series, and take a runner minor bump at the same time so the supported pairing is readable from the version alone.

Note: `GET /v1/settings` also exposes `version` (release) plus `api_version` (the REST prefix, currently `"v1"` — *not* a semver). The runner still reads `settings()` mid-run to capture `version` / `api_version` into the run log, but the version *gate* is the `/health` preflight.

## Out of scope (for now)

- Scheduling — drive from cron / systemd / Cloud Scheduler / GitHub Actions
- Chart generation — landing later, likely via OrionBelt Analytics
- Multi-model session orchestration — supported via `model_id` only
- Non-S3 object stores (GCS, Azure) and partitioned / append-mode dataset writes — one file per query per target today
- Per-target query filtering — every target exports every query

When any of these arrive, keep them behind the same `ObslClient` boundary or add a sibling module — do not couple them into `runner.py` directly.
