<p align="center">
  <img src="https://raw.githubusercontent.com/ralforion/orionbelt-semantic-layer/main/docs/assets/ORIONBELT_Logo.png" alt="OrionBelt logo — a stylized belt of three stars" width="400">
</p>

<h1 align="center">OrionBelt® Runner</h1>

<p align="center"><strong>Run <a href="https://github.com/ralforion/orionbelt-semantic-layer">OrionBelt Semantic Layer</a> query batches and emit reports.</strong></p>

[![Version 0.7.0](https://img.shields.io/badge/version-0.7.0-purple.svg)](https://github.com/ralforion/orionbelt-runner/releases)
[![OBSL 2.23.x](https://img.shields.io/badge/OBSL-2.23.x-9cf.svg)](https://github.com/ralforion/orionbelt-semantic-layer)
[![Python 3.12+](https://img.shields.io/badge/python-3.12+-blue.svg)](https://www.python.org/downloads/)
[![License: BSL 1.1](https://img.shields.io/badge/License-BSL_1.1-orange.svg)](LICENSE)

[![Pydantic v2](https://img.shields.io/badge/Pydantic-v2-E92063.svg?logo=pydantic&logoColor=white)](https://docs.pydantic.dev)
[![Typer](https://img.shields.io/badge/Typer-CLI-009688.svg)](https://typer.tiangolo.com)
[![WeasyPrint](https://img.shields.io/badge/WeasyPrint-PDF-FF6F00.svg)](https://weasyprint.org)
[![Ruff](https://img.shields.io/badge/lint-ruff-D7FF64.svg?logo=ruff&logoColor=black)](https://docs.astral.sh/ruff/)

[![Docker Hub](https://img.shields.io/docker/v/ralforion/orionbelt-runner?logo=docker&logoColor=white&label=Docker%20Hub&color=2496ED&sort=semver)](https://hub.docker.com/r/ralforion/orionbelt-runner/tags)
[![Docker pulls](https://img.shields.io/docker/pulls/ralforion/orionbelt-runner?logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/ralforion/orionbelt-runner)
[![Image size](https://img.shields.io/docker/image-size/ralforion/orionbelt-runner/latest?logo=docker&logoColor=white&color=2496ED)](https://hub.docker.com/r/ralforion/orionbelt-runner)

Run [OrionBelt Semantic Layer](https://github.com/ralforion/orionbelt-semantic-layer) query batches and emit reports.

A run is a YAML document combining:

- An **OBSL endpoint** (base URL, optional auth, optional locale/timezone, optional model to load)
- A list of **named queries** — any valid OBML query body
- A **report config** — markdown, HTML, or PDF output with sections bound to queries
- Optional **data exports** — Parquet / Arrow / TSV to a folder or an S3 bucket (report optional: set `no_report: true` for an export-only run)

Numeric and timestamp cells are pre-rendered server-side using each column's `format` pattern from the OBML model (the runner sends `format_values=true` on every query), so reports show e.g. `1.853.429,67` for `locale: de` without any client-side formatting. See [`examples/monthly-revenue-2026-04-29.md`](examples/monthly-revenue-2026-04-29.md) (markdown), [`examples/monthly-revenue-2026-04-29.html`](examples/monthly-revenue-2026-04-29.html) (HTML), and [`examples/monthly-revenue-2026-04-29.pdf`](examples/monthly-revenue-2026-04-29.pdf) (PDF) for sample outputs.

## Status

Early scaffold (v0.7.0). Markdown, HTML, and PDF reports, per-query data exports (Parquet / Arrow / TSV, to a folder or S3), and an always-on YAML run log sidecar. No scheduler yet — drive it from cron / systemd / GitHub Actions / Cloud Scheduler / etc.

## Install

```bash
uv sync                  # core: markdown + HTML reports, TSV exports
uv sync --extra pdf      # also enable PDF output (requires Pango / Cairo)
uv sync --extra arrow    # also enable Parquet / Arrow exports and S3 destinations
```

PDF output needs WeasyPrint, which depends on system libraries (Pango,
Cairo, GDK-Pixbuf). On macOS: `brew install pango`. On Debian / Ubuntu:
`apt install libpango-1.0-0 libpangoft2-1.0-0`. See the
[WeasyPrint install guide](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#installation)
for other platforms. Skip the extra if you only need markdown / HTML.

> **Apple Silicon note:** Homebrew installs libraries to `/opt/homebrew/lib`,
> which Python's loader doesn't search by default. If WeasyPrint can't find
> `libgobject-2.0-0`, prefix the runner with:
>
> ```bash
> DYLD_LIBRARY_PATH=/opt/homebrew/lib:/usr/local/lib \
> DYLD_FALLBACK_LIBRARY_PATH=/opt/homebrew/lib:/usr/local/lib \
> uv run orionbelt-runner run spec.yaml
> ```
>
> `DYLD_FALLBACK_LIBRARY_PATH` is often enough, but setting both variables
> covers Python/cffi environments that call `dlopen()` with bare library
> names such as `libgobject-2.0-0`.

## Run

```bash
uv run orionbelt-runner run examples/monthly-revenue.yaml
```

Override the OBSL endpoint without editing the spec:

```bash
uv run orionbelt-runner run examples/monthly-revenue.yaml \
  --base-url http://my-obsl:8080
```

Or via env (`.env` or shell):

```bash
OBSL_BASE_URL=http://my-obsl:8080 \
OBSL_API_KEY=obsl_pat_... \
uv run orionbelt-runner run examples/monthly-revenue.yaml
```

## Docker

The runner ships as a container image on Docker Hub
([`ralforion/orionbelt-runner`](https://hub.docker.com/r/ralforion/orionbelt-runner)).
The `ENTRYPOINT` is the CLI, so arguments go straight through:

```bash
docker run --rm ralforion/orionbelt-runner version
```

Mount your specs in and a directory for the reports out. The image works under
`/work`, so relative paths in the spec resolve there:

```bash
docker run --rm \
  -e OBSL_BASE_URL=http://my-obsl:8080 \
  -e OBSL_API_KEY=obsl_pat_... \
  -v "$PWD/examples:/work/examples" \
  -v "$PWD/reports:/work/reports" \
  ralforion/orionbelt-runner run examples/monthly-revenue.yaml
```

> When OBSL runs on the host, use `--add-host=host.docker.internal:host-gateway`
> and `OBSL_BASE_URL=http://host.docker.internal:8080` so the container can
> reach it.

The image covers markdown / HTML reports and all data exports — pyarrow ships
in it, so Parquet / Arrow and `s3://` destinations work out of the box (mount a
volume for local export folders, or point at a bucket and pass AWS credentials
as env vars). PDF output (WeasyPrint + Pango / Cairo system libs) is not
bundled — run `--extra pdf` on a host install for that.

```bash
docker run --rm \
  -e OBSL_BASE_URL=http://my-obsl:8080 \
  -e AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY -e AWS_REGION \
  -v "$PWD/examples:/work/examples" \
  ralforion/orionbelt-runner run examples/revenue-export-only.yaml
```

Build it yourself instead of pulling:

```bash
docker build -t orionbelt-runner .
```

## Authentication

OBSL >= 2.12 supports `AUTH_MODE=api_key` (see the OBSL [authentication
guide](https://github.com/ralforion/orionbelt-semantic-layer/blob/main/docs/guide/authentication.md)).
When the server enforces auth, give the runner an API key:

```yaml
obsl:
  base_url: http://my-obsl:8080
  api_key: obsl_pat_...        # preferred
  api_key_header: X-API-Key    # optional; match the server's API_KEY_HEADER
```

Or via env, which overrides the spec — keep secrets out of YAML:

| Variable | Purpose |
|----------|---------|
| `OBSL_API_KEY` | API key. |
| `OBSL_API_KEY_HEADER` | Header name (default `X-API-Key`). |

The key is sent in the `X-API-Key` header by default (OBSL also accepts
`Authorization: Bearer`). When auth is off (`AUTH_MODE=none`) the key is
ignored, so it's safe to leave set. A `401`/`403` from OBSL surfaces as an
`ObslAuthError` with a hint about which key/header was sent.

## Compatibility & startup preflight

This runner's **0.7.x** line tracks **OBSL 2.23.x**. Before running any query,
`orionbelt-runner run` calls the unauthenticated `/health` endpoint and checks:

- the server version is in the supported `2.23.x` line (older → upgrade the
  server; newer → upgrade the runner), and
- an API key is configured when the server reports `AUTH_MODE=api_key`.

A failed check exits non-zero with `Preflight failed: …` before any session is
created. Pass `--skip-preflight` to bypass it (e.g. against a custom build).

OBSL 2.16 validates model and query payloads against its published JSON Schemas
at the ingestion boundary. A malformed OBML model loaded by the runner, or a
query body in the spec that violates the schema, comes back as a `422` and
surfaces as an `ObslSchemaError` listing the offending fields. The schema is
camelCase — use canonical camelCase keys, lowercase enum values, and a numeric
(not string) model `version`.

## Server expectations

The runner calls **`/v1/query/execute`**, so OBSL needs to be configured to execute queries (not just compile them):

- `QUERY_EXECUTE=true` (or `FLIGHT_ENABLED=true`)
- DB driver credentials configured for the dialect(s) you query

Three deployment shapes are supported, in order of preference:

1. **Single-model mode** (`MODEL_FILE=...` on the server). Spec leaves `obsl.model` and `obsl.model_id` unset; the runner uses top-level shortcut endpoints.
2. **Multi-model server** with a model already loaded. Set `obsl.model_id` in the spec; the runner still uses shortcut endpoints and keys into the named model.
3. **Runner-loaded model**. Set `obsl.model.yaml_path` in the spec — the runner creates a session, posts the model to `/v1/sessions/{id}/models`, runs queries against `/v1/sessions/{id}/query/execute`, and deletes the session in a `finally` block. Useful for ad-hoc reports against a model you keep next to the spec file.

## Spec format

See [`examples/monthly-revenue.yaml`](examples/monthly-revenue.yaml) for a full spec.

```yaml
name: Monthly Revenue
obsl:
  base_url: http://localhost:8080
  locale: de                       # optional — BCP-47, drives display formatting
  # timezone: Europe/Berlin        # optional — IANA TZ
  # model_id: sales                # multi-model server with a pre-loaded model
  # model:                         # OR: load your own model into a fresh session
  #   yaml_path: ./sales.obml.yaml # path is resolved relative to this spec file
  #   extends: [./fragments/dim-time.yaml]
queries:
  - name: total_revenue
    dialect: postgres
    query:
      select:
        measures: [Total Revenue]
report:
  format: markdown
  output: reports/{name}-{date}.md
  title: "Monthly Revenue — {date}"
  sections:
    - heading: Headline number
      query: total_revenue
      render: value
```

**Section render modes:**

| `render` | Output |
|---|---|
| `table` | Table of all rows |
| `value` | Single bold value (first numeric column of first row by default) |
| `list`  | Bullet list of one column |

**Path placeholders** are accepted in `report.output`, `report.title`, `report.intro`, and `report.footer`. The instant comes from OBSL's `GET /v1/settings` (server clock + effective TZ); falls back to the runner's UTC clock when settings is unreachable.

| Placeholder | Example | Notes |
|---|---|---|
| `{name}` | `Monthly Revenue` | the spec name |
| `{date}` | `2026-04-29` | YYYY-MM-DD in the resolved TZ |
| `{datetime}` | `2026-04-29T18-02-06Z` | filesystem-safe; trailing `Z` only when TZ is UTC |
| `{time}` | `18:02:06` | colons are unsafe on Windows paths — use `{time_filename}` |
| `{time_filename}` | `18_02_06` | filesystem-safe |
| `{tz}` | `Europe/Berlin` | IANA name |
| `{tz_filename}` / `{timezone}` | `Europe, Berlin` | `/` replaced with `, ` for path safety |
| `{runner_version}` | `0.4.0` | the OrionBelt Runner version that produced this report |
| `{duration_ms}` | `1234` | wall-clock run duration in whole milliseconds; best in `intro` / `footer` / `title`, not paths |

`report.footer` additionally accepts result-derived counters: `{number_of_queries}`, `{number_of_sections}`, `{number_of_rows}` (camelCase aliases — `{numberOfQueries}` etc. — also work).

### Queries from a folder

For larger query libraries, point at a directory instead of (or in addition to) inline `queries:`:

```yaml
queries_dir: ./queries        # recursive *.yaml + *.yml, alpha-sorted
queries:                      # optional, runs after dir queries in spec order
  - name: ad_hoc
    query: { ... }
```

Each file is a full `QuerySpec` (`name`, `dialect`, `query`, optional `description`). When `name:` is omitted the filename stem is used verbatim — `total-revenue.yaml` → `total-revenue`. Duplicate names across the dir + inline list raise an error at load time. Paths resolve relative to the spec file.

## Outputs

A run produces up to three artefacts in the report directory, plus any
configured `exports:` destinations:

```
reports/monthly-revenue-2026-04-29.md           ← report
reports/monthly-revenue-2026-04-29.run.yaml     ← run log (always)
reports/monthly-revenue-2026-04-29_exports/     ← TSV exports (opt-in)
  ├── total_revenue.tsv
  ├── revenue_by_nation.tsv
  └── top_orders_raw.tsv

exports/Monthly Revenue/2026-04-29/             ← `exports:` target (opt-in)
  ├── total_revenue.parquet
  ├── revenue_by_nation.parquet
  └── top_orders_raw.parquet
s3://analytics-landing/orionbelt/…/*.parquet    ← `exports:` target (opt-in)
```

### Report — markdown, HTML, or PDF

`format: markdown` (default) writes a `.md`. `format: html` writes a self-contained HTML5 document with inline default CSS and a light/dark theme — no external assets, so the file works when emailed, opened from disk, or served from a static host. The output extension follows the template you set in `output:`.

```yaml
report:
  format: html
  output: reports/{name}-{date}.html
  title: "Monthly Revenue — {date}"
```

See [`examples/monthly-revenue-2026-04-29.html`](examples/monthly-revenue-2026-04-29.html) for a rendered HTML sample.

`format: pdf` reuses the same HTML pipeline and runs the result through [WeasyPrint](https://weasyprint.org/) — so PDF layout stays automatically in lockstep with the HTML output. A print-only stylesheet adds page margins and a `page n / total` footer; section headings (`h2`) get a `page-break-before: auto` / `page-break-after: avoid` hint so tables don't get orphaned.

```yaml
report:
  format: pdf
  output: reports/{name}-{date}.pdf
  title: "Monthly Revenue — {date}"
  pdf_page_size: A4         # "A4" (default) or "A3"
  pdf_orientation: portrait # "portrait" (default) or "landscape"
```

`pdf_page_size` and `pdf_orientation` are ignored for `markdown` / `html` output. Reach for **A3** or **landscape** when a table has many columns or wide cell values that wrap awkwardly in A4 portrait — the same content, just more horizontal room.

PDF requires the optional `pdf` extra (`uv sync --extra pdf`) and WeasyPrint's [system libraries](https://doc.courtbouillon.org/weasyprint/stable/first_steps.html#installation) — see [Install](#install). See [`examples/monthly-revenue-2026-04-29.pdf`](examples/monthly-revenue-2026-04-29.pdf) for a rendered PDF sample.

### Run log (YAML sidecar) — always written

Always emitted next to the report at `<report-stem>.run.yaml`, even when queries fail (when it's most useful). Captures, for each query: the compiled SQL as a literal block scalar, the OBSL `explain` plan (planner + reasons + joins + CFL legs), the `resolved` info (fact tables / dimensions / measures), wall-clock and server-side timing, warnings, and any errors. The file header records the spec name, OBSL `version` / `api_version`, session/model IDs, and the report path.

YAML so it's both human-skimmable and trivially machine-parseable in one step. Useful for debugging, audit trails, and downstream tooling that wants the SQL or the plan.

See [`examples/monthly-revenue-2026-04-29.run.yaml`](examples/monthly-revenue-2026-04-29.run.yaml).

### Per-query TSV exports — opt-in

Set `report.export_results: true` to write each query's rows as TSV into a sibling `<report-stem>_exports/` directory:

```yaml
report:
  output: reports/{name}-{date}.md
  export_results: true
```

One file per query, named after the query and sanitised to safe path chars. TSV uses `\t` separator and `\n` line endings; cells with embedded tabs / newlines / quotes are quoted (compatible with `pandas.read_csv(..., sep='\t')`). Cells reflect the same `format_values=true` data the report shows. Exports are only written on a fully successful run.

See [`examples/monthly-revenue-2026-04-29_exports/`](examples/monthly-revenue-2026-04-29_exports/).

### Data exports — Parquet / Arrow / TSV, to a folder or S3

`exports:` is a list of destinations, each written on a fully successful run —
one file per query, named after the query (sanitised to safe path chars):

```yaml
exports:
  - format: parquet                        # parquet | arrow | tsv
    uri: exports/{name}/{date}/            # local folder
    compression: zstd                      # default | none | snappy | gzip | brotli | lz4 | zstd

  - format: parquet
    uri: s3://analytics-landing/orionbelt/{date}/
    region: eu-central-1
    # endpoint_override: http://localhost:9000   # MinIO / R2 / Ceph
```

`uri` takes the same placeholders as `report.output` (`{name}`, `{date}`,
`{datetime}`, `{time_filename}`, `{tz}`, …) and names a **directory prefix**.
Local relative paths are rebased under `--output-dir` when that flag is set,
exactly like the report path. Several targets can run side by side (e.g. a
local folder for the team, an S3 prefix for the warehouse).

**Values are natively typed.** Parquet and Arrow are typed formats, so for
those targets the runner executes with `format_values=false` and writes what
comes back — `double`, `int64`, `timestamp[us]`, not the report's
locale-formatted strings.

The runner only pays for what a run actually consumes:

| spec | executes per query |
| --- | --- |
| report (or a `tsv` target) only | 1 — formatted |
| report (or a `tsv` target) **+** parquet / arrow | 2 — formatted for the report, raw for the export |
| `no_report: true` with only parquet / arrow targets | 1 — raw |

So a report run keeps rendering from the formatted call (its cells stay
OBSL-authoritative), while an export-only job doesn't pay double DB cost for
display strings nobody reads.

| format    | extension  | values                    | compression                                   |
| --------- | ---------- | ------------------------- | --------------------------------------------- |
| `parquet` | `.parquet` | native types (raw run)    | `snappy` (default), `gzip`, `zstd`, `brotli`, `lz4`, `none` |
| `arrow`   | `.arrow`   | native types (raw run)    | uncompressed (default), `lz4`, `zstd`         |
| `tsv`     | `.tsv`     | formatted, as in the report | —                                           |

`parquet` / `arrow` and any `s3://` destination need the optional `arrow`
extra (`uv sync --extra arrow`), which pulls in pyarrow — it provides both the
file formats and the S3 client. `tsv` to a local folder needs nothing extra.

**S3 credentials are never read from the spec.** pyarrow uses the standard AWS
chain: `AWS_ACCESS_KEY_ID` / `AWS_SECRET_ACCESS_KEY` (+ `AWS_SESSION_TOKEN`),
`~/.aws/credentials`, or the instance / task / IRSA role — so a spec file stays
safe to commit. Only the non-secret `region` and `endpoint_override` live in
the spec; `AWS_ENDPOINT_URL` works as a fallback for the latter.

**Failures are visible, but never destructive.** An unreachable bucket or a
failed raw re-execute is logged (`export_target_failed` / `raw_execute_failed`)
and skipped, leaving the report and run log intact — but the CLI then prints
what didn't land and **exits 1**, so a scheduled job whose data never arrived
can't look like a clean run:

```
$ orionbelt-runner run extract.yaml; echo "exit=$?"
Run log written: reports/Revenue Extract-2026-04-29T12-00-00.run.yaml
  export parquet → s3://analytics-landing/… failed: OSError: bucket not found

1 export target(s) failed
exit=1
```

A query whose raw re-execute failed is *omitted* from typed targets rather than
written with string columns, so a downstream schema never silently changes
shape — and that omission is reported the same way. On `RunResult`,
`succeeded` still means "every query ran"; `fully_delivered` additionally means
"every configured export landed".

### Export-only runs — `no_report`

Set `no_report: true` to skip report rendering entirely: queries run, `exports:`
targets are written, and the run log still lands (that's the audit trail).

```yaml
name: Revenue Extract
no_report: true
queries: [...]
exports:
  - format: parquet
    uri: s3://analytics-landing/orionbelt/{name}/{date}/
```

The `report:` block becomes optional in this mode — keep it to toggle reporting
off without deleting the config (the run log then stays in its configured
folder), or drop it and the log goes to `{name}-{datetime}.run.yaml`, rebased
under `--output-dir`. `report.export_results` (the sibling-TSV shortcut) is
report-relative and therefore inert here; use an `exports:` target instead.

See [`examples/revenue-export-only.yaml`](examples/revenue-export-only.yaml).

## Architecture

The runner talks to OBSL through a small `ObslClient` Protocol. One implementation today (HTTP). Tests can drop in a fake; an in-process implementation can be added later without touching the runner, report, or CLI code.

```
spec.yaml ──▶ load_spec ──▶ Runner ──▶ ObslClient ──▶ OBSL (HTTP)
                              │
                              ├─▶ render_markdown / render_html / render_pdf ──▶ report.md|html|pdf
                              ├─▶ render_runlog                 ──▶ report.run.yaml
                              ├─▶ render_tsv (× N)              ──▶ report_exports/*.tsv
                              └─▶ render_export (× N) ──▶ Sink  ──▶ ./folder/*.parquet|arrow|tsv
                                                                    s3://bucket/prefix/*
```

## License

Copyright 2025 [RALFORION d.o.o.](https://ralforion.com)

Licensed under the [Business Source License 1.1](LICENSE). The Licensed Work will convert to Apache License 2.0 on 2030-03-16.

For commercial licensing inquiries, contact: licensing@ralforion.com

---

<p align="center">
  <a href="https://ralforion.com">
    <img src="https://raw.githubusercontent.com/ralforion/orionbelt-semantic-layer/main/docs/assets/RALFORION_doo_Logo.png" alt="RALFORION d.o.o." width="200">
  </a>
</p>

<p align="center">
  Copyright © 2026 RALFORION d.o.o.<br>
  OrionBelt® is a registered trademark of RALFORION d.o.o.
</p>
