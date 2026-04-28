# OrionBelt Runner

Run [OrionBelt Semantic Layer](https://github.com/ralfbecher/orionbelt-semantic-layer) query batches and emit reports.

A run is a YAML document combining:

- An **OBSL endpoint** (base URL, optional auth, optional locale/timezone, optional model to load)
- A list of **named queries** — any valid OBML query body
- A **report config** — markdown output with sections bound to queries

Numeric and timestamp cells are pre-rendered server-side using each column's `format` pattern from the OBML model (the runner sends `format_values=true` on every query), so reports show e.g. `1.853.429,67` for `locale: de` without any client-side formatting. See [`examples/monthly-revenue-sample.md`](examples/monthly-revenue-sample.md) for what a rendered report looks like.

## Status

Early scaffold (v0.1.0). Markdown reports only. No scheduler yet — drive it from cron / systemd / GitHub Actions / Cloud Scheduler / etc.

## Install

```bash
uv sync
```

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
OBSL_API_TOKEN=... \
uv run orionbelt-runner run examples/monthly-revenue.yaml
```

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
| `table` | Markdown table of all rows |
| `value` | Single bold value (first numeric column of first row by default) |
| `list`  | Bullet list of one column |

**Path placeholders** in `report.output`, `report.title`, `report.intro`: `{name}`, `{date}`, `{datetime}`.

## Architecture

The runner talks to OBSL through a small `ObslClient` Protocol. One implementation today (HTTP). Tests can drop in a fake; an in-process implementation can be added later without touching the runner, report, or CLI code.

```
spec.yaml ──▶ load_spec ──▶ Runner ──▶ ObslClient ──▶ OBSL (HTTP)
                              │
                              └─▶ render_markdown ──▶ report.md
```

## License

BSL-1.1 (mirrors OrionBelt Semantic Layer).
