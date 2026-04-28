# OrionBelt Runner

Run [OrionBelt Semantic Layer](https://github.com/ralfbecher/orionbelt-semantic-layer) query batches and emit reports.

A run is a YAML document combining:

- An **OBSL endpoint** (base URL, optional model id, optional auth)
- A list of **named queries** — any valid OBML query body
- A **report config** — markdown output with sections bound to queries

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

- Single-model mode (`MODEL_FILE=...`) — uses top-level shortcut endpoints
- `QUERY_EXECUTE=true` (or `FLIGHT_ENABLED=true`)
- DB driver credentials configured for the dialect(s) you query

Multi-model deployments pass `obsl.model_id` in the spec.

## Spec format

See [`examples/monthly-revenue.yaml`](examples/monthly-revenue.yaml) for a full spec.

```yaml
name: Monthly Revenue
obsl:
  base_url: http://localhost:8080
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
