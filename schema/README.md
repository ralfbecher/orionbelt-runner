# Vendored OBML schema

`obml-schema.json` is a **vendored copy** of the OBML semantic-model JSON Schema
from [orionbelt-semantic-layer](https://github.com/ralforion/orionbelt-semantic-layer).
It exists only so editors (via `# yaml-language-server: $schema=...`) can validate
and autocomplete the example model files under `examples/` — it is **not** read at
runtime. The runner never parses OBML models; it forwards run-specs to OBSL over REST.

## Provenance

- Source: `schema/obml-schema.json` in orionbelt-semantic-layer
- OBSL version: **v2.25.1** (matches this runner's supported line, 0.9.x ↔ OBSL 2.25.x)

## Re-syncing

This is a static copy and will drift. Re-sync whenever the runner's supported OBSL
minor line bumps (see `SUPPORTED_OBSL_MINOR` in `src/orionbelt_runner/client.py`):

```bash
# from the runner repo root, with the OBSL clone checked out to the matching tag
cp ../orionbelt-semantic-layer/schema/obml-schema.json schema/obml-schema.json
```

Then update the "OBSL version" line above.
