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
./scripts/check-action-pins.sh                          # after editing a workflow
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

scripts/
└── third_party_notices.py   # regenerates / verifies THIRD-PARTY-NOTICES.md
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

The runner declares an OBSL **floor**, not an exact pairing: it runs against that release or anything newer, and is developed against the latest. `HttpObslClient.preflight()` calls the unauthenticated `GET /health` (which returns the OBSL release `version` and the active `auth_mode`) before any query. Below the floor raises `ObslVersionError`; above the newest tested minor logs `obsl_version_newer_than_tested` and proceeds; `ObslPreflightError` if the server enforces `AUTH_MODE=api_key` but no key was configured. The CLI runs preflight automatically (skippable with `--skip-preflight`, though that disables the auth check too).

The floor lives in `client.py` as `MINIMUM_OBSL_MAJOR` / `MINIMUM_OBSL_MINOR` — raise it only when the runner starts *depending* on something new, not every time OBSL ships a minor. `TESTED_OBSL_MAJOR` / `TESTED_OBSL_MINOR` record the newest minor exercised in CI and decide the warning wording only; bump them with the vendored schema when adopting a release. This replaced an exact-equality gate that pinned the runner to one OBSL minor and blocked any model written against a newer authoring surface until the runner cut a release of its own.

Note: `GET /v1/settings` also exposes `version` (release) plus `api_version` (the REST prefix, currently `"v1"` — *not* a semver). The runner still reads `settings()` mid-run to capture `version` / `api_version` into the run log, but the version *gate* is the `/health` preflight.

## Releasing

A release is a tag. `pyproject.toml`, `src/orionbelt_runner/__init__.py` and
`uv.lock` carry the version; bump all three, merge, then tag `vX.Y.Z` on `main`.
Pushing that tag fans out to two workflows: `docker-publish.yml` builds and pushes
`:X.Y.Z`, `:X.Y`, `:X` and `:latest`, and `pypi-publish.yml` uploads the sdist and
wheel. Both are tag-gated and never fire on a branch push.

PyPI uses Trusted Publishing, so there is no token: the `pypi` GitHub
environment is what the identity binds to, and renaming it breaks the upload
until the publisher on PyPI is renamed to match. A manual run publishes the
current ref, which is how a version tagged before the workflow existed gets
uploaded — dispatch against `main` in that case, since the workflow file does not
exist at the older tag.

`pypi-publish.yml` is two jobs. `build` checks out the tree, verifies the tag
against both version strings, builds the sdist and wheel and runs `twine check`,
all with no `id-token`; `publish` downloads that artifact and uploads it, and is
the only job holding the identity PyPI accepts. The split is the security
boundary rather than a staging convenience: `uv build` invokes the build backend,
which resolves and executes whatever `pyproject.toml` names, and none of that
should be able to mint a token good for uploading this project. PyPI matches its
publisher on the workflow file and the environment, not the job, so `pypi` sits
on `publish` and the binding above is unaffected.

The version gate runs first, in `build`. That check exists because the drift has
happened: 0.9.0 sat in `pyproject.toml` through
several commits with no tag behind it, so v0.8.1 stayed the newest release while
two user-facing changes went unshipped. A tag that disagrees with what it
packages is worse than a missing one.

## Workflow Actions

Every Action the three workflows use is pinned to a commit SHA rather than a
version tag, because a tag is a movable label its owner can repoint at new code
at any time. A SHA is unreadable, though, so the `# vX.Y.Z` comment beside it is
the only part a reviewer actually reads, and nothing makes the two agree.

That gap is what `scripts/check-action-pins.sh` closes. It resolves each
comment's tag upstream with `git ls-remote` and fails when the SHA pinned in the
workflow is not the commit that tag names, so a hash quietly swapped for one
taken from a fork stops looking like a routine Dependabot bump — forks share
object storage with their parent, so such a commit is reachable under the real
repository's URL too, and only resolving the tag tells the two apart. It also
rejects any Action that is not SHA-pinned, any owner outside `ALLOWED_OWNERS`,
and any container action not pinned by digest, since an image tag such as
`:latest` moves just as a git tag does.

`ALLOWED_OWNERS` carries the most weight of the three. A SHA matching its own tag
says nothing about whether the Action belongs here, because a hostile
repository's tags verify against themselves perfectly well. Adding an owner is a
deliberate edit to the script, reviewed as such.

Comments must name exact patch releases. A major tag such as `v7` moves with
every upstream release, so checking against it would turn CI red the moment
`v7.0.2` ships for a pin that is still good, which is a dependency on exactly the
mutable pointer that pinning was meant to escape. To bump an Action, resolve the
release and paste both halves:

```bash
git ls-remote https://github.com/actions/checkout 'refs/tags/v7.0.1^{}'
```

The check runs as the first step of CI's `check` job — the one context branch
protection requires — and as the first step after checkout in both publishing
workflows, since those are the ones holding the Docker Hub credentials and the
PyPI identity. `pypi-publish.yml`'s `publish` job needs no copy: it cannot start
until `build` has passed, so its Actions are verified before they exist as
running code. `--offline` skips the upstream lookups and checks only SHA and
comment format.

Permissions are scoped the same way. Each workflow declares `contents: read` at
the top, and the one job that needs more says so itself. Nothing here needs a
writable `GITHUB_TOKEN`: Docker Hub is authenticated by `DOCKERHUB_TOKEN` and
PyPI by OIDC.

What this does **not** do, since a pin check is easy to over-trust: it does not
judge whether an Action is safe, only that the hash matches its own label; it
does not stop a downgrade to a real but ancient release; and `actions/checkout`
runs before the check and is therefore unverified, which is an irreducible
bootstrap dependency rather than an oversight.

## Third-party licenses

`THIRD-PARTY-NOTICES.md` is **generated** — edit `scripts/third_party_notices.py`,
never the file. It derives the closure from `uv.lock` (runtime + `arrow` + `pdf`;
`dev` is tooling the project runs, not a work it ships) and pulls each license text
from the wheel's own `*.dist-info/licenses/`, so the notice matches what is
installed rather than a hand-kept list.

Two scopes, kept distinct because conflating them makes the file claim things that
are not true: the *noticed* closure is everything the project can pull in, while
the *redistributed* set is only what the Dockerfile installs, parsed live by
`dockerfile_extras()` (comments dropped, continuations joined, `--extra=x` and
`--all-extras` / `--no-extra` handled — this is the seam the pyphen guard hangs
off, so it must not be defeated by reformatting a `RUN` line). Its dangerous
failure is an *empty* answer rather than a wrong one: nothing would be marked as
redistributed and the election check would not fire, while the notice still read
as verified. So an install command it does not understand raises instead — if you
change how the image installs, teach the parser first. Packages outside the image are still credited, marked
`no` in the summary, and described as informational. Install a new extra in the
image and the run fails until `NOTICED_EXTRAS` accounts for it.

The **Platform layer** section of that file covers what `uv.lock` cannot see — the
interpreter and the Debian userland the image inherits. It is prose in the
script's `PLATFORM_SECTION`, parameterised from the Dockerfile's
`FROM python:<version> AS runtime` line, so bumping the base image updates the
notice instead of silently invalidating it. Change that line's shape and
`runtime_base_image()` will fail loudly rather than emit a stale claim.

CI runs it with `--check`, which fails on drift **and** on any dependency whose
license is not permissive and not in the script's `ACKNOWLEDGED` map — where an
entry records the exact license expression that was reviewed, so a package that
relicenses fails rather than inheriting an approval granted to different terms. That gate is
the point: adding a copyleft dependency should be a decision someone writes down,
not something that arrives with a Dependabot bump. When it fires, either drop the
dependency or add an entry explaining why it is acceptable (see `certifi` and
`pyphen` for the shape). pyphen carries a second guard: it offers a choice of
three licenses, and choosing one only becomes necessary once we redistribute it.
The image installs `--extra pdf`, so it does, and `PYPHEN_ELECTION` records the
answer — **LGPL-2.1-or-later**, the arrangement for a library imported unmodified
from `site-packages`. `enforce_pyphen_election()` fails the build in both
directions: adding the extra without recording an election, and leaving an
election recorded after the extra is dropped. The second is not tidiness — the
elected wording states that the image hands over a copy of pyphen, so a stale
election puts a false sentence in a licence document.

A wheel that ships no license file gets a hand-vendored one in
`scripts/license-overrides/`, with its provenance recorded in the file. Anything
so vendored is called out in the notice as the only copy of those terms inside the
image, since the package's own `dist-info` carries none.

## Out of scope (for now)

- Scheduling — drive from cron / systemd / Cloud Scheduler / GitHub Actions
- Chart generation — landing later, likely via OrionBelt Analytics
- Multi-model session orchestration — supported via `model_id` only
- Non-S3 object stores (GCS, Azure) and partitioned / append-mode dataset writes — one file per query per target today
- Per-target query filtering — every target exports every query

When any of these arrive, keep them behind the same `ObslClient` boundary or add a sibling module — do not couple them into `runner.py` directly.
