# syntax=docker/dockerfile:1

# ── Build stage ───────────────────────────────────────────────────────────────
# Resolve dependencies and install the package into a self-contained venv with
# uv. Kept separate from the runtime image so build tooling never ships.
FROM python:3.14-slim AS build

# uv: fast, reproducible installs. Copied from the official distroless image.
# Pinned, not :latest — a floating uv would let the tool that installs uv.lock
# change without a commit, which would undercut the lockfile's guarantee.
COPY --from=ghcr.io/astral-sh/uv:0.11.28 /uv /uvx /bin/

ENV UV_COMPILE_BYTECODE=1 \
    UV_LINK_MODE=copy \
    UV_PYTHON_DOWNLOADS=0

WORKDIR /app

# Install dependencies first (cached layer) using only the manifests, so source
# edits don't bust the dependency cache. --frozen installs the committed
# uv.lock as-is: the image gets the exact versions CI tested, and the build
# never re-resolves (so it can't drift between builds of the same commit).
#
# Both shipping extras are bundled. Without them the corresponding targets fail
# at runtime with an "install the extra" error the image cannot act on, which is
# a poor answer for a container whose whole job is unattended scheduled runs.
#
#   arrow — pyarrow, for Parquet / Arrow exports and s3:// destinations (~120 MB)
#   pdf   — WeasyPrint, which additionally needs the Pango system libraries
#           installed in the runtime stage below
COPY pyproject.toml uv.lock README.md ./
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-install-project --no-dev --extra arrow --extra pdf

# Now install the project itself. --no-editable copies the package into the
# venv (rather than linking to /app/src), so the runtime stage needs only .venv.
#
# LICENSE comes along because pyproject's `license-files` resolves at build time:
# without it here, the wheel built inside this image would carry no license file
# and the installed dist-info would be the only one in site-packages missing one.
# Copied after the dependency sync above so editing it can't bust that cache.
COPY LICENSE ./
COPY src/ ./src/
RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev --no-editable --extra arrow --extra pdf

# ── Runtime stage ─────────────────────────────────────────────────────────────
FROM python:3.14-slim AS runtime

# WeasyPrint draws through Pango, which is a system library rather than a wheel —
# the reason the pdf extra used to stay out of this image. Cairo and GDK-Pixbuf
# are deliberately absent: WeasyPrint has written PDFs itself since v53 and reads
# images through Pillow, so the old three-library recipe is larger than needed.
#
# fonts-dejavu-core is not optional. With no font installed, Pango substitutes
# nothing and text renders as empty boxes — a PDF that looks broken rather than a
# run that fails, which is the worse of the two outcomes.
RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        libpango-1.0-0 \
        libpangoft2-1.0-0 \
        fonts-dejavu-core \
    && rm -rf /var/lib/apt/lists/*

# Run as a non-root user; reports are written under the working dir.
RUN useradd --create-home --uid 1000 runner

# Bring over the resolved virtualenv from the build stage.
COPY --from=build --chown=runner:runner /app/.venv /app/.venv

# The image redistributes every dependency as a binary, so the attribution has to
# travel with it. Each wheel's own license text already ships inside
# .venv/**/dist-info/licenses/; these two put the runner's license and the
# consolidated notices somewhere a person (or an audit) can actually find them.
COPY --chown=runner:runner LICENSE THIRD-PARTY-NOTICES.md /app/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    # Default OBSL endpoint; override at `docker run` time.
    OBSL_BASE_URL=http://localhost:8080

USER runner
WORKDIR /work

# Mount specs in and reports out, e.g.:
#   docker run --rm -v "$PWD/examples:/work/examples" -v "$PWD/reports:/work/reports" \
#     ralforion/orionbelt-runner run examples/monthly-revenue.yaml
ENTRYPOINT ["orionbelt-runner"]
CMD ["--help"]
