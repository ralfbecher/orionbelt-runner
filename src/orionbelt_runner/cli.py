"""CLI entry point — ``orionbelt-runner run path/to/spec.yaml``."""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import Annotated

import structlog
import typer

from orionbelt_runner import __version__
from orionbelt_runner.client import HttpObslClient
from orionbelt_runner.runner import Runner
from orionbelt_runner.spec import load_spec

app = typer.Typer(
    name="orionbelt-runner",
    help="Run OBSL query batches and emit reports.",
    add_completion=False,
)
log = structlog.get_logger("orionbelt_runner")


@app.command()
def run(
    spec_path: Annotated[Path, typer.Argument(help="Path to a YAML run spec.")],
    output_dir: Annotated[
        Path | None,
        typer.Option("--output-dir", "-o", help="Resolve relative report paths under this dir."),
    ] = None,
    base_url: Annotated[
        str | None,
        typer.Option("--base-url", help="Override OBSL base URL (else from spec or env)."),
    ] = None,
) -> None:
    """Execute a run spec end-to-end and write its report."""
    spec = load_spec(spec_path)

    base = base_url or os.environ.get("OBSL_BASE_URL") or spec.obsl.base_url
    token = spec.obsl.api_token or os.environ.get("OBSL_API_TOKEN")
    timeout = spec.obsl.timeout_seconds

    with HttpObslClient(base, api_token=token, timeout_seconds=timeout) as client:
        runner = Runner(client)
        result = runner.run(spec, output_dir=output_dir)

    if not result.succeeded:
        for name, err in result.errors.items():
            typer.echo(f"  {name}: {err}", err=True)
        typer.echo(f"\n{len(result.errors)} query/queries failed", err=True)
        sys.exit(1)

    if result.report_path:
        typer.echo(f"Report written: {result.report_path}")


@app.command()
def version() -> None:
    """Print version and exit."""
    typer.echo(__version__)


def main() -> None:
    app()


if __name__ == "__main__":
    main()
