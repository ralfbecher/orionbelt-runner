"""pyarrow availability guard, shared by the client, exporters, and sinks.

pyarrow is an optional dependency (``uv sync --extra arrow``): it powers the
Arrow result transport, the Parquet / Arrow file formats, and the S3 sink.
Everything imports it lazily through :func:`require_pyarrow` so a core install
— markdown / HTML reports, TSV exports to a folder — stays dependency-free and
any missing-extra failure names the install command instead of surfacing a bare
``ModuleNotFoundError``.

Lives in its own module so ``client.py`` and ``exports.py`` can both use the
guard without importing each other.
"""

from __future__ import annotations

from typing import Any


class MissingArrowDependencyError(RuntimeError):
    """An Arrow-backed feature was requested but pyarrow isn't installed."""


def require_pyarrow(feature: str = "Parquet / Arrow exports") -> Any:
    """Import and return ``pyarrow``, or raise with the install command."""
    try:
        import pyarrow as pa
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on install
        raise MissingArrowDependencyError(
            f"{feature} need pyarrow. Install the optional extra: "
            "`uv sync --extra arrow` (or `pip install orionbelt-runner[arrow]`)."
        ) from exc
    return pa
