"""Per-query export rendering — TSV, Parquet, and Arrow IPC.

Pure functions: each ``render_*`` returns the file body; the runner is the
only place that writes files (locally or to S3, via :mod:`sinks`).

**TSV** uses Python's ``excel-tab`` dialect with ``QUOTE_MINIMAL`` so
embedded tabs / newlines / quotes are handled the same way
``pandas.read_csv(..., sep='\\t')`` expects. Cell values come straight from
the ``ExecuteResult`` rows; because the runner calls OBSL with
``format_values=True`` for the report, numeric and date cells are already
locale-formatted strings — TSV therefore mirrors what the report shows.
``None`` becomes an empty cell.

**Parquet / Arrow** are typed formats, and get their rows from OBSL's
Arrow transport (``?format=arrow``): the server sends a real Arrow table,
so ``decimal128`` / ``timestamp`` / ``int64`` are written through
untouched — see :func:`to_arrow_table`.

:func:`build_arrow_table` is the fallback for rows that only exist as JSON
(a deployment that ignored ``format=arrow``). It infers types from the
values, using OBSL's per-column ``type`` hint to pin datetime, binary, and
``decimal(p, s)`` columns — the last of those matters because OBSL delivers
DECIMAL cells as exact strings, which would otherwise infer as text.

Both formats need the optional ``arrow`` extra (``uv sync --extra arrow``).
"""

from __future__ import annotations

import csv
import io
import re
from typing import Any

from orionbelt_runner.arrow_support import (
    MissingArrowDependencyError,  # re-exported: historical import site
    require_pyarrow,
)
from orionbelt_runner.client import ArrowResult, ColumnMetadata, ExecuteResult

__all__ = [
    "FORMAT_EXTENSIONS",
    "MissingArrowDependencyError",
    "build_arrow_table",
    "render_arrow_ipc",
    "render_export",
    "render_parquet",
    "render_tsv",
    "safe_export_filename",
    "to_arrow_table",
]

# OBSL reports governed DECIMAL columns as e.g. "decimal(18, 2)" (scale is
# optional in the grammar, so it defaults to 0 when absent).
_DECIMAL_HINT = re.compile(r"decimal\s*\(\s*(\d+)\s*(?:,\s*(\d+)\s*)?\)")

# Extension per export format — also the lookup the runner uses to name files.
FORMAT_EXTENSIONS: dict[str, str] = {
    "tsv": "tsv",
    "parquet": "parquet",
    "arrow": "arrow",
}


def render_tsv(result: ExecuteResult) -> str:
    """Render an ``ExecuteResult`` as TSV (header row + one row per result row)."""
    buf = io.StringIO()
    # Override excel-tab's default \r\n line terminator — TSVs are typically
    # consumed on unix-y stacks (pandas / awk / DuckDB) and \n is friendlier.
    writer = csv.writer(buf, dialect="excel-tab", lineterminator="\n")
    writer.writerow([c.name for c in result.columns])
    for row in result.rows:
        writer.writerow([_cell(v) for v in row])
    return buf.getvalue()


def _cell(value: Any) -> str:
    if value is None:
        return ""
    return str(value)


def to_arrow_table(source: ExecuteResult | ArrowResult) -> Any:
    """Get a ``pyarrow.Table`` for ``source``, whichever shape it arrived in.

    An :class:`ArrowResult` from the ``format=arrow`` transport already holds
    the server's typed table — it's passed through untouched, so DECIMAL stays
    ``decimal128``, timestamps keep their zone, and no JSON scalar is ever
    re-inferred. Everything else (a JSON ``ExecuteResult``, or an
    ``ArrowResult`` from a server that ignored ``format=arrow``) goes through
    :func:`build_arrow_table`.
    """
    if isinstance(source, ArrowResult):
        return source.table if source.table is not None else build_arrow_table(source.meta)
    return build_arrow_table(source)


def build_arrow_table(result: ExecuteResult) -> Any:
    """Build a ``pyarrow.Table`` from an ``ExecuteResult``.

    Rows arrive column-major-agnostic (a list of row lists), so each column is
    transposed and converted independently. Type resolution per column:

    * ``datetime`` / ``binary`` per OBSL's column metadata → converted to
      ``timestamp[us]`` / ``binary`` when every value parses.
    * everything else → pyarrow's own inference (``int64``, ``double``,
      ``bool``, ``string``, …).
    * anything that fails to convert → ``string`` via ``str(value)``, so a
      surprising cell degrades one column instead of failing the export.

    An all-``None`` column becomes pyarrow's ``null`` type, which both parquet
    and the IPC format store faithfully.
    """
    pa = require_pyarrow()
    names = [c.name for c in result.columns]
    columns: list[Any] = []
    for i, meta in enumerate(result.columns):
        values = [row[i] if i < len(row) else None for row in result.rows]
        columns.append(_arrow_column(pa, values, meta))
    return pa.Table.from_arrays(columns, names=names)


def _arrow_column(pa: Any, values: list[Any], meta: ColumnMetadata) -> Any:
    """Convert one column's values into a typed ``pyarrow.Array``."""
    hint = (meta.type or "").lower()
    inferred = _try_array(pa, values, None)

    if hint == "datetime" and inferred is not None:
        timestamps = _as_timestamps(pa, inferred)
        if timestamps is not None:
            return timestamps
    elif hint == "binary":
        typed = _try_array(pa, values, pa.binary())
        if typed is not None:
            return typed
    elif hint.startswith("decimal") and inferred is not None:
        decimals = _as_decimals(pa, inferred, hint)
        if decimals is not None:
            return decimals

    if inferred is not None:
        return inferred
    # Mixed / unconvertible cells (e.g. a dict in a JSON column): keep the data
    # rather than the type.
    return pa.array([None if v is None else str(v) for v in values], type=pa.string())


def _as_timestamps(pa: Any, array: Any) -> Any | None:
    """Cast an inferred column to ``timestamp[us]`` when it holds ISO strings.

    JSON has no timestamp type, so OBSL sends datetimes as ISO-8601 strings.
    Naive stamps (``2026-04-29T00:00:00``) cast to a zone-less
    ``timestamp[us]``; offset-carrying ones (``…+02:00``) only cast when the
    target type has a zone, so UTC is used as the second attempt — the
    instant is preserved either way. Returns ``None`` when neither works, so
    the caller can keep the column as-is.
    """
    if pa.types.is_timestamp(array.type):
        return array
    if not (pa.types.is_string(array.type) or pa.types.is_large_string(array.type)):
        return None
    for target in (pa.timestamp("us"), pa.timestamp("us", tz="UTC")):
        try:
            return array.cast(target)
        except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
            continue
    return None


def _as_decimals(pa: Any, array: Any, hint: str) -> Any | None:
    """Cast a column OBSL typed as ``decimal(p, s)`` to Arrow's decimal type.

    OBSL delivers governed DECIMAL cells in raw JSON as *exact decimal
    strings* rather than floats, so precision past float64's ~15-16
    significant digits survives the wire (OBSL issue #136). Inference alone
    would therefore land money columns in Parquet as ``string`` — correct
    values, useless schema — so the ``decimal(p, s)`` hint is honoured here.

    Precision above 38 digits needs ``decimal256``. Returns ``None`` when the
    hint is unparseable or a value won't fit, leaving the column as-is rather
    than losing data to a narrow target.
    """
    match = _DECIMAL_HINT.match(hint)
    if match is None:
        return None
    precision, scale = int(match.group(1)), int(match.group(2) or 0)
    if precision < 1:
        return None
    target = pa.decimal128(precision, scale) if precision <= 38 else pa.decimal256(precision, scale)
    try:
        return array.cast(target)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError):
        return None


def _try_array(pa: Any, values: list[Any], type_: Any) -> Any | None:
    try:
        return pa.array(values, type=type_)
    except (pa.ArrowInvalid, pa.ArrowTypeError, pa.ArrowNotImplementedError, ValueError):
        return None


def render_parquet(result: ExecuteResult | ArrowResult, *, compression: str = "default") -> bytes:
    """Render a result as a Parquet file body.

    ``compression`` is one of ``default`` (snappy), ``none``, ``snappy``,
    ``gzip``, ``brotli``, ``lz4``, ``zstd``.
    """
    pa = require_pyarrow()
    import pyarrow.parquet as pq

    table = to_arrow_table(result)
    codec = "snappy" if compression == "default" else compression
    buf = pa.BufferOutputStream()
    pq.write_table(table, buf, compression=codec)
    value: bytes = buf.getvalue().to_pybytes()
    return value


def render_arrow_ipc(result: ExecuteResult | ArrowResult, *, compression: str = "default") -> bytes:
    """Render a result as an Arrow IPC *file* (Feather V2) body.

    The random-access file format (not the streaming one) so consumers can
    memory-map it. ``compression`` is one of ``default`` (uncompressed),
    ``none``, ``lz4``, ``zstd``.
    """
    pa = require_pyarrow()

    table = to_arrow_table(result)
    codec = None if compression in {"default", "none"} else compression
    options = pa.ipc.IpcWriteOptions(compression=codec)
    buf = pa.BufferOutputStream()
    with pa.ipc.new_file(buf, table.schema, options=options) as writer:
        writer.write_table(table)
    value: bytes = buf.getvalue().to_pybytes()
    return value


def render_export(
    result: ExecuteResult | ArrowResult, *, fmt: str, compression: str = "default"
) -> bytes:
    """Render ``result`` in ``fmt``, returning the file body as bytes.

    ``tsv`` is only ever rendered from a JSON ``ExecuteResult`` — it mirrors the
    report's formatted cells, which the Arrow transport deliberately doesn't
    carry.
    """
    if fmt == "tsv":
        if isinstance(result, ArrowResult):
            raise TypeError("tsv exports render from formatted rows, not the Arrow transport")
        return render_tsv(result).encode("utf-8")
    if fmt == "parquet":
        return render_parquet(result, compression=compression)
    if fmt == "arrow":
        return render_arrow_ipc(result, compression=compression)
    raise ValueError(f"Unknown export format: {fmt!r}")


# Filenames are derived from query names. Strip anything that isn't a safe
# path char so the runner can't accidentally write outside the exports dir
# when a spec uses an unusual query name (e.g. one containing ``/``).
_UNSAFE_FILENAME = re.compile(r"[^A-Za-z0-9._-]+")


def safe_export_filename(query_name: str, *, extension: str = "tsv") -> str:
    """Return a filesystem-safe export filename for a query.

    Replaces any non-``[A-Za-z0-9._-]`` run with a single underscore so that
    a query named ``orders/by_country`` never escapes the exports directory.
    Empty results after sanitisation fall back to ``query``.
    """
    sanitized = _UNSAFE_FILENAME.sub("_", query_name).strip("._-")
    return f"{sanitized or 'query'}.{extension}"
