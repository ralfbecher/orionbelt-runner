"""Tests for the Parquet / Arrow exporters and the folder / S3 sink layer."""

from __future__ import annotations

import io
from collections.abc import Callable
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from orionbelt_runner.client import ExecuteResult
from orionbelt_runner.exports import (
    build_arrow_table,
    render_arrow_ipc,
    render_export,
    render_parquet,
    safe_export_filename,
)
from orionbelt_runner.sinks import LocalSink, S3Sink, open_sink
from orionbelt_runner.spec import ExportTarget


def _result() -> ExecuteResult:
    """A raw (format_values=false) result: native ints / floats / ISO stamps."""
    return ExecuteResult(
        sql="SELECT 1",
        dialect="postgres",
        columns=[
            {"name": "country", "type": "string"},
            {"name": "orders", "type": "number"},
            {"name": "revenue", "type": "number"},
            {"name": "as_of", "type": "datetime"},
        ],
        rows=[
            ["DE", 12, 5000.5, "2026-04-29T00:00:00"],
            ["US", 7, 7345.25, "2026-04-30T12:30:00"],
        ],
        row_count=2,
    )


# -- table construction ----------------------------------------------------


def test_build_arrow_table_infers_native_types() -> None:
    table = build_arrow_table(_result())
    assert table.num_rows == 2
    assert table.column_names == ["country", "orders", "revenue", "as_of"]
    schema = {f.name: f.type for f in table.schema}
    assert schema["country"] == pa.string()
    assert schema["orders"] == pa.int64()
    assert schema["revenue"] == pa.float64()
    assert schema["as_of"] == pa.timestamp("us")
    assert table.column("revenue").to_pylist() == [5000.5, 7345.25]


def test_build_arrow_table_keeps_nulls() -> None:
    result = ExecuteResult(
        sql="SELECT 1",
        dialect="postgres",
        columns=[{"name": "country", "type": "string"}, {"name": "revenue", "type": "number"}],
        rows=[["DE", None], [None, 3.5]],
        row_count=2,
    )
    table = build_arrow_table(result)
    assert table.column("revenue").to_pylist() == [None, 3.5]
    assert table.column("country").to_pylist() == ["DE", None]


def test_build_arrow_table_falls_back_to_string_for_unconvertible_cells() -> None:
    """A column pyarrow can't type must degrade to string, not fail the export."""
    result = ExecuteResult(
        sql="SELECT 1",
        dialect="postgres",
        columns=[{"name": "payload", "type": "string"}],
        rows=[[{"a": 1}], ["plain"]],
        row_count=2,
    )
    table = build_arrow_table(result)
    assert table.schema.field("payload").type == pa.string()
    assert table.column("payload").to_pylist() == ["{'a': 1}", "plain"]


def test_build_arrow_table_datetime_hint_falls_back_when_unparseable() -> None:
    result = ExecuteResult(
        sql="SELECT 1",
        dialect="postgres",
        columns=[{"name": "as_of", "type": "datetime"}],
        rows=[["not-a-timestamp"]],
        row_count=1,
    )
    table = build_arrow_table(result)
    assert table.schema.field("as_of").type == pa.string()


def test_build_arrow_table_with_no_rows_keeps_columns() -> None:
    result = ExecuteResult(
        sql="SELECT 1",
        dialect="postgres",
        columns=[{"name": "country", "type": "string"}, {"name": "revenue", "type": "number"}],
        rows=[],
        row_count=0,
    )
    table = build_arrow_table(result)
    assert table.num_rows == 0
    assert table.column_names == ["country", "revenue"]


# -- file bodies -----------------------------------------------------------


def test_render_parquet_roundtrips() -> None:
    body = render_parquet(_result())
    assert body[:4] == b"PAR1"
    table = pq.read_table(pa.BufferReader(body))
    assert table.column("country").to_pylist() == ["DE", "US"]
    assert table.schema.field("revenue").type == pa.float64()


@pytest.mark.parametrize("compression", ["default", "none", "gzip", "zstd"])
def test_render_parquet_compression_codecs(compression: str) -> None:
    body = render_parquet(_result(), compression=compression)
    table = pq.read_table(pa.BufferReader(body))
    assert table.num_rows == 2


def test_render_arrow_ipc_roundtrips() -> None:
    body = render_arrow_ipc(_result())
    assert body[:6] == b"ARROW1"  # IPC *file* format, not the stream format
    with pa.ipc.open_file(pa.BufferReader(body)) as reader:
        table = reader.read_all()
    assert table.column("orders").to_pylist() == [12, 7]


def test_render_arrow_ipc_zstd() -> None:
    body = render_arrow_ipc(_result(), compression="zstd")
    with pa.ipc.open_file(pa.BufferReader(body)) as reader:
        assert reader.read_all().num_rows == 2


def test_render_export_dispatches_by_format() -> None:
    assert render_export(_result(), fmt="parquet")[:4] == b"PAR1"
    assert render_export(_result(), fmt="arrow")[:6] == b"ARROW1"
    assert render_export(_result(), fmt="tsv").startswith(b"country\torders")
    with pytest.raises(ValueError, match="Unknown export format"):
        render_export(_result(), fmt="orc")


def test_safe_export_filename_honours_extension() -> None:
    assert safe_export_filename("orders/by_country", extension="parquet") == (
        "orders_by_country.parquet"
    )
    assert safe_export_filename("total") == "total.tsv"


# -- spec validation -------------------------------------------------------


def test_export_target_rejects_codec_the_format_cannot_use() -> None:
    with pytest.raises(ValueError, match="not valid for format 'arrow'"):
        ExportTarget(format="arrow", uri="./out", compression="snappy")


def test_export_target_needs_raw_values() -> None:
    assert ExportTarget(format="parquet", uri="./out").needs_raw_values
    assert ExportTarget(format="arrow", uri="./out").needs_raw_values
    assert not ExportTarget(format="tsv", uri="./out").needs_raw_values


# -- sinks -----------------------------------------------------------------


def test_open_sink_local_relative_path_rebased_under_output_dir(tmp_path: Path) -> None:
    sink = open_sink("data/exports", base_dir=tmp_path)
    assert isinstance(sink, LocalSink)
    assert sink.path == tmp_path / "data/exports"


def test_open_sink_local_absolute_path_ignores_output_dir(tmp_path: Path) -> None:
    sink = open_sink(str(tmp_path / "abs"), base_dir=Path("/elsewhere"))
    assert isinstance(sink, LocalSink)
    assert sink.path == tmp_path / "abs"


def test_open_sink_file_url(tmp_path: Path) -> None:
    sink = open_sink(f"file://{tmp_path}/out")
    assert isinstance(sink, LocalSink)
    assert sink.path == Path(f"{tmp_path}/out")


def test_local_sink_creates_dirs_and_writes(tmp_path: Path) -> None:
    sink = open_sink("nested/deeper", base_dir=tmp_path)
    written = sink.write("total.parquet", b"PAR1data")
    assert Path(written) == tmp_path / "nested/deeper/total.parquet"
    assert (tmp_path / "nested/deeper/total.parquet").read_bytes() == b"PAR1data"


def test_open_sink_rejects_unknown_scheme() -> None:
    with pytest.raises(ValueError, match="Unsupported export URI scheme 'gs'"):
        open_sink("gs://bucket/prefix")


def test_open_sink_rejects_bucketless_s3_uri() -> None:
    with pytest.raises(ValueError, match="missing a bucket"):
        open_sink("s3:///just-a-path")


class _FakeStream(io.BytesIO):
    def __init__(self, on_close: Callable[[bytes], None]) -> None:
        super().__init__()
        self._on_close = on_close

    def __enter__(self) -> _FakeStream:
        return self

    def __exit__(self, *exc: object) -> None:
        self._on_close(self.getvalue())


class _FakeFs:
    """Stands in for pyarrow.fs.S3FileSystem — records what was written where."""

    def __init__(self) -> None:
        self.written: dict[str, bytes] = {}

    def open_output_stream(self, path: str) -> _FakeStream:
        return _FakeStream(lambda data: self.written.__setitem__(path, data))


def test_s3_sink_writes_to_bucket_prefixed_key() -> None:
    """S3Sink must join bucket + prefix + filename and never call create_dir."""
    fs = _FakeFs()
    sink = S3Sink(fs, "my-bucket/reports/2026-04-29", location="s3://my-bucket/reports/2026-04-29")
    location = sink.write("total.parquet", b"PAR1")
    assert location == "s3://my-bucket/reports/2026-04-29/total.parquet"
    assert fs.written == {"my-bucket/reports/2026-04-29/total.parquet": b"PAR1"}


def test_s3_sink_built_from_uri_without_network(monkeypatch: pytest.MonkeyPatch) -> None:
    """open_sink must parse the URI into bucket/prefix and pass endpoint+region on."""
    captured: dict[str, object] = {}

    class FakeS3FileSystem:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    from pyarrow import fs as pa_fs

    monkeypatch.setattr(pa_fs, "S3FileSystem", FakeS3FileSystem)
    sink = open_sink(
        "s3://my-bucket/reports/2026-04-29/",
        endpoint_override="http://localhost:9000",
        region="eu-central-1",
    )
    assert isinstance(sink, S3Sink)
    assert sink.location == "s3://my-bucket/reports/2026-04-29"
    # A full URL endpoint is split into host + scheme, which is what
    # S3FileSystem expects (MinIO / R2 docs all give the full URL).
    assert captured == {
        "region": "eu-central-1",
        "endpoint_override": "localhost:9000",
        "scheme": "http",
    }


def test_s3_endpoint_falls_back_to_env(monkeypatch: pytest.MonkeyPatch) -> None:
    captured: dict[str, object] = {}

    class FakeS3FileSystem:
        def __init__(self, **kwargs: object) -> None:
            captured.update(kwargs)

    from pyarrow import fs as pa_fs

    monkeypatch.setattr(pa_fs, "S3FileSystem", FakeS3FileSystem)
    monkeypatch.setenv("AWS_ENDPOINT_URL", "https://accountid.r2.cloudflarestorage.com")
    sink = open_sink("s3://bucket")
    assert isinstance(sink, S3Sink)
    assert captured["endpoint_override"] == "accountid.r2.cloudflarestorage.com"
    assert captured["scheme"] == "https"
