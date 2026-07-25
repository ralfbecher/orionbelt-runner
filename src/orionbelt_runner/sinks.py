"""Where exported bytes land — a local folder or an S3 prefix.

The runner renders export bodies as bytes (:mod:`exports`) and hands them to
a :class:`Sink`, which owns the write. Two implementations:

* :class:`LocalSink` — a directory on disk. Plain ``pathlib``; no extra
  dependency, so TSV exports to a folder stay zero-friction.
* :class:`S3Sink` — an ``s3://bucket/prefix`` target backed by
  ``pyarrow.fs.S3FileSystem``, which also covers S3-compatible stores
  (MinIO, Cloudflare R2, Ceph) via ``endpoint_override``. Needs the optional
  ``arrow`` extra.

**Credentials are never read from the run spec.** ``S3FileSystem`` uses the
standard AWS chain: ``AWS_ACCESS_KEY_ID`` / ``AWS_SECRET_ACCESS_KEY`` (+
``AWS_SESSION_TOKEN``), ``~/.aws/credentials``, or the instance / task /
IRSA role. Only the non-secret ``endpoint_override`` and ``region`` are
spec-configurable, so a spec file stays safe to commit.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Any, Protocol
from urllib.parse import urlparse

import structlog

from orionbelt_runner.exports import MissingArrowDependencyError

log = structlog.get_logger("orionbelt_runner.sinks")

S3_SCHEMES = {"s3", "s3a", "s3n"}


class Sink(Protocol):
    """A directory-like destination that accepts ``(filename, bytes)``."""

    @property
    def location(self) -> str:
        """Human-readable base location, for logs and the run summary."""
        ...

    def write(self, filename: str, data: bytes) -> str:
        """Write one file; return its full location."""
        ...


class LocalSink:
    """A local directory. Created (with parents) on first write."""

    def __init__(self, directory: Path) -> None:
        self._dir = directory

    @property
    def location(self) -> str:
        return str(self._dir)

    @property
    def path(self) -> Path:
        return self._dir

    def write(self, filename: str, data: bytes) -> str:
        self._dir.mkdir(parents=True, exist_ok=True)
        target = self._dir / filename
        target.write_bytes(data)
        return str(target)


class S3Sink:
    """An ``s3://bucket/prefix`` destination backed by ``pyarrow.fs``."""

    def __init__(self, filesystem: Any, base_path: str, *, location: str) -> None:
        self._fs = filesystem
        # pyarrow paths are "bucket/key" — no scheme, no leading slash.
        self._base = base_path.strip("/")
        self._location = location

    @property
    def location(self) -> str:
        return self._location

    def write(self, filename: str, data: bytes) -> str:
        key = f"{self._base}/{filename}" if self._base else filename
        # No create_dir(): S3 has no real directories, and calling it on a
        # bucket root would attempt a CreateBucket the credentials may not
        # (and should not) allow.
        with self._fs.open_output_stream(key) as stream:
            stream.write(data)
        return f"s3://{key}"


def open_sink(
    uri: str,
    *,
    endpoint_override: str | None = None,
    region: str | None = None,
    base_dir: Path | None = None,
) -> Sink:
    """Resolve an export URI into a :class:`Sink`.

    ``s3://`` (also ``s3a://`` / ``s3n://``) → :class:`S3Sink`. Everything
    else — a bare path or a ``file://`` URL — → :class:`LocalSink`, with
    relative paths resolved under ``base_dir`` (the CLI's ``--output-dir``)
    when one is given, matching how the report path is rebased.
    """
    parsed = urlparse(uri)
    scheme = parsed.scheme.lower()

    if scheme in S3_SCHEMES:
        return _open_s3_sink(uri, parsed, endpoint_override=endpoint_override, region=region)

    if scheme == "file":
        return LocalSink(Path(parsed.path))

    # A Windows drive letter ("C:/data") parses as scheme="c"; anything else
    # with a scheme is a destination we don't support yet.
    if scheme and len(scheme) > 1:
        raise ValueError(
            f"Unsupported export URI scheme {scheme!r} in {uri!r} — "
            "expected a local path or an s3:// URI."
        )

    path = Path(uri)
    if base_dir is not None and not path.is_absolute():
        path = base_dir / path
    return LocalSink(path)


def _open_s3_sink(
    uri: str,
    parsed: Any,
    *,
    endpoint_override: str | None,
    region: str | None,
) -> S3Sink:
    try:
        from pyarrow import fs as pa_fs
    except ModuleNotFoundError as exc:  # pragma: no cover - depends on install
        raise MissingArrowDependencyError(
            "S3 exports need pyarrow. Install the optional extra: "
            "`uv sync --extra arrow` (or `pip install orionbelt-runner[arrow]`)."
        ) from exc

    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"S3 export URI is missing a bucket: {uri!r}")
    prefix = parsed.path.strip("/")
    base_path = f"{bucket}/{prefix}" if prefix else bucket

    endpoint = endpoint_override or os.environ.get("AWS_ENDPOINT_URL")
    kwargs: dict[str, Any] = {}
    if region is not None:
        kwargs["region"] = region
    if endpoint:
        # S3FileSystem wants the host and the scheme separately; accept a full
        # URL ("http://localhost:9000") because that's what AWS_ENDPOINT_URL
        # and every MinIO doc uses.
        host, http_scheme = _split_endpoint(endpoint)
        kwargs["endpoint_override"] = host
        if http_scheme is not None:
            kwargs["scheme"] = http_scheme

    log.debug("s3_sink_open", bucket=bucket, prefix=prefix, endpoint=endpoint, region=region)
    filesystem = pa_fs.S3FileSystem(**kwargs)
    return S3Sink(filesystem, base_path, location=f"s3://{base_path}")


def _split_endpoint(endpoint: str) -> tuple[str, str | None]:
    """``http://localhost:9000`` → ``("localhost:9000", "http")``."""
    parsed = urlparse(endpoint)
    if parsed.scheme in {"http", "https"} and parsed.netloc:
        return parsed.netloc, parsed.scheme
    return endpoint, None
