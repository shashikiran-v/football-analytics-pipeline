"""
File loader.

The single chokepoint between "raw file on disk" and "validated data
plus audit metadata ready for Bronze." Bronze never calls pandas
directly, never opens a file, never computes a checksum — it calls
`load_source()` and gets a typed result.

This sits at the intersection of four Phase 1 + 2a modules:

    ingestion.registry  — gives us the SourceDefinition (schema, path)
    engines             — reads the CSV into an opaque DataFrame
    utils.checksums     — computes file and schema fingerprints
    ingestion.manifest  — reads vendor metadata if present

The loader is intentionally engine-agnostic. It receives a
DataFrameEngine instance and returns the engine's native DataFrame
type wrapped in a LoadResult. Bronze passes the engine through;
swapping pandas <-> spark requires no loader changes.

What the loader does NOT do:
  - Write anything (Bronze owns that)
  - Touch the audit DAO (Bronze orchestrates registration)
  - Apply transformations (Silver's responsibility)
  - Run DQ checks (the DQ task does that against Bronze data)

The loader's only job: turn a SourceDefinition + raw_root into a
LoadResult.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from src.engines.base import DataFrame, DataFrameEngine
from src.ingestion.manifest import get_manifest_for
from src.ingestion.registry import SourceDefinition
from src.utils.checksums import file_checksum_md5, schema_version_hash
from src.utils.logging import get_logger
from src.metadata.audit import FileFingerprint


log = get_logger(__name__)


# ---------------------------------------------------------------------------
# Result type
# ---------------------------------------------------------------------------


@dataclass(frozen=True)
class LoadResult:
    """
    Everything Bronze needs after a successful load.

    Fields:
      source_name:  echo of the source's logical name (for logging)
      source_file_path: the actual resolved path on disk
      dataframe:    the engine's native DataFrame, schema-coerced
      fingerprint:  FileFingerprint ready for audit.register_file()
      source_row_count: rows the engine successfully loaded
    """

    source_name: str
    source_file_path: str
    dataframe: DataFrame
    fingerprint: FileFingerprint
    source_row_count: int


class FileLoaderError(Exception):
    """Raised when a file cannot be loaded (missing, malformed, etc.)."""


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------


def load_source(
    *,
    source: SourceDefinition,
    raw_root: Path | str,
    engine: DataFrameEngine,
) -> LoadResult:
    """
    Load one source from disk into a LoadResult.

    Args:
        source:    the source definition from the registry
        raw_root:  the directory containing the source's file
                   (e.g. data/sample/ or data/day1/)
        engine:    the DataFrameEngine to read with

    Raises:
        FileLoaderError: if the file is missing or unreadable.
        Engine-specific errors propagate if schema coercion produces
        a fatal failure (rare; pandas usually coerces with NaN fallback).
    """
    raw_root = Path(raw_root)
    path = source.resolve_path(raw_root)
    if not path.is_file():
        raise FileLoaderError(
            f"Source file not found for source={source.name!r}: {path} "
            f"(raw_root={raw_root})"
        )

    log.info(
        "load_source_started",
        source_name=source.name,
        path=str(path),
        format=source.format,
    )

    # --- Read the data ----------------------------------------------------
    # Only csv is currently supported. SourceDefinition's pydantic
    # constraint already restricts format to {csv, parquet, json}, but
    # parquet and json aren't wired yet — guard explicitly so a future
    # YAML edit gets a clear error rather than a silent NotImplementedError.
    if source.format != "csv":
        raise FileLoaderError(
            f"Loader does not yet handle format={source.format!r} "
            f"(source={source.name!r}). Add support to file_loader.py."
        )

    dataframe = engine.read_csv(path, schema=source.schema_)
    row_count = engine.count(dataframe)

    # --- Compute the fingerprint -----------------------------------------
    file_md5 = file_checksum_md5(path)
    schema_hash = schema_version_hash(source.schema_)
    size_bytes = path.stat().st_size

    # Filesystem mtime — always known. We record in ISO8601 UTC.
    fs_mtime_dt = datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)
    fs_mtime_iso = fs_mtime_dt.isoformat(timespec="seconds")

    # --- Vendor metadata via manifest, if present ------------------------
    # We pass the str form to get_manifest_for() because lru_cache requires
    # hashable arguments and pathlib.Path equality is platform-fiddly.
    manifest = get_manifest_for(str(raw_root))
    vendor_mtime: str | None = None
    vendor_source: str | None = None
    if manifest is not None:
        # Verify the manifest's per-file checksum matches what we just
        # computed. A mismatch means the file changed since the manifest
        # was written — log a warning but trust our freshly-computed value.
        entry = manifest.file_entry(path.name)
        if entry is not None and entry.checksum_md5 != file_md5:
            log.warning(
                "manifest_checksum_mismatch",
                source_name=source.name,
                path=str(path),
                manifest_md5=entry.checksum_md5,
                computed_md5=file_md5,
                note="file changed since manifest was written; using computed checksum",
            )
        vendor_mtime = manifest.vendor_last_updated
        vendor_source = f"{manifest.vendor}_manifest"   # e.g. 'kaggle_manifest'

    fingerprint = FileFingerprint(
        path=path,
        size_bytes=size_bytes,
        checksum_md5=file_md5,
        schema_version_hash=schema_hash,
        source_modified_at_filesystem=fs_mtime_iso,
        source_modified_at_vendor=vendor_mtime,
        vendor_timestamp_source=vendor_source,
    )

    log.info(
        "load_source_finished",
        source_name=source.name,
        path=str(path),
        rows=row_count,
        size_bytes=size_bytes,
        file_md5=file_md5,
        schema_hash=schema_hash,
        vendor_timestamp_source=vendor_source or "filesystem_only",
    )

    return LoadResult(
        source_name=source.name,
        source_file_path=str(path),
        dataframe=dataframe,
        fingerprint=fingerprint,
        source_row_count=row_count,
    )
