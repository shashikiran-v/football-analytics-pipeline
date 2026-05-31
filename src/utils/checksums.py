"""
File and schema checksums.

Two kinds of fingerprints used by the audit layer:

  1. file_checksum_md5(path)
        Streaming MD5 of file bytes. Constant memory regardless of size.
        Used by audit.register_file to detect:
          - duplicate vendor resends (same checksum across batches)
          - bytes-level changes when a filename appears stable

  2. schema_version_hash(columns_and_types)
        Deterministic hash of a source's column-name + column-type pairs.
        Used by audit.record_schema_drift to detect:
          - new columns added by the vendor
          - existing columns whose declared type changed

Both use MD5: cryptographically broken but fine for non-adversarial
change detection. We already use MD5 for SCD2 row hashing, so this
keeps the hashing primitive consistent across the codebase.

Streaming chunk size of 64KB is the sweet spot on modern CPUs — large
enough to amortise the Python loop overhead, small enough that even a
laptop with 4GB RAM never feels it.
"""

from __future__ import annotations

import hashlib
from pathlib import Path


# 64 KiB. Empirically the throughput plateau on commodity hardware; bigger
# chunks don't go faster, smaller chunks waste cycles in the Python loop.
_DEFAULT_CHUNK_BYTES = 64 * 1024


class ChecksumError(Exception):
    """Raised when a checksum operation cannot complete (e.g. file missing)."""


def file_checksum_md5(
    path: str | Path,
    *,
    chunk_size: int = _DEFAULT_CHUNK_BYTES,
) -> str:
    """
    Compute the MD5 of a file's contents by streaming, returning the
    32-char lowercase hex digest.

    Args:
        path: Path to the file. Must exist and be readable.
        chunk_size: Bytes per read. The default is good; only override
                    for benchmarking or testing.

    Raises:
        ChecksumError: if the file does not exist or cannot be read.
    """
    p = Path(path)
    if not p.is_file():
        # Wrap rather than letting FileNotFoundError leak so callers
        # have a single exception type for all checksum failures.
        raise ChecksumError(f"Cannot checksum non-file path: {p}")

    md5 = hashlib.md5()
    try:
        with p.open("rb") as f:
            # `iter(callable, sentinel)` is the idiomatic way to stream
            # a file in fixed-size chunks until EOF (empty bytes).
            for chunk in iter(lambda: f.read(chunk_size), b""):
                md5.update(chunk)
    except OSError as e:
        raise ChecksumError(f"Failed to read {p}: {e}") from e
    return md5.hexdigest()


def schema_version_hash(columns_and_types: dict[str, str]) -> str:
    """
    Deterministic hash of a (column_name -> type_tag) mapping.

    Sorted by column name before hashing so that the same logical
    schema always produces the same digest regardless of the order
    columns are declared in YAML or returned by a reader.

    Args:
        columns_and_types: e.g. {"player_id": "int", "name": "string"}.
                           Type tags are the same vocabulary used by
                           the engine and source registry.

    Returns:
        32-char lowercase hex digest. Empty mapping yields the MD5 of
        the empty string (d41d8cd98f00b204e9800998ecf8427e) — which is
        itself a valid, recognisable sentinel for "no columns".
    """
    # The wire format we hash is the canonical form: "col:type" pairs,
    # newline-separated, sorted by column. Any equivalent schema
    # produces an identical byte sequence and therefore an identical hash.
    items = sorted(columns_and_types.items())
    wire = "\n".join(f"{name}:{type_tag}" for name, type_tag in items)
    return hashlib.md5(wire.encode("utf-8")).hexdigest()
