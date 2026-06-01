"""
Vendor manifest reader.

When the Kaggle fetcher (or any future vendor-aware fetcher) downloads
source files, it writes a sibling _manifest.json describing the
dataset's vendor-side metadata: when the vendor published this version,
which version number, which files belong to it, and what their fetched
checksums were.

This module is the READ side of that contract. The WRITE side lives in
scripts/seed_kaggle.py (so the Kaggle-specific knowledge stays there).
Phase 2b's file loader uses the reader to populate FileFingerprint's
vendor-timestamp fields when registering files with the audit DAO.

Manifest schema (version 1):

    {
      "manifest_version": 1,
      "vendor": "kaggle",
      "dataset": "davidcariboo/player-scores",
      "dataset_version": 47,
      "vendor_last_updated": "2025-05-21T09:34:17+00:00",
      "fetched_at": "2026-06-01T14:00:00+00:00",
      "files": {
        "players.csv": {
          "size_bytes": 47829431,
          "checksum_md5": "abc123..."
        },
        ...
      }
    }

Missing manifest is not an error — it's the "no vendor provenance
available, fall back to filesystem mtime" path. Callers check
`load_manifest(...) is None` and handle accordingly. The audit DAO
will emit a vendor_timestamp_unavailable event in that case (already
wired in Phase 2a).
"""

from __future__ import annotations

import json
from functools import lru_cache
from pathlib import Path

from pydantic import BaseModel, ConfigDict

from src.utils.logging import get_logger


log = get_logger(__name__)

# The conventional manifest filename, sibling to the source CSVs.
MANIFEST_FILENAME = "_manifest.json"

# Current schema version. The reader rejects manifests it can't understand.
SUPPORTED_MANIFEST_VERSIONS: frozenset[int] = frozenset({1})


# ---------------------------------------------------------------------------
# Typed models
# ---------------------------------------------------------------------------


class _Frozen(BaseModel):
    """Immutable, strict-on-extras base. Same pattern as elsewhere."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class ManifestFileEntry(_Frozen):
    """Per-file metadata recorded by the fetcher."""

    size_bytes: int
    checksum_md5: str


class Manifest(_Frozen):
    """The full vendor manifest."""

    manifest_version: int
    vendor: str                         # 'kaggle' | 'http' | ...
    dataset: str                        # vendor's identifier for the dataset
    dataset_version: int | str          # int for Kaggle, possibly str elsewhere
    vendor_last_updated: str            # ISO8601, the AUTHORITATIVE vendor ts
    fetched_at: str                     # ISO8601, when our fetcher ran
    files: dict[str, ManifestFileEntry]

    def file_entry(self, filename: str) -> ManifestFileEntry | None:
        """Return the entry for `filename`, or None if not in the manifest."""
        return self.files.get(filename)


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


class ManifestError(Exception):
    """Raised for unrecoverable manifest errors (bad JSON, version mismatch)."""


def load_manifest(directory: str | Path) -> Manifest | None:
    """
    Look for _manifest.json inside `directory` and return a typed Manifest.

    Returns None if the file does not exist (this is the legitimate
    "no vendor provenance" path; callers fall back to filesystem mtime).

    Raises:
        ManifestError: file exists but is malformed (bad JSON, unknown
                       manifest_version, missing required fields).
    """
    manifest_path = Path(directory) / MANIFEST_FILENAME
    if not manifest_path.is_file():
        return None

    try:
        raw = json.loads(manifest_path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as e:
        raise ManifestError(
            f"Manifest at {manifest_path} is not valid JSON: {e}"
        ) from e

    if not isinstance(raw, dict):
        raise ManifestError(
            f"Manifest at {manifest_path} must be a JSON object, got {type(raw).__name__}"
        )

    version = raw.get("manifest_version")
    if version not in SUPPORTED_MANIFEST_VERSIONS:
        raise ManifestError(
            f"Manifest at {manifest_path} has unsupported version "
            f"{version!r}; supported: {sorted(SUPPORTED_MANIFEST_VERSIONS)}"
        )

    try:
        manifest = Manifest.model_validate(raw)
    except Exception as e:
        # pydantic ValidationError wraps under here; the message is
        # readable but we add context about which file.
        raise ManifestError(
            f"Manifest at {manifest_path} failed validation: {e}"
        ) from e

    log.info(
        "manifest_loaded",
        path=str(manifest_path),
        vendor=manifest.vendor,
        dataset=manifest.dataset,
        dataset_version=manifest.dataset_version,
        vendor_last_updated=manifest.vendor_last_updated,
        file_count=len(manifest.files),
    )
    return manifest


@lru_cache(maxsize=8)
def get_manifest_for(directory: str) -> Manifest | None:
    """
    Cached lookup. Phase 2b's Bronze loader calls this once per source
    directory per run; lru_cache prevents re-reading the file for each
    source within a batch.

    Note the str (not Path) argument — lru_cache requires hashable keys,
    and pathlib.Path equality is fiddly across platforms. Callers stringify
    before calling.
    """
    return load_manifest(directory)
