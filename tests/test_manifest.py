"""
Tests for src.ingestion.manifest.

Covers:
  - Happy path: well-formed manifest loads into a typed object
  - Missing _manifest.json returns None (legitimate path, not an error)
  - Malformed JSON raises ManifestError with a helpful message
  - Unsupported manifest_version raises ManifestError
  - Missing required fields raise ManifestError (via pydantic)
  - Unknown extra fields raise ManifestError (strict-on-extras)
  - file_entry() helper returns the right record or None
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.ingestion.manifest import (
    MANIFEST_FILENAME,
    Manifest,
    ManifestError,
    load_manifest,
)

# ---------------------------------------------------------------------------
# Fixture builders
# ---------------------------------------------------------------------------


def _valid_manifest_dict() -> dict:
    """A minimal but valid manifest. Tests mutate copies of this."""
    return {
        "manifest_version": 1,
        "vendor": "kaggle",
        "dataset": "davidcariboo/player-scores",
        "dataset_version": 47,
        "vendor_last_updated": "2025-05-21T09:34:17+00:00",
        "fetched_at": "2026-06-01T14:00:00+00:00",
        "files": {
            "players.csv": {
                "size_bytes": 47829431,
                "checksum_md5": "abc123def456" + "0" * 20,
            },
            "games.csv": {
                "size_bytes": 12345678,
                "checksum_md5": "ffeeddccbbaa" + "0" * 20,
            },
        },
    }


def _write_manifest(directory: Path, manifest_dict: dict | str) -> Path:
    """Write a manifest to the given directory. Accepts dict or raw str."""
    directory.mkdir(parents=True, exist_ok=True)
    path = directory / MANIFEST_FILENAME
    if isinstance(manifest_dict, dict):
        path.write_text(json.dumps(manifest_dict))
    else:
        path.write_text(manifest_dict)
    return path


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_loads_well_formed_manifest(self, tmp_path):
        _write_manifest(tmp_path, _valid_manifest_dict())
        manifest = load_manifest(tmp_path)
        assert isinstance(manifest, Manifest)
        assert manifest.vendor == "kaggle"
        assert manifest.dataset_version == 47
        assert manifest.vendor_last_updated == "2025-05-21T09:34:17+00:00"

    def test_file_entry_returns_existing_file(self, tmp_path):
        _write_manifest(tmp_path, _valid_manifest_dict())
        manifest = load_manifest(tmp_path)
        entry = manifest.file_entry("players.csv")
        assert entry is not None
        assert entry.size_bytes == 47829431

    def test_file_entry_returns_none_for_unknown_file(self, tmp_path):
        _write_manifest(tmp_path, _valid_manifest_dict())
        manifest = load_manifest(tmp_path)
        assert manifest.file_entry("nonexistent.csv") is None


# ---------------------------------------------------------------------------
# Missing manifest — legitimate "no vendor provenance" path
# ---------------------------------------------------------------------------


class TestMissingManifest:
    def test_no_manifest_file_returns_none(self, tmp_path):
        """
        Crucial: a missing manifest must return None, not raise. The
        audit layer handles None by recording vendor_timestamp_source
        ='filesystem_only'. If this returned an error, every
        unauthenticated reviewer running the pipeline on samples would
        get a stack trace.
        """
        assert load_manifest(tmp_path) is None

    def test_no_manifest_in_nonexistent_directory_returns_none(self, tmp_path):
        # Path that doesn't even exist on disk; still returns None.
        # We just need the path NOT to point at a real manifest.
        assert load_manifest(tmp_path / "does-not-exist") is None


# ---------------------------------------------------------------------------
# Malformed JSON
# ---------------------------------------------------------------------------


class TestMalformedJson:
    def test_invalid_json_raises_manifest_error(self, tmp_path):
        _write_manifest(tmp_path, "{not valid json")
        with pytest.raises(ManifestError, match="not valid JSON"):
            load_manifest(tmp_path)

    def test_json_not_an_object_raises_manifest_error(self, tmp_path):
        # An array at the top level is technically valid JSON but not
        # a valid manifest shape.
        _write_manifest(tmp_path, "[1, 2, 3]")
        with pytest.raises(ManifestError, match="must be a JSON object"):
            load_manifest(tmp_path)


# ---------------------------------------------------------------------------
# Version handling
# ---------------------------------------------------------------------------


class TestVersionHandling:
    def test_unsupported_version_raises(self, tmp_path):
        d = _valid_manifest_dict()
        d["manifest_version"] = 999  # not in SUPPORTED_MANIFEST_VERSIONS
        _write_manifest(tmp_path, d)
        with pytest.raises(ManifestError, match="unsupported version"):
            load_manifest(tmp_path)

    def test_missing_version_raises(self, tmp_path):
        d = _valid_manifest_dict()
        del d["manifest_version"]
        _write_manifest(tmp_path, d)
        with pytest.raises(ManifestError, match="unsupported version"):
            load_manifest(tmp_path)


# ---------------------------------------------------------------------------
# Schema enforcement via pydantic
# ---------------------------------------------------------------------------


class TestSchemaEnforcement:
    def test_missing_required_field_raises(self, tmp_path):
        d = _valid_manifest_dict()
        del d["vendor_last_updated"]
        _write_manifest(tmp_path, d)
        with pytest.raises(ManifestError, match="failed validation"):
            load_manifest(tmp_path)

    def test_unknown_top_level_field_raises(self, tmp_path):
        """Strict-on-extras: typos should fail loud, not silently pass."""
        d = _valid_manifest_dict()
        d["unknown_field"] = "oops"
        _write_manifest(tmp_path, d)
        with pytest.raises(ManifestError, match="failed validation"):
            load_manifest(tmp_path)

    def test_unknown_file_entry_field_raises(self, tmp_path):
        d = _valid_manifest_dict()
        d["files"]["players.csv"]["bogus_attribute"] = "oops"
        _write_manifest(tmp_path, d)
        with pytest.raises(ManifestError, match="failed validation"):
            load_manifest(tmp_path)
