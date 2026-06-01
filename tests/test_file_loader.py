"""
Tests for src.ingestion.file_loader.

Two layers of coverage:

  Unit tests use small synthetic CSVs written into tmp_path with their
  own minimal SourceDefinition. Fast, deterministic, every branch
  covered.

  Integration tests load the real committed data/sample/ CSVs through
  the registry. They double as a smoke test that all six committed
  sources can round-trip through the loader without error.
"""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from src.engines.pandas_engine import PandasEngine
from src.ingestion.file_loader import (
    FileLoaderError,
    LoadResult,
    load_source,
)
from src.ingestion.manifest import MANIFEST_FILENAME
from src.ingestion.registry import SourceDefinition, get_registry


# ---------------------------------------------------------------------------
# Helpers — build small synthetic sources for unit tests
# ---------------------------------------------------------------------------


def _minimal_source(name: str = "widgets") -> SourceDefinition:
    """A tiny SourceDefinition pointing at {raw_root}/<name>.csv."""
    return SourceDefinition(
        name=name,
        description=f"test source: {name}",
        format="csv",
        path_pattern="{raw_root}/" + name + ".csv",
        primary_key=["id"],
        schema={"id": "int", "label": "string", "price": "float"},
    )


def _write_csv(path: Path, header: str, rows: list[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(header + "\n" + "\n".join(rows) + "\n", encoding="utf-8")


def _write_manifest(directory: Path, *, file_entries: dict[str, dict]) -> None:
    """Write a minimal valid v1 manifest into `directory`."""
    payload = {
        "manifest_version": 1,
        "vendor": "kaggle",
        "dataset": "test/synthetic",
        "dataset_version": 1,
        "vendor_last_updated": "2025-05-21T09:34:17+00:00",
        "fetched_at": "2026-06-01T14:00:00+00:00",
        "files": file_entries,
    }
    (directory / MANIFEST_FILENAME).write_text(json.dumps(payload))


@pytest.fixture
def engine():
    """Plain Pandas engine for these tests; loader tests don't need
    cross-engine parametrisation."""
    return PandasEngine()


# ---------------------------------------------------------------------------
# Happy path
# ---------------------------------------------------------------------------


class TestHappyPath:
    def test_returns_load_result_with_expected_fields(self, tmp_path, engine):
        _write_csv(
            tmp_path / "widgets.csv",
            "id,label,price",
            ["1,red,9.99", "2,green,12.50", "3,blue,7.25"],
        )
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        assert isinstance(result, LoadResult)
        assert result.source_name == "widgets"
        assert result.source_file_path.endswith("widgets.csv")
        assert result.source_row_count == 3

    def test_dataframe_has_declared_schema_columns(self, tmp_path, engine):
        _write_csv(
            tmp_path / "widgets.csv",
            "id,label,price",
            ["1,red,9.99"],
        )
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        cols = engine.columns(result.dataframe)
        assert set(cols) == {"id", "label", "price"}

    def test_fingerprint_has_real_md5_and_size(self, tmp_path, engine):
        _write_csv(
            tmp_path / "widgets.csv",
            "id,label,price",
            ["1,red,9.99"],
        )
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        # MD5 is a 32-char lowercase hex string.
        assert len(result.fingerprint.checksum_md5) == 32
        assert result.fingerprint.checksum_md5 == result.fingerprint.checksum_md5.lower()
        # Size matches what we wrote.
        expected_size = (tmp_path / "widgets.csv").stat().st_size
        assert result.fingerprint.size_bytes == expected_size

    def test_fingerprint_has_schema_hash(self, tmp_path, engine):
        _write_csv(
            tmp_path / "widgets.csv",
            "id,label,price",
            ["1,x,1.0"],
        )
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        # Schema hash is also a 32-char hex string and deterministic
        assert len(result.fingerprint.schema_version_hash) == 32

    def test_fingerprint_has_filesystem_mtime(self, tmp_path, engine):
        _write_csv(
            tmp_path / "widgets.csv",
            "id,label,price",
            ["1,x,1.0"],
        )
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        # ISO8601 with timezone — at minimum starts with a 4-digit year.
        fs_ts = result.fingerprint.source_modified_at_filesystem
        assert fs_ts is not None
        assert fs_ts[:4].isdigit()
        assert "+" in fs_ts or fs_ts.endswith("Z")    # tz info present


# ---------------------------------------------------------------------------
# Vendor manifest branches
# ---------------------------------------------------------------------------


class TestManifestBranches:
    def test_no_manifest_yields_none_vendor_timestamp(self, tmp_path, engine):
        """The legitimate filesystem-only path. No manifest = vendor None."""
        _write_csv(
            tmp_path / "widgets.csv",
            "id,label,price",
            ["1,x,1.0"],
        )
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        assert result.fingerprint.source_modified_at_vendor is None
        assert result.fingerprint.vendor_timestamp_source is None

    def test_manifest_populates_vendor_timestamp(self, tmp_path, engine):
        """When a manifest matches the file, vendor metadata flows through."""
        csv_path = tmp_path / "widgets.csv"
        _write_csv(csv_path, "id,label,price", ["1,x,1.0"])

        from src.utils.checksums import file_checksum_md5
        from src.ingestion import manifest as manifest_mod

        # Clear the manifest reader's lru_cache so the fixture's
        # synthetic manifest is fresh-read for each test.
        manifest_mod.get_manifest_for.cache_clear()

        _write_manifest(
            tmp_path,
            file_entries={
                "widgets.csv": {
                    "size_bytes": csv_path.stat().st_size,
                    "checksum_md5": file_checksum_md5(csv_path),
                },
            },
        )

        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        assert result.fingerprint.source_modified_at_vendor == "2025-05-21T09:34:17+00:00"
        assert result.fingerprint.vendor_timestamp_source == "kaggle_manifest"

    def test_manifest_checksum_mismatch_does_not_fail(self, tmp_path, engine, caplog):
        """If the manifest's checksum doesn't match what we just hashed,
        we log a warning but still load the file — the freshly-computed
        checksum wins. (We never let a stale manifest break ingestion.)"""
        csv_path = tmp_path / "widgets.csv"
        _write_csv(csv_path, "id,label,price", ["1,x,1.0"])

        from src.ingestion import manifest as manifest_mod
        manifest_mod.get_manifest_for.cache_clear()

        _write_manifest(
            tmp_path,
            file_entries={
                "widgets.csv": {
                    "size_bytes": csv_path.stat().st_size,
                    "checksum_md5": "deadbeef" * 4,   # deliberately wrong
                },
            },
        )

        # Should NOT raise. We trust the just-computed md5 over the stale one.
        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        assert result.fingerprint.checksum_md5 != "deadbeef" * 4

    def test_manifest_without_entry_for_this_file_still_loads(self, tmp_path, engine):
        """Manifest exists but doesn't list THIS file. Vendor timestamp
        still flows through (dataset-level), but no per-file conflict."""
        csv_path = tmp_path / "widgets.csv"
        _write_csv(csv_path, "id,label,price", ["1,x,1.0"])

        from src.ingestion import manifest as manifest_mod
        manifest_mod.get_manifest_for.cache_clear()

        _write_manifest(
            tmp_path,
            file_entries={
                # Different filename — widgets.csv NOT in the manifest
                "other_file.csv": {
                    "size_bytes": 999,
                    "checksum_md5": "0" * 32,
                },
            },
        )

        result = load_source(
            source=_minimal_source(),
            raw_root=tmp_path,
            engine=engine,
        )
        # Dataset-level metadata still flows through
        assert result.fingerprint.source_modified_at_vendor == "2025-05-21T09:34:17+00:00"
        assert result.fingerprint.vendor_timestamp_source == "kaggle_manifest"


# ---------------------------------------------------------------------------
# Error paths
# ---------------------------------------------------------------------------


class TestErrorPaths:
    def test_missing_file_raises_file_loader_error(self, tmp_path, engine):
        # raw_root exists but the expected CSV inside it doesn't
        with pytest.raises(FileLoaderError, match="not found"):
            load_source(
                source=_minimal_source(),
                raw_root=tmp_path,
                engine=engine,
            )

    def test_unsupported_format_raises_file_loader_error(self, tmp_path, engine):
        """parquet/json are pydantic-valid format values but the loader
        hasn't been wired for them yet. Must raise a clear error."""
        source = SourceDefinition(
            name="widgets",
            description="t",
            format="parquet",
            path_pattern="{raw_root}/widgets.parquet",
            primary_key=["id"],
            schema={"id": "int"},
        )
        (tmp_path / "widgets.parquet").write_bytes(b"not really parquet")
        with pytest.raises(FileLoaderError, match="does not yet handle format"):
            load_source(source=source, raw_root=tmp_path, engine=engine)


# ---------------------------------------------------------------------------
# Integration — real committed sample data
# ---------------------------------------------------------------------------


SAMPLES_DIR = Path(__file__).resolve().parents[1] / "data" / "sample"


@pytest.mark.parametrize(
    "source_name",
    ["competitions", "clubs", "players",
     "games", "appearances", "player_valuations"],
)
def test_load_committed_sample(source_name, engine):
    """
    Round-trip every committed sample CSV through the loader.

    This is the integration smoke test: if any committed sample no
    longer satisfies the registry's declared schema, this test fails
    immediately. Catches drift between sources.yaml and the generator
    in a single check per source.
    """
    # Clear cache from earlier tests that pointed at tmp_paths
    from src.ingestion import manifest as manifest_mod
    manifest_mod.get_manifest_for.cache_clear()

    source = get_registry().get(source_name)
    result = load_source(
        source=source,
        raw_root=SAMPLES_DIR,
        engine=engine,
    )
    assert result.source_row_count > 0
    cols = set(engine.columns(result.dataframe))
    assert set(source.columns).issubset(cols), (
        f"{source_name}: declared columns missing from loaded DataFrame: "
        f"{set(source.columns) - cols}"
    )
    # Samples directory has no manifest, so vendor timestamp is None
    assert result.fingerprint.source_modified_at_vendor is None
    assert result.fingerprint.vendor_timestamp_source is None
