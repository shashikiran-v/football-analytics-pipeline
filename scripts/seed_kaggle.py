"""
Kaggle dataset fetcher.

Downloads the Kaggle Football dataset (davidcariboo/player-scores) into
data/day1/ and writes a sibling _manifest.json carrying the vendor's
authoritative metadata: dataset version, lastUpdated timestamp, and
per-file size/checksum.

The manifest is the pivot that lets Phase 2b's Bronze loader populate
the audit DAO's vendor_timestamp fields. Without this script, downstream
audit rows would carry vendor_timestamp_source='filesystem_only' — still
correct, but less informative than knowing the real vendor publication date.

Usage:

    # First-time setup: place your kaggle.json in ~/.kaggle/
    # (Get it from https://kaggle.com -> Account -> Create New Token)
    python -m scripts.seed_kaggle

    # Or, equivalently, via the Makefile:
    make seed

What this script does:

  1. Validates that kaggle.json exists and the kaggle CLI is available.
  2. Calls the Kaggle API to list the dataset's metadata (we need
     dataset_version and lastUpdated BEFORE the download for the manifest).
  3. Downloads all files into data/day1/.
  4. Computes per-file size and MD5 checksum.
  5. Writes data/day1/_manifest.json with manifest_version=1.

Failure modes (loudly):
  * No kaggle.json found  -> instructive error
  * kaggle package not installed -> instructive error
  * Network failure during download -> propagates, partial files NOT
    cleaned up (intentional: lets human inspect what got through)
"""

from __future__ import annotations

import json
import os
import sys
from datetime import datetime, timezone
from pathlib import Path

from src.utils.checksums import file_checksum_md5

# ----- Configuration --------------------------------------------------------

# The Kaggle dataset slug (owner/name) we're fetching.
KAGGLE_DATASET = "davidcariboo/player-scores"

# Output directory, resolved at runtime to be robust to working directory.
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = PROJECT_ROOT / "data" / "day1"

# Manifest schema version — must match SUPPORTED_MANIFEST_VERSIONS in
# src/ingestion/manifest.py. Bump both together if we ever evolve the format.
MANIFEST_VERSION = 1


# ----- Pre-flight checks ----------------------------------------------------


def _ensure_kaggle_available() -> None:
    """
    Verify kaggle.json is present in one of the expected locations and
    the kaggle Python package is installed. Fail with a helpful message
    rather than a cryptic ImportError or HTTP 401.

    Important: the kaggle SDK calls authenticate() at IMPORT time
    (in its __init__.py). So even a credential file in the wrong
    location causes `import kaggle` itself to raise OSError. We pre-check
    for the file in either of the two canonical locations before importing.
    """
    # kaggle SDK looks at ~/.kaggle/ first, ~/.config/kaggle/ second.
    candidate_paths = [
        Path.home() / ".kaggle" / "kaggle.json",
        Path.home() / ".config" / "kaggle" / "kaggle.json",
    ]
    found_path = next((p for p in candidate_paths if p.is_file()), None)
    has_env_creds = (
        os.environ.get("KAGGLE_USERNAME") and os.environ.get("KAGGLE_KEY")
    )

    if found_path is None and not has_env_creds:
        print(
            "ERROR: Kaggle credentials not found.\n"
            f"  Expected ~/.kaggle/kaggle.json or ~/.config/kaggle/kaggle.json, "
            "or KAGGLE_USERNAME/KAGGLE_KEY env vars.\n"
            "\n"
            "  To get a token:\n"
            "    1. Sign in at https://kaggle.com\n"
            "    2. Account -> Settings -> Create New API Token\n"
            "    3. Move the downloaded kaggle.json to ~/.kaggle/\n"
            "    4. chmod 600 ~/.kaggle/kaggle.json\n",
            file=sys.stderr,
        )
        sys.exit(2)

    try:
        # Importing here (not at module top) so the manifest-reader
        # parts of the codebase don't have to depend on the kaggle SDK.
        # Catch both ImportError (not installed) and OSError (the SDK
        # raises OSError if it can't find creds during its import-time
        # authenticate() call).
        import kaggle  # noqa: F401
    except ImportError:
        print(
            "ERROR: The 'kaggle' Python package is not installed.\n"
            "  Run: pip install kaggle==1.6.17\n"
            "  (Also available in requirements.txt as of Phase 2b.)\n",
            file=sys.stderr,
        )
        sys.exit(2)
    except OSError as e:
        # The SDK failed its own credential discovery despite our check
        # above. Pass the SDK's message through for transparency.
        print(
            f"ERROR: The kaggle SDK could not authenticate: {e}\n"
            "  Try setting KAGGLE_USERNAME and KAGGLE_KEY env vars, "
            "or ensure ~/.kaggle/kaggle.json has mode 600.\n",
            file=sys.stderr,
        )
        sys.exit(2)


# ----- Fetch + manifest assembly --------------------------------------------


def _fetch_dataset_metadata() -> tuple[int, str]:
    """
    Query the Kaggle API for the dataset's current version number and
    lastUpdated timestamp. Returns (version, iso_timestamp).

    NB: the kaggle API returns datetime objects in local-time naive
    form. We convert to UTC ISO8601 for storage.
    """
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    # kaggle's `datasets_list` returns a list of Dataset objects; we
    # filter to our slug. The owner+name form ensures uniqueness.
    owner, name = KAGGLE_DATASET.split("/", 1)
    matches = api.dataset_list(search=name, user=owner)
    target = next(
        (d for d in matches if str(d).endswith(KAGGLE_DATASET)),
        None,
    )
    if target is None:
        raise RuntimeError(
            f"Dataset {KAGGLE_DATASET} not found via Kaggle API. "
            "Check the dataset slug and your token permissions."
        )

    # `target.lastUpdated` is a string like '2025-05-21 09:34:17' (UTC by Kaggle convention)
    last_updated_naive = datetime.fromisoformat(str(target.lastUpdated))
    last_updated_utc = last_updated_naive.replace(tzinfo=timezone.utc)
    return target.currentVersionNumber, last_updated_utc.isoformat(timespec="seconds")


def _download_files() -> None:
    """Download (and unzip) the dataset into OUTPUT_DIR."""
    from kaggle.api.kaggle_api_extended import KaggleApi

    api = KaggleApi()
    api.authenticate()

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {KAGGLE_DATASET} -> {OUTPUT_DIR}")
    api.dataset_download_files(
        KAGGLE_DATASET,
        path=str(OUTPUT_DIR),
        unzip=True,
        force=False,   # respect existing files; rerun-safe
    )


def _build_manifest(
    *,
    dataset_version: int,
    vendor_last_updated: str,
) -> dict[str, object]:
    """Assemble the manifest dict ready to JSON-serialise."""
    files: dict[str, dict[str, object]] = {}
    for path in sorted(OUTPUT_DIR.glob("*.csv")):
        files[path.name] = {
            "size_bytes": path.stat().st_size,
            "checksum_md5": file_checksum_md5(path),
        }
    return {
        "manifest_version": MANIFEST_VERSION,
        "vendor": "kaggle",
        "dataset": KAGGLE_DATASET,
        "dataset_version": dataset_version,
        "vendor_last_updated": vendor_last_updated,
        "fetched_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "files": files,
    }


def _write_manifest(manifest: dict[str, object]) -> Path:
    """Write the manifest as pretty-printed JSON next to the data."""
    manifest_path = OUTPUT_DIR / "_manifest.json"
    manifest_path.write_text(
        json.dumps(manifest, indent=2, sort_keys=False) + "\n",
        encoding="utf-8",
    )
    return manifest_path


# ----- Entry point ----------------------------------------------------------


def main() -> None:
    _ensure_kaggle_available()

    print(f"Fetching dataset metadata for {KAGGLE_DATASET}...")
    dataset_version, vendor_last_updated = _fetch_dataset_metadata()
    print(f"  version       = {dataset_version}")
    print(f"  lastUpdated   = {vendor_last_updated}")

    _download_files()

    print("Computing per-file checksums...")
    manifest = _build_manifest(
        dataset_version=dataset_version,
        vendor_last_updated=vendor_last_updated,
    )

    manifest_path = _write_manifest(manifest)
    print(f"Wrote manifest: {manifest_path}")
    print(f"Files captured ({len(manifest['files'])}):")
    for filename, meta in manifest["files"].items():
        size_mb = meta["size_bytes"] / 1_048_576
        print(f"  {filename:30s}  {size_mb:7.2f} MB  md5={meta['checksum_md5']}")


if __name__ == "__main__":
    main()
