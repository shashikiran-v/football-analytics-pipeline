"""
Source Registry — loads configs/sources.yaml into typed objects.

This is the framework that makes adding a new dataset a YAML edit
rather than a Python code change. Every downstream module that needs
to enumerate sources (Bronze, DQ, Silver, Gold, the DAG) consults
this registry rather than maintaining its own hard-coded list.

Public API
----------
    get_registry()               -> SourceRegistry (cached)
    load_registry(path=...)      -> SourceRegistry (uncached, for tests)

    registry.all_sources()       -> list[SourceDefinition]
    registry.get(name)           -> SourceDefinition
    registry.scd2_sources()      -> list[SourceDefinition]
    registry.pii_sources()       -> list[SourceDefinition]
    registry.incremental_sources() -> list[SourceDefinition]

Design notes
------------
* We use pydantic for parsing+validation because a malformed YAML file
  should fail loudly at startup, not silently produce a broken pipeline.
  An unknown field in sources.yaml raises ValidationError (extra='forbid').

* `path_pattern` is a string template, not a Path, because it contains
  unresolved placeholders like {raw_root}. The registry doesn't know
  what raw_root is at load time — the caller (Bronze) resolves it when
  it's about to read the file, using SourceDefinition.resolve_path().

* `version: 1` at the top of sources.yaml gives us future-proofing.
  When the registry schema evolves, we bump the version and the loader
  warns on stale configs. Not enforced today; the hook is there.
"""

from __future__ import annotations

from functools import lru_cache
from pathlib import Path
from typing import Iterator

import yaml
from pydantic import BaseModel, ConfigDict, Field

from src.utils.config import get_config
from src.utils.logging import get_logger


log = get_logger(__name__)


# Current registry schema version. The loader logs a warning if the
# version in sources.yaml differs from this.
REGISTRY_SCHEMA_VERSION = 1


# ---------------------------------------------------------------------------
# Typed source-definition models
# ---------------------------------------------------------------------------


class _Frozen(BaseModel):
    """Immutable, strict-on-extras base. Same pattern as AppConfig."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class SCD2Spec(_Frozen):
    """SCD Type 2 configuration for a source."""

    tracked_columns: list[str] = Field(
        ..., min_length=1,
        description="Columns whose changes open a new SCD2 version. "
                    "Hashed together to detect changes cheaply.",
    )


class PIISpec(_Frozen):
    """PII anonymisation configuration for a source."""

    hash_columns: list[str] = Field(
        ..., min_length=1,
        description="Columns whose values are salted-SHA-256 hashed "
                    "at the Bronze -> Silver boundary.",
    )


class AuditSpec(_Frozen):
    """Per-source audit overrides. Extensible for future checks."""

    expected_min_rows: int | None = Field(
        default=None,
        description="If set, ingesting a snapshot with fewer rows than "
                    "this raises a WARN-level DQ event.",
    )


class SourceDefinition(_Frozen):
    """
    One entry from sources.yaml. Fully self-contained: a new joiner
    only has to read one block to understand a source completely.
    """

    name: str = Field(..., min_length=1)
    description: str
    format: str = Field(..., pattern="^(csv|parquet|json)$")
    path_pattern: str
    primary_key: list[str] = Field(..., min_length=1)
    schema_: dict[str, str] = Field(..., alias="schema", min_length=1)
    timestamp_column: str | None = None
    scd2: SCD2Spec | None = None
    pii: PIISpec | None = None
    audit: AuditSpec = Field(default_factory=AuditSpec)

    model_config = ConfigDict(
        frozen=True,
        extra="forbid",
        populate_by_name=True,           # accept 'schema' from YAML, expose as schema_
    )

    # -- Convenience accessors so callers don't reach into the dict --

    @property
    def is_scd2(self) -> bool:
        return self.scd2 is not None

    @property
    def has_pii(self) -> bool:
        return self.pii is not None

    @property
    def is_incremental(self) -> bool:
        """True if Bronze can advance a watermark for this source."""
        return self.timestamp_column is not None

    @property
    def columns(self) -> list[str]:
        return list(self.schema_.keys())

    def resolve_path(self, raw_root: Path | str) -> Path:
        """
        Substitute {raw_root} in path_pattern with an actual root and
        return a Path. Used by Bronze when it's about to read the file.
        """
        return Path(self.path_pattern.format(raw_root=str(raw_root)))


class _RegistryFile(_Frozen):
    """The top-level shape of sources.yaml. Used only at parse time."""

    version: int
    sources: list[SourceDefinition] = Field(..., min_length=1)


# ---------------------------------------------------------------------------
# Registry object — typed lookup over the parsed sources
# ---------------------------------------------------------------------------


class SourceRegistry:
    """
    In-memory registry of all known sources. Immutable after construction.

    Provides lookup by name, plus filtered views (SCD2-only, PII-only,
    incremental-only) that downstream modules use to iterate over the
    relevant subset without conditional logic at every call site.
    """

    def __init__(self, sources: list[SourceDefinition]) -> None:
        # Detect duplicate names early — would silently break lookups.
        names = [s.name for s in sources]
        duplicates = {n for n in names if names.count(n) > 1}
        if duplicates:
            raise ValueError(f"Duplicate source names in registry: {sorted(duplicates)}")
        self._by_name: dict[str, SourceDefinition] = {s.name: s for s in sources}

    # -- Iteration / lookup -------------------------------------------------

    def all_sources(self) -> list[SourceDefinition]:
        """All sources in YAML declaration order."""
        return list(self._by_name.values())

    def names(self) -> list[str]:
        return list(self._by_name.keys())

    def get(self, name: str) -> SourceDefinition:
        try:
            return self._by_name[name]
        except KeyError:
            raise KeyError(
                f"Unknown source: {name!r}. Known: {sorted(self._by_name)}"
            ) from None

    def __contains__(self, name: object) -> bool:
        return isinstance(name, str) and name in self._by_name

    def __len__(self) -> int:
        return len(self._by_name)

    def __iter__(self) -> Iterator[SourceDefinition]:
        return iter(self._by_name.values())

    # -- Filtered views ----------------------------------------------------

    def scd2_sources(self) -> list[SourceDefinition]:
        """Sources flagged for SCD Type 2 processing in Silver."""
        return [s for s in self if s.is_scd2]

    def pii_sources(self) -> list[SourceDefinition]:
        """Sources with at least one PII column to anonymise."""
        return [s for s in self if s.has_pii]

    def incremental_sources(self) -> list[SourceDefinition]:
        """Sources Bronze can ingest incrementally via watermark."""
        return [s for s in self if s.is_incremental]


# ---------------------------------------------------------------------------
# Loading
# ---------------------------------------------------------------------------


def load_registry(path: str | Path | None = None) -> SourceRegistry:
    """
    Read sources.yaml, validate, and return a SourceRegistry.

    Args:
        path: Path to sources.yaml. Defaults to configs/sources.yaml
              relative to the repo root.

    Raises:
        FileNotFoundError: if the file doesn't exist.
        pydantic.ValidationError: if the file is malformed (unknown
                                  field, missing required field, bad type).
        ValueError: if duplicate source names are declared.
    """
    config_path = Path(path) if path else _default_path()
    if not config_path.exists():
        raise FileNotFoundError(f"sources.yaml not found at {config_path}")

    raw = yaml.safe_load(config_path.read_text())
    parsed = _RegistryFile.model_validate(raw)

    if parsed.version != REGISTRY_SCHEMA_VERSION:
        log.warning(
            "registry_schema_version_mismatch",
            file_version=parsed.version,
            loader_version=REGISTRY_SCHEMA_VERSION,
            note="loader is forward-compatible but config may use unrecognised fields",
        )

    registry = SourceRegistry(parsed.sources)
    log.info(
        "registry_loaded",
        path=str(config_path),
        sources=registry.names(),
        scd2_count=len(registry.scd2_sources()),
        pii_count=len(registry.pii_sources()),
        incremental_count=len(registry.incremental_sources()),
    )
    return registry


@lru_cache(maxsize=1)
def get_registry() -> SourceRegistry:
    """Process-wide cached accessor. Use this from pipeline code."""
    return load_registry()


def _default_path() -> Path:
    # src/ingestion/registry.py -> repo root is two parents up
    return Path(__file__).resolve().parents[2] / "configs" / "sources.yaml"
