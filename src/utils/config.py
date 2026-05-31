"""
Config loader.

Reads configs/config.yaml, interpolates ${ENV_VAR} and ${ENV_VAR:default}
patterns from os.environ, and resolves intra-config references like
${data_root} so paths can be expressed compactly.

The output is a frozen pydantic model so downstream code gets type checking
and IDE autocomplete instead of opaque dicts.
"""

from __future__ import annotations

import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field


# ---------------------------------------------------------------------------
# Typed config models
# ---------------------------------------------------------------------------


class _Frozen(BaseModel):
    """Immutable base — config should never be mutated at runtime."""

    model_config = ConfigDict(frozen=True, extra="forbid")


class PathsConfig(_Frozen):
    data_root: Path
    raw_day1: Path
    raw_day2: Path
    sample: Path
    lake: Path
    bronze: Path
    silver: Path
    gold: Path
    rejected: Path
    metadata_db: Path
    dq_reports: Path


class SCD2Config(_Frozen):
    tracked_columns: list[str]


class TableConfig(_Frozen):
    name: str
    primary_key: list[str]
    scd2: SCD2Config | None = None


class BatchConfig(_Frozen):
    granularity: str = "hourly"
    skip_if_already_succeeded: bool = True


class DQConfig(_Frozen):
    fail_fast: bool = False
    suites_path: str


class PIIConfig(_Frozen):
    enabled: bool = True
    config_path: str
    salt_env_var: str = "PII_SALT"


class ReferenceConfig(_Frozen):
    position_taxonomy: str
    country_iso: str


class LoggingConfig(_Frozen):
    level: str = "INFO"
    # Renamed from 'json' to avoid shadowing BaseModel.json() in pydantic v2.
    # The YAML key is aliased back to 'json' for human readability.
    as_json: bool = Field(default=True, alias="json")

    model_config = ConfigDict(frozen=True, extra="forbid", populate_by_name=True)


class SparkConfig(_Frozen):
    app_name: str = "football-pipeline"
    master: str = "local[*]"
    config: dict[str, str] = Field(default_factory=dict)


class AppConfig(_Frozen):
    engine: str
    paths: PathsConfig
    tables: list[TableConfig]
    batch: BatchConfig
    dq: DQConfig
    pii: PIIConfig
    reference: ReferenceConfig
    logging: LoggingConfig
    spark: SparkConfig

    def table(self, name: str) -> TableConfig:
        """Lookup helper used by ingestion / SCD2 modules."""
        for t in self.tables:
            if t.name == name:
                return t
        raise KeyError(f"Unknown table in config: {name}")


# ---------------------------------------------------------------------------
# Interpolation
# ---------------------------------------------------------------------------

# Matches ${NAME} or ${NAME:default}. We don't allow nested braces.
_INTERP_RE = re.compile(r"\$\{([^}:]+)(?::([^}]*))?\}")


def _interpolate_value(value: str, env: dict[str, str], local: dict[str, Any]) -> str:
    """
    Replace ${VAR} / ${VAR:default} in *value*.

    Resolution order:
      1. process environment
      2. already-resolved keys in *local* (allows ${data_root} -> /data)
      3. default after the colon (if provided)
      4. raise if nothing matched and no default

    We loop until the string stops changing, so chained references resolve
    (within a safety limit to prevent runaway from a self-reference).
    """
    for _ in range(8):  # generous, real configs need 1–2 passes
        match = _INTERP_RE.search(value)
        if not match:
            return value
        full, name, default = match.group(0), match.group(1), match.group(2)
        if name in env:
            replacement = env[name]
        elif name in local:
            replacement = str(local[name])
        elif default is not None:
            replacement = default
        else:
            raise KeyError(f"Cannot resolve config reference '{name}' in value '{value}'")
        value = value.replace(full, replacement)
    raise ValueError(f"Interpolation did not converge for value: {value!r}")


def _walk_and_interpolate(node: Any, env: dict[str, str], local: dict[str, Any]) -> Any:
    """Recursively interpolate strings inside dicts/lists."""
    if isinstance(node, str):
        return _interpolate_value(node, env, local)
    if isinstance(node, dict):
        out: dict[str, Any] = {}
        for k, v in node.items():
            resolved = _walk_and_interpolate(v, env, {**local, **out})
            out[k] = resolved
        return out
    if isinstance(node, list):
        return [_walk_and_interpolate(v, env, local) for v in node]
    return node


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def load_config(path: str | Path | None = None) -> AppConfig:
    """
    Load and validate the pipeline config.

    Args:
        path: Path to config.yaml. Defaults to repo-root/configs/config.yaml.
    """
    config_path = Path(path) if path else _default_config_path()
    raw = yaml.safe_load(config_path.read_text())

    # Two-pass interpolation:
    # 1. Resolve scalars at the top level so paths.* can reference data_root.
    # 2. Walk the whole tree so nested values pick up newly-resolved keys.
    flat_locals = {k: v for k, v in raw.items() if isinstance(v, (str, int, float, bool))}
    interpolated = _walk_and_interpolate(raw, dict(os.environ), flat_locals)
    # paths/* often reference each other (lake -> bronze), so re-walk with
    # the now-resolved paths block exposed as locals.
    if isinstance(interpolated.get("paths"), dict):
        interpolated["paths"] = _walk_and_interpolate(
            interpolated["paths"], dict(os.environ), interpolated["paths"]
        )

    return AppConfig.model_validate(interpolated)


@lru_cache(maxsize=1)
def get_config() -> AppConfig:
    """Cached accessor used by long-running processes (Airflow workers)."""
    return load_config()


def _default_config_path() -> Path:
    # src/utils/config.py -> repo root is two parents up
    return Path(__file__).resolve().parents[2] / "configs" / "config.yaml"
