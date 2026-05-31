"""
Shared pytest fixtures.

The big idea: every engine-level test runs against every available
engine. A test that takes `engine` as a fixture gets called once per
engine; failures are tagged so we know which backend disagreed.

For Phase 1 only the pandas engine exists; the parametrisation harness
already supports spark so adding it later is zero-effort.
"""

from __future__ import annotations

import os
import tempfile
from pathlib import Path
from typing import Iterator

import pytest

from src.engines.base import DataFrameEngine
from src.engines.pandas_engine import PandasEngine


# Engines available for parametrised tests. Spark is added in a later phase.
_AVAILABLE_ENGINES: list[tuple[str, type[DataFrameEngine]]] = [
    ("pandas", PandasEngine),
]


@pytest.fixture(params=[name for name, _ in _AVAILABLE_ENGINES])
def engine(request) -> DataFrameEngine:
    """
    Parametrised engine fixture. Tests that take `engine` are run once
    per available backend. Mark tests with @pytest.mark.pandas or
    @pytest.mark.spark to scope them to one backend.
    """
    name = request.param
    cls = dict(_AVAILABLE_ENGINES)[name]
    # Auto-apply the matching marker so `pytest -m pandas` works.
    request.applymarker(getattr(pytest.mark, name))
    return cls()


@pytest.fixture
def tmp_lake(tmp_path: Path) -> Path:
    """A temporary directory used as a mini data lake during tests."""
    lake = tmp_path / "lake"
    lake.mkdir()
    return lake


@pytest.fixture(autouse=True)
def _isolate_metadata_db(tmp_path: Path, monkeypatch) -> Iterator[None]:
    """
    Every test gets its own metadata DB. Without this, tests would
    pollute the developer's local data/metadata.db and interfere
    with each other.

    We point the config's metadata_db path at a temp location by
    setting DATA_ROOT before the config is loaded — the config loader
    interpolates ${DATA_ROOT} into paths.
    """
    monkeypatch.setenv("DATA_ROOT", str(tmp_path))
    # Clear cached config / engine so they re-read the new env.
    from src.utils import config as cfg_mod
    from src.engines import factory as factory_mod

    cfg_mod.get_config.cache_clear()
    factory_mod.get_engine.cache_clear()
    yield
    cfg_mod.get_config.cache_clear()
    factory_mod.get_engine.cache_clear()
