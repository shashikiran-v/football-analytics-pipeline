"""
Engine factory.

The single place in the codebase that knows about both engine classes.
Everywhere else imports only `DataFrameEngine` (the protocol) and calls
`get_engine()` to obtain an instance.

The Spark engine is imported lazily because importing pyspark eagerly
costs ~3s of JVM startup that we don't want when engine=pandas.
"""

from __future__ import annotations

from functools import lru_cache

from src.engines.base import DataFrameEngine
from src.utils.config import get_config
from src.utils.logging import get_logger

log = get_logger(__name__)


def _build_engine(kind: str) -> DataFrameEngine:
    if kind == "pandas":
        from src.engines.pandas_engine import PandasEngine

        return PandasEngine()
    if kind == "spark":
        # Lazy import: pyspark startup is ~3s, skip it for pandas runs.
        from src.engines.spark_engine import SparkEngine

        return SparkEngine()
    raise ValueError(f"Unknown engine kind: {kind!r} (expected 'pandas' or 'spark')")


@lru_cache(maxsize=2)
def get_engine(kind: str | None = None) -> DataFrameEngine:
    """
    Return a (cached) engine instance.

    Args:
        kind: 'pandas' or 'spark'. If omitted, taken from config.yaml.
    """
    resolved = kind or get_config().engine
    engine = _build_engine(resolved)
    log.info("engine_initialised", kind=engine.kind)
    return engine
