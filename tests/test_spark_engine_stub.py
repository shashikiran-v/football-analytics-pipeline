"""
Tests for the Spark engine stub.

These tests prove that the engine abstraction's factory and protocol
wiring work end-to-end for both engine kinds, even though the Spark
side is deliberately a stub (see ADR-0009).

Two test classes:
  1. TestFactoryWiring — the factory selects the right engine class
     based on config / explicit kind argument.
  2. TestStubBehaviour — instantiation succeeds; operations raise
     NotImplementedError with a useful message pointing at ADR-0009.

We deliberately don't test every method's NotImplementedError —
that's a stub, not an implementation. Three representative methods
is enough to prove "yes, the class is truly a stub" without paying
the maintenance cost of 22 trivial tests.
"""

from __future__ import annotations

import pytest

from src.engines.base import DataFrameEngine
from src.engines.factory import get_engine
from src.engines.pandas_engine import PandasEngine
from src.engines.spark_engine import SparkEngine

# ---------------------------------------------------------------------------
# Factory wiring
# ---------------------------------------------------------------------------


class TestFactoryWiring:
    def setup_method(self):
        # Clear lru_cache between tests so each gets a fresh engine
        get_engine.cache_clear()

    def test_factory_returns_pandas_engine_when_kind_is_pandas(self):
        engine = get_engine(kind="pandas")
        assert isinstance(engine, PandasEngine)
        assert engine.kind == "pandas"

    def test_factory_returns_spark_engine_when_kind_is_spark(self):
        """The factory must instantiate the Spark engine successfully —
        proving the protocol wiring works for both engines, even though
        the Spark side is a stub. This is the test that demonstrates
        the abstraction is real and not aspirational."""
        engine = get_engine(kind="spark")
        assert isinstance(engine, SparkEngine)
        assert engine.kind == "spark"

    def test_factory_rejects_unknown_engine_kind(self):
        with pytest.raises(ValueError, match="Unknown engine kind"):
            get_engine(kind="snowflake")

    def test_both_engines_satisfy_dataframe_engine_protocol(self):
        """Both engines must be valid DataFrameEngine instances. This
        catches the case where someone adds an abstract method to the
        base protocol but forgets to declare it on one of the engines —
        without this test, the missing method would only fail at the
        runtime call site."""
        # Each engine must be instantiable without error (which only
        # works if all @abstractmethod members are declared).
        PandasEngine()
        SparkEngine()
        # And each is an instance of the protocol.
        assert isinstance(get_engine(kind="pandas"), DataFrameEngine)
        assert isinstance(get_engine(kind="spark"), DataFrameEngine)


# ---------------------------------------------------------------------------
# Stub behaviour
# ---------------------------------------------------------------------------


class TestStubBehaviour:
    def test_stub_can_be_instantiated(self):
        """The Spark stub must be cheap to instantiate — no JVM startup,
        no pyspark import. This test passing without importing pyspark
        proves the stub keeps that promise."""
        engine = SparkEngine()
        # Successful instantiation is the assertion. The kind tag is
        # populated correctly:
        assert engine.kind == "spark"

    def test_stub_operations_raise_not_implemented_error(self):
        """Three representative operations raise NotImplementedError
        with a message that points at ADR-0009. We don't test every
        method — that would be 22 trivial tests for a stub. These
        three (one I/O, one row-op, one aggregation) prove the pattern."""
        engine = SparkEngine()

        with pytest.raises(NotImplementedError) as exc_info:
            engine.read_csv("data/sample/players.csv")
        assert "ADR-0009" in str(exc_info.value)

        with pytest.raises(NotImplementedError) as exc_info:
            engine.filter_eq(df=None, column="club_id", value=1)
        assert "ADR-0009" in str(exc_info.value)

        with pytest.raises(NotImplementedError) as exc_info:
            engine.group_by_agg(
                df=None,
                by=["club_id"],
                aggs={"total_goals": ("goals", "sum")},
            )
        assert "ADR-0009" in str(exc_info.value)

    def test_stub_does_not_import_pyspark(self):
        """The stub claims (in its module docstring and ADR-0009) that
        it deliberately doesn't depend on pyspark — so users running
        Pandas don't pay JVM startup or installation overhead. This
        test asserts that claim is real by checking sys.modules after
        a fresh SparkEngine instantiation."""
        import sys

        # Trigger fresh import path
        SparkEngine()
        assert "pyspark" not in sys.modules, (
            "Spark stub should not have imported pyspark — "
            "module is meant to be a zero-dependency stub"
        )
