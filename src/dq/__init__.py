"""
Data Quality framework.

Three modules:

  rules     — typed Pydantic models for the five rule types
              (NotNull, Range, Unique, ForeignKey, Schema), each with
              an evaluate() method returning a per-row pass/fail signal.

  runner    — orchestrates rule evaluation for one source, splits the
              DataFrame into passing and failing rows, returns a typed
              DQResult with row-level reasons captured.

  quarantine — writes failing rows to data/lake/_rejected/<source>/
               batch_id=<id>/ with a _dq_failure_reason column.

Rules live in configs/dq_rules.yaml and are loaded lazily via lru_cache.
"""
