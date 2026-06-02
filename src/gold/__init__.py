"""
Gold layer — analytical aggregations over Silver dim/fact tables.

Three modules:

  duckdb_session  — manages the DuckDB connection, registers Silver
                    Parquet directories as queryable views

  artifacts       — typed GoldArtifact definitions, one per §6
                    analytical question from the brief

  builders        — execute artifact SQL and materialise results to
                    partitioned Parquet at data/lake/gold/

Gold artifacts are queryable two ways:
  1. Direct SQL via the DuckDB session: `SELECT * FROM top_scorers...`
  2. As Parquet files on disk: `pd.read_parquet('data/lake/gold/...')`

Both are valid — DuckDB views give an analyst-facing SQL interface;
Parquet materialisation gives audit/lineage parity with Bronze/Silver.
"""
