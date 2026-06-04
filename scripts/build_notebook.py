"""
Build and execute the gold_exploration notebook.

We construct the notebook programmatically (cleaner than hand-writing
JSON), execute every cell against the actual Gold layer, and write
out the .ipynb with the outputs preserved. The committed notebook
then renders inline on GitHub with results visible.

Run from project root:
    python scripts/build_notebook.py
"""

from __future__ import annotations

from pathlib import Path

import nbformat
from nbclient import NotebookClient
from nbformat import v4 as nbf


def make_md(text: str) -> nbformat.NotebookNode:
    return nbf.new_markdown_cell(text)


def make_code(code: str) -> nbformat.NotebookNode:
    return nbf.new_code_cell(code)


def build_notebook() -> nbformat.NotebookNode:
    nb = nbf.new_notebook()
    cells: list[nbformat.NotebookNode] = []

    # ----------------------------------------------------------------
    # Title + intro
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "# Gold Layer Exploration\n"
            "\n"
            "A narrative tour of the Gold layer of the football analytics pipeline.\n"
            "Every query in this notebook hits real Parquet files produced by\n"
            "running the pipeline against the committed sample data. The cells\n"
            "are intended to be read in order — each one builds on the context\n"
            "of the previous.\n"
            "\n"
            "**To reproduce locally:**\n"
            "\n"
            "```bash\n"
            "make clean                              # wipe any prior state\n"
            "PII_ENABLED=false \\\n"
            "  python -m src.bronze.run --batch-id notebook-demo --raw-root data/sample\n"
            "PII_ENABLED=false \\\n"
            "  python -m src.silver.run --batch-id notebook-demo\n"
            "PII_ENABLED=false \\\n"
            "  python -m src.gold.run --batch-id notebook-demo\n"
            "jupyter notebook notebooks/gold_exploration.ipynb\n"
            "```\n"
            "\n"
            "We disable PII so player names render as plaintext for readability.\n"
            "In production these are `pii_<8hex>` tokens (see ADR-0012).\n"
        )
    )

    # ----------------------------------------------------------------
    # Setup cell
    # ----------------------------------------------------------------
    cells.append(make_md("## Setup\n\nLoad the Gold parquet files via DuckDB."))
    cells.append(
        make_code(
            "import duckdb\n"
            "import pandas as pd\n"
            "from pathlib import Path\n"
            "\n"
            "# Disable noisy pandas display options\n"
            'pd.set_option("display.max_columns", 20)\n'
            'pd.set_option("display.width", 200)\n'
            "\n"
            "# Connect to an in-memory DuckDB session and register every Gold\n"
            "# parquet table as a view. This matches what src/gold/duckdb_session.py\n"
            "# does in production — DuckDB is our query layer over Parquet.\n"
            'GOLD = Path("../data/lake/gold")\n'
            'SILVER = Path("../data/lake/silver")\n'
            "\n"
            'con = duckdb.connect(":memory:")\n'
            "for table_dir in GOLD.iterdir():\n"
            "    if table_dir.is_dir():\n"
            "        con.execute(\n"
            '            f"CREATE VIEW {table_dir.name} AS "\n'
            "            f\"SELECT * FROM read_parquet('{table_dir}/**/*.parquet')\"\n"
            "        )\n"
            "for table_dir in SILVER.iterdir():\n"
            "    if table_dir.is_dir():\n"
            "        con.execute(\n"
            '            f"CREATE VIEW {table_dir.name} AS "\n'
            "            f\"SELECT * FROM read_parquet('{table_dir}/**/*.parquet')\"\n"
            "        )\n"
            "\n"
            'con.execute("SHOW TABLES").df()\n'
        )
    )

    # ----------------------------------------------------------------
    # Q1 — top scorers
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## Who were the top scorers of the season?\n"
            "\n"
            "The `top_scorers_by_season` Gold table is built from\n"
            "`fact_appearances` aggregated by season and player, joined to\n"
            "`dim_players` for the player name and `dim_clubs` for the club\n"
            "name *as of the match date* (as-of-event FK resolution — see\n"
            "[ADR-0008](../docs/adr/0008-cross-batch-semantics.md))."
        )
    )
    cells.append(
        make_code(
            'con.execute("""\n'
            "    SELECT \n"
            "        season,\n"
            "        player_name,\n"
            "        position_canonical,\n"
            "        club_name_at_event AS club,\n"
            "        total_goals,\n"
            "        total_assists,\n"
            "        appearance_count\n"
            "    FROM top_scorers_by_season\n"
            "    ORDER BY total_goals DESC, total_assists DESC\n"
            "    LIMIT 10\n"
            '""").df()\n'
        )
    )

    # ----------------------------------------------------------------
    # Q2 — goals by position
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## How are goals distributed by playing position?\n"
            "\n"
            "Aggregate the top-scorers table by `position_canonical`. This\n"
            "column was produced in Silver via `normalise_position`\n"
            "([ADR-0004](../docs/adr/0004-silver-transformations.md)) — vendor\n"
            "position strings like `'Centre-Forward'`, `'CF'`, `'Striker'`\n"
            "all collapse to one canonical token."
        )
    )
    cells.append(
        make_code(
            'goals_by_position = con.execute("""\n'
            "    SELECT \n"
            "        position_canonical,\n"
            "        SUM(total_goals) AS goals,\n"
            "        SUM(total_assists) AS assists,\n"
            "        COUNT(DISTINCT player_id) AS players\n"
            "    FROM top_scorers_by_season\n"
            "    GROUP BY position_canonical\n"
            "    ORDER BY goals DESC\n"
            '""").df()\n'
            "goals_by_position\n"
        )
    )
    cells.append(
        make_code(
            "import matplotlib.pyplot as plt\n"
            "\n"
            "fig, ax = plt.subplots(figsize=(10, 5))\n"
            "goals_by_position.plot(\n"
            '    x="position_canonical",\n'
            '    y=["goals", "assists"],\n'
            '    kind="bar",\n'
            "    ax=ax,\n"
            '    color=["#f57c00", "#0288d1"],\n'
            ")\n"
            'ax.set_title("Goals and assists by canonical position")\n'
            'ax.set_xlabel("position")\n'
            'ax.set_ylabel("count")\n'
            'plt.xticks(rotation=30, ha="right")\n'
            "plt.tight_layout()\n"
            "plt.show()\n"
        )
    )

    # ----------------------------------------------------------------
    # Q3 — club performance
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## Which clubs had the best season?\n"
            "\n"
            "`club_season_summary` rolls up `fact_games` to give wins, draws,\n"
            "losses, goals for and against. We sort by points (3 * wins + draws)\n"
            "to mimic a standings table."
        )
    )
    cells.append(
        make_code(
            'con.execute("""\n'
            "    SELECT \n"
            "        club_name,\n"
            "        matches_played,\n"
            "        wins,\n"
            "        draws,\n"
            "        losses,\n"
            "        goals_for,\n"
            "        goals_against,\n"
            "        goal_difference,\n"
            "        points\n"
            "    FROM club_season_summary\n"
            "    ORDER BY points DESC\n"
            '""").df()\n'
        )
    )

    # ----------------------------------------------------------------
    # Q4 — value over time
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## How do player valuations evolve over time?\n"
            "\n"
            "`player_valuation_rolling_avg` carries a rolling mean of each\n"
            "player's market value across observations. This is one of the\n"
            "rare cases where the source data already has a time grain — every\n"
            "row in `player_valuations` is a point-in-time observation, not a\n"
            "current-state record. The pipeline keeps it as a fact table\n"
            "rather than forcing it into SCD2 ([ADR-0005](../docs/adr/0005-scd2-implementation.md))."
        )
    )
    cells.append(
        make_code(
            'val_trends = con.execute("""\n'
            "    SELECT \n"
            "        pv.date,\n"
            "        dp.name AS player,\n"
            "        pv.market_value_in_eur AS value_eur,\n"
            "        pv.rolling_avg_90d AS rolling_90d_avg,\n"
            "        pv.rolling_sample_count AS n_observations\n"
            "    FROM player_valuation_rolling_avg pv\n"
            "    JOIN dim_players dp ON pv.player_id = dp.player_id\n"
            "    WHERE dp.is_current = TRUE\n"
            "    ORDER BY player, pv.date\n"
            '""").df()\n'
            "val_trends.head(15)\n"
        )
    )

    # ----------------------------------------------------------------
    # Q5 — SCD2 history (dim_players)
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## How does SCD Type 2 actually look in storage?\n"
            "\n"
            "`dim_players` keeps every version of every player as a separate\n"
            "row, with `effective_date` / `end_date` defining when each\n"
            "version was current. For an initial load (the sample data here),\n"
            "every player has one version with a far-past `effective_date`.\n"
            "Subsequent runs with changed tracked columns would create new\n"
            "versions ([ADR-0005](../docs/adr/0005-scd2-implementation.md))."
        )
    )
    cells.append(
        make_code(
            'con.execute("""\n'
            "    SELECT \n"
            "        player_sk,\n"
            "        player_id,\n"
            "        name,\n"
            "        position_canonical,\n"
            "        market_value_in_eur,\n"
            "        effective_date,\n"
            "        end_date,\n"
            "        is_current\n"
            "    FROM dim_players\n"
            "    ORDER BY player_id, effective_date\n"
            "    LIMIT 10\n"
            '""").df()\n'
        )
    )

    # ----------------------------------------------------------------
    # Q6 — Joins across the star schema
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## A star-schema join: top scorer at each club\n"
            "\n"
            "The point of a star schema is that this kind of question is just\n"
            "a SQL `JOIN` and `GROUP BY`. The Gold layer's pre-aggregated\n"
            "tables save us from re-running the joins on every dashboard load,\n"
            "but the dimensions are still queryable for ad-hoc analysis."
        )
    )
    cells.append(
        make_code(
            'con.execute("""\n'
            "    WITH ranked AS (\n"
            "        SELECT\n"
            "            club_name_at_event AS club,\n"
            "            player_name,\n"
            "            total_goals,\n"
            "            ROW_NUMBER() OVER (\n"
            "                PARTITION BY club_name_at_event\n"
            "                ORDER BY total_goals DESC, total_assists DESC\n"
            "            ) AS rank_in_club\n"
            "        FROM top_scorers_by_season\n"
            "    )\n"
            "    SELECT club, player_name, total_goals\n"
            "    FROM ranked\n"
            "    WHERE rank_in_club = 1\n"
            "    ORDER BY total_goals DESC\n"
            '""").df()\n'
        )
    )

    # ----------------------------------------------------------------
    # Q7 — Provenance / DQ check
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## What does the pipeline say about itself?\n"
            "\n"
            "The pipeline's metadata DB records every batch run. This is the\n"
            "audit trail that powers idempotency and the DQ gate. The same\n"
            "table is consulted by both host-side Airflow and the Docker\n"
            "container — they agree on what's been run\n"
            "([ADR-0001](../docs/adr/0001-audit-table-design.md))."
        )
    )
    cells.append(
        make_code(
            "import sqlite3\n"
            'meta = sqlite3.connect("../data/metadata.db")\n'
            "pd.read_sql_query(\n"
            '    "SELECT batch_id, layer, status, rows_out, "\n'
            '    "started_at, finished_at FROM pipeline_runs "\n'
            '    "ORDER BY started_at",\n'
            "    meta,\n"
            ")\n"
        )
    )

    # ----------------------------------------------------------------
    # Closing
    # ----------------------------------------------------------------
    cells.append(
        make_md(
            "## Where to next\n"
            "\n"
            "The Gold parquet files behind these queries are stable, partitioned,\n"
            "and ready for connection from any BI tool that speaks SQL —\n"
            "Superset, Metabase, Tableau, or even a Pandas notebook like this\n"
            "one. The pipeline's job ends at producing Gold; the consumer chooses\n"
            "the visualisation layer.\n"
            "\n"
            "Things to explore from here:\n"
            "\n"
            "- **Re-run the pipeline** with a different `--batch-id` and see\n"
            "  how SCD2 handles changes. The `data/sample/day2/` directory has\n"
            "  pre-built mutations designed to exercise the merge.\n"
            "- **Browse the DQ reports** at `data/dq_reports/`. Each batch\n"
            "  produces a JSON summary plus rejected-row partitions under\n"
            "  `data/lake/_rejected/`.\n"
            "- **Inspect the audit trail** with\n"
            "  `sqlite3 data/metadata.db 'SELECT * FROM file_audit'` — every\n"
            "  ingested file is recorded by checksum.\n"
            "- **Trigger the DAG via Docker** (`make docker-up`) and re-run\n"
            "  this notebook — the same Gold tables are produced.\n"
        )
    )

    nb["cells"] = cells
    nb["metadata"]["kernelspec"] = {
        "display_name": "Python 3",
        "language": "python",
        "name": "python3",
    }
    nb["metadata"]["language_info"] = {"name": "python"}
    return nb


def main() -> None:
    notebooks_dir = Path("notebooks")
    notebooks_dir.mkdir(exist_ok=True)

    out_path = notebooks_dir / "gold_exploration.ipynb"

    nb = build_notebook()
    print(f"Built notebook with {len(nb['cells'])} cells")

    # Execute in-place — sets outputs and execution_count on every code cell.
    # cwd = notebooks_dir so relative paths in cells (../data/lake/...) resolve.
    client = NotebookClient(
        nb,
        timeout=120,
        kernel_name="python3",
        resources={"metadata": {"path": str(notebooks_dir)}},
    )
    client.execute()

    nbformat.write(nb, out_path)
    print(f"Wrote {out_path}")


if __name__ == "__main__":
    main()
