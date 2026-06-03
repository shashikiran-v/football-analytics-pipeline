# =====================================================================
# Football Analytics Pipeline — convenience Makefile
# ---------------------------------------------------------------------
# Common operations as short verbs. Run `make help` for the inventory.
# =====================================================================

.PHONY: help install test lint typecheck samples seed clean clean-caches clean-data clean-all

# Use bash with strict flags so a failing command in a recipe stops the line.
SHELL := /bin/bash
.SHELLFLAGS := -eu -o pipefail -c

PYTHON ?= python

help:                ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
	  | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

install:             ## Install runtime + dev dependencies into the active venv.
	$(PYTHON) -m pip install -r requirements-dev.txt

test:                ## Run the full test suite (quiet).
	$(PYTHON) -m pytest tests/

test-v:              ## Run the full test suite (verbose).
	$(PYTHON) -m pytest tests/ -v

lint:                ## Run ruff lint checks.
	$(PYTHON) -m ruff check src/ tests/ scripts/

typecheck:           ## Run mypy.
	$(PYTHON) -m mypy src/

samples:             ## Regenerate the committed sample CSVs in data/sample/.
	$(PYTHON) -m scripts.generate_samples

seed:                ## Download the full Kaggle dataset to data/day1/ + write manifest.
	$(PYTHON) -m scripts.seed_kaggle

clean:               ## Reset pipeline state: lake, DQ reports, metadata DB, caches (preserves data/sample, data/day1, data/day2).
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -rf data/lake data/dq_reports
	rm -f data/metadata.db data/metadata.db-wal data/metadata.db-shm
	@echo "Reset pipeline state. Sample data, Kaggle data and code preserved."

clean-caches:        ## Remove only Python/test caches (does NOT touch pipeline data).
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	@echo "Cleaned caches only. Pipeline state preserved."

clean-data:          ## Remove only pipeline outputs (lake, DQ reports, metadata DB).
	rm -rf data/lake data/dq_reports
	rm -f data/metadata.db data/metadata.db-wal data/metadata.db-shm
	@echo "Cleaned pipeline data. Caches preserved."

clean-all:           ## Nuclear reset: clean + drop raw Kaggle data from data/day1/, data/day2/.
	$(MAKE) clean
	find data/day1 -mindepth 1 -not -name '.gitkeep' -delete 2>/dev/null || true
	find data/day2 -mindepth 1 -not -name '.gitkeep' -delete 2>/dev/null || true
	@echo "Nuclear reset complete. Raw Kaggle data also removed."
