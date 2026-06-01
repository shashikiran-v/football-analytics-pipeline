# =====================================================================
# Football Analytics Pipeline — convenience Makefile
# ---------------------------------------------------------------------
# Common operations as short verbs. Run `make help` for the inventory.
# =====================================================================

.PHONY: help install test lint typecheck samples seed clean

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

clean:               ## Remove caches and SQLite metadata DB (data/sample/ is preserved).
	rm -rf .pytest_cache .mypy_cache .ruff_cache
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true
	rm -f data/metadata.db data/metadata.db-wal data/metadata.db-shm
	@echo "Cleaned caches and metadata DB."
