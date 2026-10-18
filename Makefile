# data-stack-lab — one synthetic SaaS business, built on the tools data teams hire for.
#
#   make setup       uv venv + dependencies
#   make data        generate the dataset (SCALE=lab, or tiny)
#   make warehouse   replay 13 monthly extracts through dbt snapshot, then dbt build
#   make measure     score the warehouse against the generator's truth
#   make mutants     break the models four ways; every break must fail the build
#   make docs        dbt docs (catalog + lineage) under dbt/target
#   make all         data, warehouse, measure --check, mutants  (what CI runs)
#   make airflow     backfill September through the Airflow DAGs three ways and score it
#   make check       ruff + generator tests
#
# DBT="uvx --from dbt-oss==2.0.8 dbt" make warehouse   runs the same project on dbt v2.

SHELL := /usr/bin/env bash
SCALE ?= lab
SEED ?= 7

.PHONY: setup data warehouse measure mutants docs all airflow check clean

setup:
	uv sync

data:
	uv run python -m datagen --scale $(SCALE) --seed $(SEED) --out data

warehouse:
	uv run python scripts/warehouse.py

measure:
	uv run python scripts/measure.py

mutants:
	uv run python scripts/mutants.py

docs:
	cd dbt && DUCKDB_PATH=../data/warehouse.duckdb uv run dbt docs generate --profiles-dir .

all: data warehouse
	uv run python scripts/measure.py --check
	uv run python scripts/mutants.py --check

airflow:
	uv sync --group airflow
	uv run python airflow/backfill.py

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest -q

clean:
	rm -rf data dbt/target dbt/logs
