# data-stack-lab — hands-on data engineering projects, one per tool, on one synthetic SaaS dataset.
#
#   make setup       uv venv + dependencies
#   make data        generate the dataset (SCALE=lab, or tiny)
#   make warehouse   replay 13 monthly extracts through dbt snapshot, then dbt build
#   make measure     score the warehouse against the generator's truth
#   make mutants     break the models four ways; every break must fail the build
#   make docs        dbt docs (catalog + lineage) under dbt/target
#   make all         data, warehouse, measure --check, mutants  (what CI runs)
#   make airflow     backfill September through the Airflow DAGs four ways and score it
#   make quality     deliver September with nine defects, check it in dbt, score both policies
#   make spark       PySpark: re-running a day, small files, partition pruning (needs Java 17+)
#   make databricks  Databricks: six loaders over ten days of deliveries, and a re-run on Delta
#                    (needs a workspace: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH)
#   make check       ruff + generator tests
#
# DBT="uvx --from dbt-oss==2.0.8 dbt" make warehouse   runs the same project on dbt v2.

SHELL := /usr/bin/env bash
SCALE ?= lab
SEED ?= 7

.PHONY: setup data warehouse measure mutants docs all airflow quality spark databricks check clean

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

quality:
	uv run python quality/run.py

spark:
	uv sync --group spark
	uv run python spark/run.py

databricks:
	uv sync --group databricks
	uv run python databricks/run.py

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest -q

clean:
	rm -rf data dbt/target dbt/logs
