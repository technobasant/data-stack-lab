# data-stack-lab — hands-on data engineering projects, one per tool, on one generated SaaS dataset.
#
#   make setup       uv venv + dependencies
#   make data        generate the dataset (SCALE=lab, or tiny)
#   make warehouse   replay 13 monthly extracts through dbt snapshot, then dbt build
#   make measure     score the warehouse against the generator's truth
#   make mutants     break the models four ways; every break must fail the build
#   make docs        dbt docs (catalog + lineage) under dbt/target
#   make all         data, warehouse, measure --check, mutants --check (the dbt project, as CI checks it)
#   make airflow     backfill September through the Airflow DAGs four ways and score it
#   make quality     deliver September with nine defects, check it in dbt, score both policies
#   make spark       PySpark: re-running a day, small files, partition pruning (needs Java 17+)
#   make databricks  Databricks: six loaders over ten days of deliveries, and a re-run on Delta
#                    (needs a workspace: DATABRICKS_HOST, DATABRICKS_TOKEN, DATABRICKS_HTTP_PATH)
#   make check       ruff + generator tests
#   make verify      every offline check CI runs (see the verify: target below for what's skipped)
#
# DBT="uvx --from dbt-oss==2.0.8 dbt" make warehouse   runs the same project on dbt v2.
#
# Unlike `all`, the airflow/quality/spark/databricks targets above write results/*.json
# rather than checking them; CI runs the same scripts with --check instead, which fails
# on any difference. `make verify` runs the --check forms, the way CI does.

SHELL := /usr/bin/env bash
SCALE ?= lab
SEED ?= 7

.PHONY: setup data warehouse measure mutants docs all airflow airflow-scheduler quality spark databricks check verify clean

setup:
	uv sync

# `data` is a file target: once data/manifest.json exists, `make data` is a no-op, even
# if SCALE or SEED changed. Run `make clean` first to force a regenerate with different
# parameters.
data: data/manifest.json

data/manifest.json:
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

airflow: data
	uv sync --group airflow
	uv run python airflow/backfill.py

# The in-order backfill on a real scheduler (airflow standalone); not run in CI.
airflow-scheduler: data
	uv sync --group airflow
	uv run python airflow/scheduler_run.py

quality: data
	uv run python quality/run.py

spark: data
	uv sync --group spark
	uv run python spark/run.py

databricks: data
	uv sync --group databricks
	uv run python databricks/run.py

check:
	uv run ruff check .
	uv run ruff format --check .
	uv run pytest -q

# Every offline check CI runs (.github/workflows/ci.yml), job for job, against a fresh
# dataset: lint, the workflow audit, the generator/DAG tests, the dbt project rebuilt
# and compared, and the airflow/quality/spark/databricks harnesses run with --check
# instead of written plain. Skipped, because they are not offline: the Databricks
# project's workspace-only steps (databricks/run.py with no --plan, and
# databricks/dbt_build.py), which need a workspace and credentials and are not run in
# CI either — CI checks only databricks/run.py --plan --check, which this runs.
verify: data
	uv run ruff format --check .
	uv run ruff check .
	uv run pytest -q
	uvx zizmor@1.30.1 --offline --persona=regular .github/workflows
	uv run python scripts/warehouse.py
	uv run python scripts/measure.py --check
	uv run python scripts/mutants.py --check
	uv sync --group airflow
	uv run pytest -q tests/test_dags.py
	uv run python airflow/backfill.py --check
	uv run python quality/run.py --check
	uv sync --group spark
	uv run python spark/run.py --check
	uv run python databricks/run.py --plan --check

clean:
	rm -rf data dbt/target dbt/logs
