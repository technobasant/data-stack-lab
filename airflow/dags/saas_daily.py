"""Daily event load and the incremental dbt models built on it.

Each run lands one day of event files into the warehouse and runs the three incremental
models that differ only in how they choose what is new (dbt/models/airflow/):

  events_by_event_time   newer than the newest event_at already loaded
  events_by_arrival      newer than the newest received_at already loaded
  events_by_interval     received inside this run's data interval

Two DAGs share the code and differ only in their schedule:

  saas_daily               CronDataIntervalTimetable: each run covers [midnight, next midnight)
  saas_daily_default_cron  "@daily", which in Airflow 3 is a CronTriggerTimetable: the run's
                           data interval starts and ends at the same instant

Paths come from the environment so the backfill harness can point each scenario at its
own warehouse: DUCKDB_PATH, LANDING_DIR, DBT_DIR and DBT (the dbt command).
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

from airflow.sdk import dag, task
from airflow.sdk.exceptions import AirflowFailException
from airflow.timetables.interval import CronDataIntervalTimetable

ROOT = Path(__file__).resolve().parents[2]


def _env_path(name: str, default: Path) -> Path:
    return Path(os.environ.get(name, default))


def _naive(ts: datetime) -> str:
    """UTC instant as the naive timestamp the warehouse stores."""
    return ts.astimezone(UTC).strftime("%Y-%m-%d %H:%M:%S")


def build(dag_id: str, schedule, description: str):
    @dag(
        dag_id=dag_id,
        description=description,
        schedule=schedule,
        start_date=datetime(2026, 9, 1, tzinfo=UTC),
        catchup=False,
        max_active_runs=1,
        default_args={"retries": 2, "retry_delay": timedelta(minutes=5)},
        tags=["data-stack-lab"],
    )
    def pipeline():
        @task
        def land(data_interval_start=None, data_interval_end=None) -> dict:
            """Append the day's arrivals; replacing the partition makes a re-run harmless."""
            import duckdb

            day = data_interval_start.date().isoformat()
            files = sorted(
                (
                    _env_path("LANDING_DIR", ROOT / "data/landing/events") / f"received_date={day}"
                ).glob("*.parquet")
            )
            if not files:
                # Retrying in five minutes will not make a missing file appear, so fail
                # without retries; the day is re-run once its file lands, which is the
                # case the backfill measures. In production a sensor would wait here.
                raise AirflowFailException(f"no event files for {day}")
            warehouse = _env_path("DUCKDB_PATH", ROOT / "data/airflow.duckdb")
            with duckdb.connect(str(warehouse)) as con:
                con.execute("delete from raw.events_landed where received_date = ?", [day])
                con.execute(
                    "insert into raw.events_landed "
                    "select *, cast(? as date) as received_date from read_parquet(?)",
                    [day, [str(f) for f in files]],
                )
            return {"start": _naive(data_interval_start), "end": _naive(data_interval_end)}

        @task
        def transform(window: dict) -> None:
            dbt = shlex.split(os.environ.get("DBT", "")) or [
                str(Path(sys.executable).with_name("dbt"))
            ]
            variables = {
                "airflow_module": True,
                "interval_start": window["start"],
                "interval_end": window["end"],
            }
            run = subprocess.run(
                [
                    *dbt,
                    "run",
                    "--select",
                    "tag:airflow",
                    "--vars",
                    json.dumps(variables),
                    "--project-dir",
                    ".",
                    "--profiles-dir",
                    ".",
                ],  # fmt: skip
                cwd=_env_path("DBT_DIR", ROOT / "dbt"),
                capture_output=True,
                text=True,
            )
            if run.returncode != 0:
                raise RuntimeError(f"dbt run failed:\n{run.stdout[-3000:]}{run.stderr[-2000:]}")

        transform(land())

    return pipeline()


saas_daily = build(
    "saas_daily",
    CronDataIntervalTimetable("0 0 * * *", timezone="UTC"),
    "Daily event load; each run covers one day of arrivals.",
)
saas_daily_default_cron = build(
    "saas_daily_default_cron",
    "@daily",
    "The same pipeline on Airflow 3's default cron schedule.",
)
