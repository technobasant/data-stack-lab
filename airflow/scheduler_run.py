"""The in-order backfill on a real scheduler, not dag.test().

airflow/backfill.py runs each day through `dag.test()`, in one process. This runs the same
September backfill the way a team would: `airflow standalone` (scheduler, API server and
DAG processor, SQLite metadata, LocalExecutor) and `airflow backfill create`, one run at a
time, then scores the tables exactly as backfill.py does and compares them with the
in-order `dag.test()` result in results/airflow-backfill.json.

    python airflow/scheduler_run.py     # writes results/airflow-scheduler-run.json

Needs the `airflow` group (`uv sync --group airflow`) and the generated data (`make data`).
Everything it writes is under data/airflow/: its own Airflow home, warehouse and log.
Unpausing the DAG also lets the scheduler run the latest interval (catchup is off); that
run has no file to land and fails at its first task, which the record notes.
"""

from __future__ import annotations

import json
import os
import shutil
import signal
import sqlite3
import subprocess
import sys
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
sys.path.insert(0, str(HERE))
import backfill  # noqa: E402  sets up paths; scoring and the initial state are shared

HOME = backfill.WORK / "standalone"
WAREHOUSE = backfill.WORK / "scheduler.duckdb"
LOG = backfill.WORK / "standalone.log"
RESULT = backfill.ROOT / "results" / "airflow-scheduler-run.json"
AIRFLOW = str(Path(sys.executable).with_name("airflow"))
BACKFILL = ["backfill", "create", "--dag-id", "saas_daily", "--from-date", str(backfill.FIRST),
            "--to-date", str(backfill.LAST), "--max-active-runs", "1"]  # fmt: skip


def environment() -> dict:
    return {
        **os.environ,
        "AIRFLOW_HOME": str(HOME),
        "AIRFLOW__CORE__DAGS_FOLDER": str(HERE / "dags"),
        "AIRFLOW__CORE__LOAD_EXAMPLES": "False",
        "AIRFLOW__API__PORT": "8099",
        "AIRFLOW__API__BASE_URL": "http://127.0.0.1:8099",
        "DUCKDB_PATH": str(WAREHOUSE),
        "LANDING_DIR": str(backfill.LANDING),
        "DBT_DIR": str(backfill.ROOT / "dbt"),
        "DBT": os.environ.get("DBT", str(Path(sys.executable).with_name("dbt"))),
    }


def airflow(*args: str) -> str:
    run = subprocess.run([AIRFLOW, *args], env=environment(), capture_output=True, text=True)
    if run.returncode != 0:
        raise SystemExit(f"airflow {' '.join(args)} failed:\n{(run.stderr or run.stdout)[-2000:]}")
    return run.stdout


def runs() -> list[dict]:
    with sqlite3.connect(HOME / "airflow.db") as db:
        db.row_factory = sqlite3.Row
        rows = db.execute(
            "select run_type, state, count(*) as runs, min(start_date) as first_start, "
            "max(end_date) as last_end from dag_run where dag_id = 'saas_daily' "
            "group by 1, 2 order by 1, 2"
        ).fetchall()
    return [dict(r) for r in rows]


def backfill_done() -> bool:
    finished = sum(
        r["runs"]
        for r in runs()
        if r["run_type"] == "backfill" and r["state"] in ("success", "failed")
    )
    return finished == len(backfill.days(backfill.FIRST, backfill.LAST))


def main() -> None:
    backfill.land_files()
    os.environ["DUCKDB_PATH"] = str(WAREHOUSE)
    backfill.initial_state(WAREHOUSE)
    shutil.rmtree(HOME, ignore_errors=True)
    HOME.mkdir(parents=True)
    airflow("db", "migrate")
    airflow("dags", "reserialize")
    version = airflow("version").strip()
    # SQLite takes one writer at a time, so everything the backfill needs is written before
    # the scheduler starts: the DAG unpaused and the backfill's runs queued. The scheduler
    # then picks them up, one at a time.
    airflow("dags", "unpause", "saas_daily")
    airflow(*BACKFILL)
    with LOG.open("w") as log:
        standalone = subprocess.Popen([AIRFLOW, "standalone"], env=environment(), stdout=log,
                                      stderr=subprocess.STDOUT, start_new_session=True)  # fmt: skip
    try:
        deadline = time.monotonic() + 3600
        while not backfill_done():
            if time.monotonic() > deadline or standalone.poll() is not None:
                raise SystemExit(f"the backfill did not finish; see {LOG}")
            time.sleep(10)
    finally:
        os.killpg(standalone.pid, signal.SIGTERM)
        standalone.wait(timeout=120)
    tables = backfill.score(WAREHOUSE)
    published = json.loads(backfill.RESULT.read_text())["in_order"]["tables"]
    today = time.strftime("%Y-%m-%d", time.gmtime())
    record = {
        "what": f"September backfilled on a real scheduler, not dag.test(): airflow standalone "
                f"(Airflow {version}, SQLite metadata, LocalExecutor) and "
                f"`airflow {' '.join(BACKFILL)}`, run on {today}",
        "runs": runs(),
        "note": "Unpausing the DAG (catchup=False) also scheduled the latest interval; it failed "
                "at land because no file exists for that day, so it never reached the models.",
        "tables": tables,
        "identical_to_in_order_dag_test": tables == published,
    }  # fmt: skip
    RESULT.write_text(json.dumps(record, indent=2) + "\n")
    print("identical to the in-order dag.test() run:", record["identical_to_in_order_dag_test"])


if __name__ == "__main__":
    main()
