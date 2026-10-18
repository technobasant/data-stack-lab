"""Backfill September 2026 through the Airflow DAGs and score each incremental model.

Every scenario starts from the same state (everything that arrived before 1 September,
loaded in full) and runs one DAG run per day through `dag.test()`, which executes the
real tasks in this process. Then each of the three incremental tables is compared with
every event that arrived before 1 October.

  in_order      saas_daily, 1 → 30 September, one run a day
  late_file     the same, except the file for 10 September lands only after the run for
                the 12th: the run for the 10th fails, and is re-run once the file is there
  backwards     saas_daily, 30 → 1 September: what `airflow backfill create
                --run-backwards` does, newest day first
  default_cron  saas_daily_default_cron, in order: Airflow 3's "@daily"

    python airflow/backfill.py            # write results/airflow-backfill.json
    python airflow/backfill.py --check    # fail if a figure differs from the file
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from datetime import UTC, date, datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("DATA_DIR", ROOT / "data"))
WORK = DATA / "airflow"
LANDING = DATA / "landing" / "events"
RESULT = ROOT / "results" / "airflow-backfill.json"
FIRST, LAST = date(2026, 9, 1), date(2026, 9, 30)
LATE_DAY, LANDS_AFTER = date(2026, 9, 10), date(2026, 9, 12)
TABLES = ("events_by_event_time", "events_by_arrival", "events_by_interval")

# Airflow reads its configuration at import, so it is pointed at a home inside data/
# before anything imports it. Nothing touches ~/airflow.
os.environ["AIRFLOW_HOME"] = str(WORK / "home")
os.environ["AIRFLOW__CORE__DAGS_FOLDER"] = str(ROOT / "airflow" / "dags")
os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
os.environ["AIRFLOW__LOGGING__LOGGING_LEVEL"] = "WARNING"
os.environ.setdefault("DBT_DIR", str(ROOT / "dbt"))
os.environ["LANDING_DIR"] = str(LANDING)

import duckdb  # noqa: E402


def days(first: date, last: date) -> list[date]:
    return [first + timedelta(n) for n in range((last - first).days + 1)]


def land_files() -> None:
    """Split the delivered events into one landing partition per day of arrival."""
    if LANDING.exists():
        shutil.rmtree(LANDING)
    LANDING.mkdir(parents=True)
    duckdb.sql(
        f"""
        copy (
            select *, cast(received_at as date) as received_date
            from read_parquet('{DATA / "raw" / "events.parquet"}')
        ) to '{LANDING}' (format parquet, partition_by (received_date))
        """
    )


def dbt(*args: str, warehouse: Path) -> None:
    command = os.environ.get("DBT", "").split() or [str(Path(sys.executable).with_name("dbt"))]
    run = subprocess.run(
        [*command, *args, "--project-dir", ".", "--profiles-dir", "."],
        cwd=os.environ["DBT_DIR"],
        env={**os.environ, "DUCKDB_PATH": str(warehouse)},
        capture_output=True,
        text=True,
    )
    if run.returncode != 0:
        raise SystemExit(f"dbt {' '.join(args)} failed:\n{run.stdout[-3000:]}")


def initial_state(warehouse: Path) -> None:
    """Everything that arrived before the backfill, loaded in full."""
    warehouse.unlink(missing_ok=True)
    with duckdb.connect(str(warehouse)) as con:
        con.execute("create schema raw")
        con.execute(
            f"""
            create table raw.events_landed as
            select *, cast(received_date as date) as received_date
            from read_parquet('{LANDING}/*/*.parquet', hive_partitioning = true)
            where received_date < '{FIRST}'
            """
        )
    dbt("run", "--select", "tag:airflow", "--full-refresh", "--vars", '{"airflow_module": true}',
        warehouse=warehouse)  # fmt: skip


def run_day(dag, day: date) -> str:
    """The run for `day`, fired when the scheduler would fire it: at the midnight after.

    In Airflow 3 a run's logical date is the moment it fires, so on the data-interval
    timetable this run covers [day, day + 1). On the default "@daily" it covers the
    single instant day + 1.
    """
    fired = datetime(day.year, day.month, day.day, tzinfo=UTC) + timedelta(1)
    return str(dag.test(logical_date=fired).state)


def run_scenario(name: str, dags: dict) -> dict:
    warehouse = WORK / f"{name}.duckdb"
    os.environ["DUCKDB_PATH"] = str(warehouse)
    initial_state(warehouse)
    dag = dags["saas_daily_default_cron" if name == "default_cron" else "saas_daily"]
    runs = []
    hidden = Path(tempfile.mkdtemp()) / f"received_date={LATE_DAY}"
    order = days(FIRST, LAST)
    if name == "backwards":
        order.reverse()
    for day in order:
        if name == "late_file" and day == LATE_DAY:
            shutil.move(LANDING / f"received_date={LATE_DAY}", hidden)
        runs.append({"day": str(day), "state": run_day(dag, day)})
        if name == "late_file" and day == LANDS_AFTER:
            shutil.move(hidden, LANDING / f"received_date={LATE_DAY}")
            runs.append({"day": str(LATE_DAY), "state": run_day(dag, LATE_DAY), "rerun": True})
    return {"runs": runs, "tables": score(warehouse)}


def score(warehouse: Path) -> dict:
    con = duckdb.connect(str(warehouse), read_only=True)
    con.execute(
        f"""
        create temp table truth as
        select event_id, min(event_at) as event_at, min(received_at) as received_at
        from read_parquet('{DATA / "raw" / "events.parquet"}')
        where received_at < '{LAST + timedelta(1)}'
        group by event_id
        """
    )
    out = {}
    for table in TABLES:
        out[table] = dict(
            zip(
                (
                    "rows",
                    "missing",
                    "missing_in_backfill",
                    "missing_late",
                    "duplicates",
                    "outside_window",
                ),
                con.execute(
                    f"""
                    with t as (select event_id from airflow.{table})
                    select
                        (select count(*) from t),
                        count(*) filter (where t.event_id is null),
                        count(*) filter (where t.event_id is null
                                           and truth.received_at >= '{FIRST}'),
                        count(*) filter (where t.event_id is null
                                           and truth.received_at - truth.event_at
                                               > interval 1 hour),
                        (select count(*) - count(distinct event_id) from t),
                        (select count(*) from t anti join truth using (event_id))
                    from truth left join (select distinct event_id from t) t using (event_id)
                    """
                ).fetchone(),
                strict=True,
            )
        )
    arrived = con.execute(f"select count(*) from truth where received_at >= '{FIRST}'").fetchone()
    out["arrived_in_backfill"] = arrived[0]
    out["late_in_backfill"] = con.execute(
        f"select count(*) from truth where received_at >= '{FIRST}' "
        "and received_at - event_at > interval 1 hour"
    ).fetchone()[0]
    out["truth"] = con.execute("select count(*) from truth").fetchone()[0]
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="compare with the published file")
    ap.add_argument("--scenario", action="append", help="run only these (default: all)")
    args = ap.parse_args()

    WORK.mkdir(parents=True, exist_ok=True)
    subprocess.run([sys.executable, "-m", "airflow", "db", "migrate"], check=True,
                   capture_output=True)  # fmt: skip
    land_files()

    sys.path.insert(0, os.environ["AIRFLOW__CORE__DAGS_FOLDER"])
    import saas_daily  # noqa: E402  (after AIRFLOW_HOME is set)

    dags = {d.dag_id: d for d in (saas_daily.saas_daily, saas_daily.saas_daily_default_cron)}
    result = {}
    for name in args.scenario or ["in_order", "late_file", "backwards", "default_cron"]:
        result[name] = run_scenario(name, dags)
        t = result[name]["tables"]
        states = [r["state"] for r in result[name]["runs"]]
        print(f"{name}: {states.count('success')}/{len(states)} runs succeeded; "
              f"{t['arrived_in_backfill']:,} events arrived in the backfill")  # fmt: skip
        for table in TABLES:
            m = t[table]
            print(f"  {table:<22} missing {m['missing']:>6,} "
                  f"({m['missing'] / t['arrived_in_backfill']:.2%} of the backfill's arrivals), "
                  f"late among them {m['missing_late']:,}, duplicates {m['duplicates']}, "
                  f"loaded from outside the window {m['outside_window']:,}")  # fmt: skip

    text = json.dumps(result, indent=2) + "\n"
    if args.check:
        if not RESULT.exists() or RESULT.read_text() != text:
            sys.exit(f"{RESULT.relative_to(ROOT)} does not match this run")
        print(f"matches {RESULT.relative_to(ROOT)}")
    elif not args.scenario:
        RESULT.write_text(text)


if __name__ == "__main__":
    main()
