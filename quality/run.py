"""A month of daily deliveries with nine defects, checked in dbt, under two policies.

August is delivered clean and accepted, to give the batch checks a baseline. Then each
day of September is delivered as its producer would send it, with one defect every
three days (DEFECTS below), and checked by models/quality: contract, row-level tests,
batch-level tests against the last seven accepted days, and control totals against the
producer's manifest. Two policies decide what is promoted:

  block       any failing check holds the whole day for a person
  quarantine  rows that fail a row-level check are set aside and the rest promoted;
              a failing batch-level check still holds the whole day

Every delivered row is labelled good or bad by the defect that produced it, so the
result counts what each policy let through and what it held back.

    python quality/run.py            # write results/quality.json
    python quality/run.py --check    # fail if a figure differs from the file
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
import tempfile
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("DATA_DIR", ROOT / "data"))
WORK = DATA / "quality"
LANDING = DATA / "landing" / "events"
RESULT = ROOT / "results" / "quality.json"
SEPT = [date(2026, 9, 1) + timedelta(n) for n in range(30)]
AUGUST = [date(2026, 8, 1) + timedelta(n) for n in range(31)]

# day -> (defect id, what happened). Every other September day is delivered clean.
DEFECTS = {
    date(2026, 9, 3): ("duplicate_file", "the day's file is delivered twice"),
    date(2026, 9, 6): ("null_account", "5% of events arrive with no account_id"),
    date(2026, 9, 9): ("renamed_column", "the producer renames event_type to type"),
    date(2026, 9, 12): ("truncated_file", "an upload stops at 40% of the file"),
    date(2026, 9, 15): ("local_time", "event times written in UTC+5 but labelled UTC"),
    date(2026, 9, 18): ("new_event_type", "3% of events have a new, legitimate type: export_pdf"),
    date(2026, 9, 21): ("platform_silent", "the iOS SDK stops sending"),
    date(2026, 9, 24): ("orphan_accounts", "2% of events carry an account_id that does not exist"),
    date(2026, 9, 27): (
        "late_third",
        "a third of the day's events arrive two days late, with the 29th",
    ),
}
LATE_ARRIVES = date(2026, 9, 29)

ROW_CHECKS = {  # dbt test name prefix -> SQL predicate selecting the offending rows
    "not_null_stg_quality__batch_event_id": "event_id is null",
    "not_null_stg_quality__batch_account_id": "account_id is null",
    "not_null_stg_quality__batch_event_at": "event_at is null",
    "relationships_stg_quality__batch_account_id": (
        "account_id not in (select account_id from raw.accounts)"
    ),
    "accepted_values_stg_quality__batch_event_type": (
        "event_type not in ('login', 'view_dashboard', 'create_report', 'share_report', "
        "'export_csv', 'invite_user', 'api_call')"
    ),
    "accepted_values_stg_quality__batch_platform": "platform not in ('web', 'ios', 'android')",
    "happened_before_arrival": "event_at > received_at + interval 5 minutes",
}
BATCH_CHECKS = ("volume_near_baseline", "platform_share_near_baseline", "matches_manifest")


def dbt_command() -> list[str]:
    return shlex.split(os.environ.get("DBT", "")) or [str(Path(sys.executable).with_name("dbt"))]


def clean_day(con: duckdb.DuckDBPyConnection, day: date) -> None:
    con.execute(
        "create or replace temp table clean as select * exclude (received_date) "
        f"from read_parquet('{LANDING}/received_date={day}/*.parquet')"
    )


def deliver(con: duckdb.DuckDBPyConnection, day: date, held: list) -> None:
    """Write the day's delivery to raw.events_delivered and raw.delivery_manifests, with
    a `bad` label per row from what the defect did to it."""
    clean_day(con, day)
    defect = DEFECTS.get(day, (None,))[0]
    f = f"events-{day}.parquet"
    base = "select *, false as bad, 1 as delivery_copy from clean"
    files = [(f, base)]
    nth = "row_number() over (order by event_id)"
    tail = "1 as delivery_copy from clean"
    if defect == "duplicate_file":
        # The same rows again under another name: harmless once deduplicated, so not "bad".
        files = [(f, base), (f"events-{day}-resent.parquet",
                             "select *, false as bad, 2 as delivery_copy from clean")]  # fmt: skip
    elif defect == "null_account":
        files = [(f, f"select * replace (case when {nth} % 20 = 0 then null else account_id end "
                     f"as account_id), {nth} % 20 = 0 as bad, {tail}")]  # fmt: skip
    elif defect == "renamed_column":
        files = [
            (f, "select * rename (event_type as type), false as bad, 1 as delivery_copy from clean")
        ]
    elif defect == "truncated_file":
        files = [(f, f"select *, false as bad, 1 as delivery_copy from clean "
                     f"qualify {nth} <= 0.4 * count(*) over ()")]  # fmt: skip
    elif defect == "local_time":
        files = [(f, "select * replace (event_at + interval 5 hours as event_at), true as bad, "
                     "1 as delivery_copy from clean")]  # fmt: skip
    elif defect == "new_event_type":
        files = [(f, f"select * replace (case when {nth} % 33 = 0 then 'export_pdf' "
                     f"else event_type end as event_type), false as bad, {tail}")]  # fmt: skip
    elif defect == "platform_silent":
        files = [
            (f, "select *, false as bad, 1 as delivery_copy from clean where platform <> 'ios'")
        ]
    elif defect == "orphan_accounts":
        files = [
            (
                f,
                f"select * replace (case when {nth} % 50 = 0 then account_id + 1000000 "
                f"else account_id end as account_id), {nth} % 50 = 0 as bad, {tail}",
            )
        ]
    elif defect == "late_third":
        files = [
            (f, f"select *, false as bad, 1 as delivery_copy from clean qualify {nth} % 3 <> 0")
        ]
        # A real table, not a temp one: the connection is closed while dbt runs.
        con.execute(f"create or replace table quality_state.late_rows as select *, false as bad, "
                    f"1 as delivery_copy from clean qualify {nth} % 3 = 0")  # fmt: skip
        held.append(day)
    if day == LATE_ARRIVES and held:
        files.append((f"events-{held[0]}-late.parquet", "select * from quality_state.late_rows"))

    con.execute("drop table if exists raw.events_delivered")
    parts = [f"select *, '{name}' as file_name from ({sql})" for name, sql in files]
    con.execute("create table raw.events_delivered as " + " union all by name ".join(parts))
    # The producer's manifest counts what it meant to send: a truncated upload still
    # declares the whole file.
    rows = {name: con.execute(f"select count(*) from ({sql})").fetchone()[0] for name, sql in files}
    if defect == "truncated_file":
        rows[f] = con.execute("select count(*) from clean").fetchone()[0]
    con.execute(
        "create or replace table raw.delivery_manifests (file_name varchar, row_count bigint)"
    )
    con.executemany("insert into raw.delivery_manifests values (?, ?)", list(rows.items()))


def check(warehouse: Path) -> tuple[bool, set[str], set[str]]:
    """Run the day's checks. Returns (contract held, failed tests, warnings).

    Each invocation gets its own --target-path, so a run that crashes before writing
    run_results.json is never mistaken for the previous day's (passing) one."""
    with tempfile.TemporaryDirectory() as target_path:
        run = subprocess.run(
            [
                *dbt_command(),
                "build",
                "--select",
                "tag:quality",
                "--vars",
                '{"quality_module": true}',
                "--project-dir",
                ".",
                "--profiles-dir",
                ".",
                "--target-path",
                target_path,
            ],  # fmt: skip
            cwd=ROOT / "dbt",
            env={**os.environ, "DUCKDB_PATH": str(warehouse)},
            capture_output=True,
            text=True,
        )
        run_results = Path(target_path) / "run_results.json"
        if not run_results.exists():
            raise SystemExit(
                f"dbt produced no run_results.json (exit {run.returncode}):\n"
                f"{run.stdout[-2000:]}\n{run.stderr[-2000:]}"
            )
        results = json.loads(run_results.read_text())["results"]
    model_ok, failed, warned = True, set(), set()
    for r in results:
        kind, _, name = r["unique_id"].split(".")[:3]
        if kind == "model" and r["status"] != "success":
            model_ok = False
        elif kind == "test" and r["status"] in ("fail", "error"):
            failed.add(name)
        elif kind == "test" and r["status"] == "warn":
            warned.add(name)
    if run.returncode != 0 and not failed:
        # dbt exited non-zero but no test was recorded as failed: do not treat the
        # build as having silently succeeded.
        model_ok = False
    return model_ok, failed, warned


BATCH_ROWS = """
    create or replace temp table batch_rows as
    select * exclude (copy) from (
        select *, row_number() over (
            partition by event_id order by received_at, delivery_copy
        ) as copy
        from raw.events_delivered
    ) where copy = 1 and event_id not in (select event_id from quality_state.accepted_events)
"""


def promote(con: duckdb.DuckDBPyConnection, day: date, where: str) -> None:
    """Promote the day's new, deduplicated rows that satisfy `where`; the model's own rule."""
    con.execute(BATCH_ROWS)
    con.execute(f"create or replace temp table promoted as select * from batch_rows where {where}")
    con.execute(
        "insert into quality_state.accepted_events select event_id, ?, platform, bad from promoted",
        [day],
    )
    con.execute(
        "insert into quality_state.accepted_daily "
        "select ?, platform, count(*) from promoted group by platform "
        "union all select ?, 'all', count(*) from promoted",
        [day, day],
    )


def run_policy(policy: str) -> dict:
    warehouse = WORK / f"{policy}.duckdb"
    warehouse.unlink(missing_ok=True)
    con = duckdb.connect(str(warehouse))
    con.execute("create schema raw")
    con.execute("create schema quality_state")
    con.execute(
        "create table raw.accounts as select * from "
        f"read_parquet('{DATA}/raw/accounts/extract_date=2026-10-01.parquet')"
    )
    con.execute("create table quality_state.accepted_events "
                "(event_id varchar, day date, platform varchar, bad boolean)")  # fmt: skip
    con.execute(
        "create table quality_state.accepted_daily (day date, platform varchar, rows bigint)"
    )
    for day in AUGUST:
        clean_day(con, day)
        con.execute("create or replace table raw.events_delivered as "
                    "select *, false as bad, 1 as delivery_copy from clean")  # fmt: skip
        promote(con, day, "true")

    days, held = [], []
    for day in SEPT:
        deliver(con, day, held)
        con.execute("create or replace temp table expected as select distinct event_id from clean")
        expected = con.execute(
            "select count(*) from expected where event_id not in "
            "(select event_id from quality_state.accepted_events)"
        ).fetchone()[0]
        con.close()
        model_ok, failed, warned = check(warehouse)
        con = duckdb.connect(str(warehouse))
        raw_rows = con.execute("select count(*) from raw.events_delivered").fetchone()[0]
        con.execute(BATCH_ROWS)
        delivered, bad = con.execute(
            "select count(*), count(*) filter (where bad) from batch_rows"
        ).fetchone()
        row_failures = [n for n in failed if any(n.startswith(p) for p in ROW_CHECKS)]
        batch_failures = [n for n in failed if any(n.startswith(p) for p in BATCH_CHECKS)]
        if not model_ok or batch_failures or (policy == "block" and row_failures):
            decision, where = "held", "false"
        elif row_failures:
            predicates = [
                p
                for prefix, p in ROW_CHECKS.items()
                if any(n.startswith(prefix) for n in row_failures)
            ]
            decision, where = (
                "quarantined",
                " and ".join(f"not coalesce({p}, true)" for p in predicates),
            )
        else:
            decision, where = "promoted", "true"
        if decision == "held":
            promoted = bad_promoted = 0
        else:
            promote(con, day, where)
            promoted, bad_promoted = con.execute(
                "select count(*), count(*) filter (where bad) from promoted"
            ).fetchone()
        days.append({
            "day": str(day),
            "defect": DEFECTS.get(day, ("late_rows_arrive" if day == LATE_ARRIVES else "none",))[0],
            "contract_held": model_ok,
            "failed_checks": sorted(failed),
            "warnings": sorted(warned),
            "decision": decision,
            "raw_rows": raw_rows,
            "delivered": delivered,
            "missing_from_delivery": max(0, expected - delivered) if day != LATE_ARRIVES else 0,
            "bad_delivered": bad,
            "promoted": promoted,
            "bad_promoted": bad_promoted,
            "good_not_promoted": (delivered - bad) - (promoted - bad_promoted),
        })  # fmt: skip
    con.close()
    return {"days": days, "totals": totals(days)}


def totals(days: list[dict]) -> dict:
    clean = [d for d in days if d["defect"] == "none"]
    return {
        "bad_delivered": sum(d["bad_delivered"] for d in days),
        "bad_promoted": sum(d["bad_promoted"] for d in days),
        "good_not_promoted": sum(d["good_not_promoted"] for d in days),
        "missing_from_delivery": sum(d["missing_from_delivery"] for d in days),
        "days_held": sum(d["decision"] == "held" for d in days),
        "clean_days_with_a_warning": sum(bool(d["warnings"]) for d in clean),
        "clean_days": len(clean),
        "clean_days_with_an_alarm": sum(
            bool(d["failed_checks"]) or not d["contract_held"] for d in clean
        ),
    }


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="compare with the published file")
    args = ap.parse_args()
    if not LANDING.exists():
        # The same split the Airflow project uses: one partition per day of arrival.
        LANDING.mkdir(parents=True)
        duckdb.sql(
            "copy (select *, cast(received_at as date) as received_date "
            f"from read_parquet('{DATA / 'raw' / 'events.parquet'}')) "
            f"to '{LANDING}' (format parquet, partition_by (received_date))"
        )
    WORK.mkdir(parents=True, exist_ok=True)
    result = {}
    for policy in ("block", "quarantine"):
        result[policy] = run_policy(policy)
        t = result[policy]["totals"]
        print(
            f"{policy}: bad rows promoted {t['bad_promoted']:,} of {t['bad_delivered']:,}; "
            f"good rows not promoted {t['good_not_promoted']:,}; days held {t['days_held']}; "
            f"clean days with an alarm {t['clean_days_with_an_alarm']}/{t['clean_days']}"
        )
        for d in result[policy]["days"]:
            if d["defect"] != "none" or d["failed_checks"] or d["warnings"]:
                checks = ", ".join(d["failed_checks"]) or (
                    "contract" if not d["contract_held"] else "nothing"
                )
                warn = f"; warned: {', '.join(d['warnings'])}" if d["warnings"] else ""
                print(
                    f"  {d['day']} {d['defect']:<16} {d['decision']:<11} caught by: {checks}{warn}"
                )
    text = json.dumps(result, indent=2) + "\n"
    if args.check:
        if not RESULT.exists() or RESULT.read_text() != text:
            sys.exit(f"{RESULT.relative_to(ROOT)} does not match this run")
        print(f"matches {RESULT.relative_to(ROOT)}")
    else:
        RESULT.write_text(text)


if __name__ == "__main__":
    main()
