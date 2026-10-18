"""Load the raw files into DuckDB and build the dbt project, the way it would happen live.

A warehouse sees the accounts table one extract at a time, so this replays them: for
each month it replaces raw.accounts with that month's extract and runs `dbt snapshot`.
The snapshot therefore knows only what the extracts showed. Then it loads the other
sources and runs `dbt build`: every model, data test and unit test.

    python scripts/warehouse.py
    DBT="uvx --from dbt-oss==2.0.8 dbt" python scripts/warehouse.py   # on dbt v2

Exits non-zero if any dbt step fails.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import sys
import time
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("DATA_DIR", ROOT / "data"))
WAREHOUSE = Path(os.environ.get("DUCKDB_PATH", DATA / "warehouse.duckdb")).resolve()
DBT_DIR = ROOT / "dbt"
SOURCES = ("plans", "users", "invoices", "events")


def dbt_command() -> list[str]:
    if os.environ.get("DBT"):
        return shlex.split(os.environ["DBT"])
    return [str(Path(sys.executable).with_name("dbt"))]


def dbt(*args: str) -> float:
    started = time.perf_counter()
    cmd = [*dbt_command(), *args, "--project-dir", ".", "--profiles-dir", "."]
    env = {**os.environ, "DUCKDB_PATH": str(WAREHOUSE)}
    run = subprocess.run(cmd, cwd=DBT_DIR, env=env, capture_output=True, text=True)
    if run.returncode != 0:
        sys.stdout.write(run.stdout + run.stderr)
        raise SystemExit(f"dbt {' '.join(args)} failed ({run.returncode})")
    return time.perf_counter() - started


def load(table: str, parquet: Path) -> None:
    with duckdb.connect(str(WAREHOUSE)) as con:
        con.execute("create schema if not exists raw")
        con.execute(
            f"create or replace table raw.{table} as select * from read_parquet(?)",
            [str(parquet)],
        )


def main() -> None:
    extracts = sorted((DATA / "raw" / "accounts").glob("extract_date=*.parquet"))
    if not extracts:
        raise SystemExit(f"no extracts under {DATA / 'raw'}; run `python -m datagen` first")
    WAREHOUSE.parent.mkdir(parents=True, exist_ok=True)
    for stale in (WAREHOUSE, WAREHOUSE.with_name(WAREHOUSE.name + ".wal")):
        stale.unlink(missing_ok=True)

    for extract in extracts:
        load("accounts", extract)
        took = dbt("snapshot")
        print(f"snapshot  {extract.stem.split('=')[1]}  {took:5.1f} s", flush=True)
    for name in SOURCES:
        load(name, DATA / "raw" / f"{name}.parquet")
    took = dbt("build", "--exclude", "resource_type:snapshot")
    print(f"build     models, data tests, unit tests  {took:5.1f} s")


if __name__ == "__main__":
    main()
