"""The dbt project, unchanged, on Databricks: the same replay, the same build, the same score.

scripts/warehouse.py builds the project on DuckDB by replaying 13 monthly account extracts
through `dbt snapshot` and then running `dbt build`. This does the same on a Databricks SQL
warehouse, in the catalog `dsl`: the raw files go to a Unity Catalog volume, each extract
replaces dsl.raw.accounts before its snapshot, and the other sources are loaded before the
build. Then the revenue questions of scripts/measure.py are asked of the Databricks marts
with the same SQL, and the answers compared with the published DuckDB figures in
results/dbt-scd2.json.

    python databricks/dbt_build.py      # writes results/dbt-databricks.json

dbt runs as `uvx --from dbt-databricks==1.12.7 --with dbt-core==1.12.5 dbt` (set DBT to
override): the adapter pins an older Databricks SDK than the loader project, so it runs
in its own environment. dbt-core is pinned explicitly rather than left to whatever uvx
resolves, because dbt-databricks 1.12.7 does not pin an exact dbt-core version itself;
1.12.5 is what it resolved to when this was last checked (`uvx --from
dbt-databricks==1.12.7 dbt --version`).
Needs DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_HTTP_PATH.
"""

from __future__ import annotations

import json
import os
import shlex
import subprocess
import sys
import tempfile
import time
from pathlib import Path

HERE = Path(__file__).resolve().parent
ROOT = HERE.parent
sys.path.insert(0, str(HERE))
from run import DATA, Workspace  # noqa: E402

RESULT = ROOT / "results" / "dbt-databricks.json"
DUCKDB_RESULT = ROOT / "results" / "dbt-scd2.json"
DBT_DIR = ROOT / "dbt"
CATALOG = "dsl"
FILES = f"/Volumes/{CATALOG}/raw/files"
SOURCES = ("plans", "users", "invoices", "events")
TRUTH = ("invoice_plans", "account_history")
# What the project needed to build here. The first run failed on the first; it is the only one.
CHANGES = [
    "model contracts: data_type varchar became string (13 columns). dbt checks a contract by "
    "casting null to each declared type, and Databricks rejects VARCHAR without a length "
    "(DATATYPE_MISSING_SIZE); string means the same on DuckDB, Databricks, Snowflake and BigQuery"
]


DEFAULT_DBT = "uvx --from dbt-databricks==1.12.7 --with dbt-core==1.12.5 dbt"


def dbt(*args: str) -> tuple[float, dict]:
    """Run one dbt invocation with its own --target-path, so a crash that leaves no
    run_results.json is never mistaken for a previous invocation's (passing) one."""
    command = shlex.split(os.environ.get("DBT", DEFAULT_DBT))
    started = time.perf_counter()
    with tempfile.TemporaryDirectory() as target_path:
        run = subprocess.run(
            [*command, *args, "--target", "databricks", "--project-dir", ".",
             "--profiles-dir", ".", "--target-path", target_path],
            cwd=DBT_DIR, capture_output=True, text=True,
        )  # fmt: skip
        took = round(time.perf_counter() - started, 1)
        run_results = Path(target_path) / "run_results.json"
        results = json.loads(run_results.read_text())["results"] if run_results.exists() else []
    statuses: dict = {}
    for r in results:
        kind = r["unique_id"].split(".")[0]
        statuses.setdefault(kind, {}).setdefault(r["status"], 0)
        statuses[kind][r["status"]] += 1
    failed = [{"node": r["unique_id"], "message": (r.get("message") or "")[:400]}
              for r in results if r["status"] in ("error", "fail")]  # fmt: skip
    if run.returncode != 0 and not failed:
        failed = [{"node": "dbt", "message": (run.stdout + run.stderr)[-1500:]}]
    return took, {"statuses": statuses, "failed": failed}


def load(ws: Workspace, table: str, path: str) -> None:
    ws.sql(f"CREATE OR REPLACE TABLE {CATALOG}.raw.{table} AS "
           f"SELECT * FROM read_files('{path}', format => 'parquet')")  # fmt: skip


def put_once(ws: Workspace, local: Path, remote: str) -> None:
    if not ws.exists(remote, local.stat().st_size):
        ws.put(local, remote)


REVENUE = f"""
with attributed as (
    select f.invoice_id, f.amount_usd, billed.plan_name as type2_plan,
           cur.plan_name as type1_plan, p.plan_name as true_plan
    from {CATALOG}.marts.fct_invoices f
    join {CATALOG}.marts.dim_account billed on billed.account_key = f.account_key
    join {CATALOG}.marts.dim_account cur on cur.account_id = f.account_id and cur.is_current
    join {CATALOG}.raw.invoice_plans t on t.invoice_id = f.invoice_id
    join {CATALOG}.marts.dim_plan p on p.plan_id = t.plan_id
)
select count(*) as invoices,
       sum(amount_usd)::double as revenue_usd,
       count(*) filter (where type2_plan = true_plan) as type2_invoices_right,
       count(*) filter (where type1_plan = true_plan) as type1_invoices_right,
       coalesce(sum(amount_usd) filter (where type2_plan <> true_plan), 0)::double
           as type2_usd_on_wrong_plan,
       sum(amount_usd) filter (where type1_plan <> true_plan)::double as type1_usd_on_wrong_plan
from attributed
"""

MODEL = f"""
with v as (
    select *, lag(plan_id) over w as prev_plan, lag(status) over w as prev_status,
           lag(country) over w as prev_country
    from {CATALOG}.marts.dim_account
    window w as (partition by account_id order by valid_from)
)
select count(distinct account_id) as accounts,
       count(*) as account_versions,
       count(*) filter (where version_number > 1 and plan_id = prev_plan
                          and status = prev_status and country = prev_country)
           as versions_same_as_previous
from v
"""


MODEL_COLUMNS = ("accounts", "account_versions", "versions_same_as_previous")
REVENUE_COLUMNS = ("invoices", "revenue_usd", "type2_invoices_right", "type1_invoices_right",
                   "type2_usd_on_wrong_plan", "type1_usd_on_wrong_plan")  # fmt: skip


def score(ws: Workspace) -> dict:
    model = ws.sql(MODEL)[0]
    revenue = ws.sql(REVENUE)[0]
    return {
        "model": {k: float(v) for k, v in zip(MODEL_COLUMNS, model, strict=True)},
        "revenue": {k: float(v) for k, v in zip(REVENUE_COLUMNS, revenue, strict=True)},
    }


def main() -> None:
    ws = Workspace()
    ws.sql(f"CREATE CATALOG IF NOT EXISTS {CATALOG}")
    ws.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.raw")
    ws.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.raw.files")
    for schema in ("staging", "snapshots", "marts"):
        ws.sql(f"DROP SCHEMA IF EXISTS {CATALOG}.{schema} CASCADE")
    extracts = sorted((DATA / "raw" / "accounts").glob("extract_date=*.parquet"))
    for f in extracts:
        put_once(ws, f, f"{FILES}/accounts/{f.name}")
    for name in SOURCES:
        put_once(ws, DATA / "raw" / f"{name}.parquet", f"{FILES}/{name}.parquet")
    for name in TRUTH:
        put_once(ws, DATA / "truth" / f"{name}.parquet", f"{FILES}/truth/{name}.parquet")
    version = ws.sql("select current_version().dbsql_version")[0][0]
    result: dict = {"databricks_sql": version, "adapter": "dbt-databricks 1.12.7",
                    "changes_to_the_project": CHANGES, "snapshots": []}  # fmt: skip
    for f in extracts:
        load(ws, "accounts", f"{FILES}/accounts/{f.name}")
        took, outcome = dbt("snapshot")
        result["snapshots"].append({"extract": f.stem.split("=")[1], "seconds": took, **outcome})
        print(f"snapshot {f.stem.split('=')[1]} {took:6.1f} s {outcome['statuses']}", flush=True)
        if outcome["failed"]:
            print(json.dumps(outcome["failed"], indent=1)[:3000])
            break
    else:
        for name in SOURCES:
            load(ws, name, f"{FILES}/{name}.parquet")
        for name in TRUTH:
            load(ws, name, f"{FILES}/truth/{name}.parquet")
        took, outcome = dbt("build", "--exclude", "resource_type:snapshot")
        result["build"] = {"seconds": took, **outcome}
        print(f"build {took:6.1f} s {outcome['statuses']}", flush=True)
        if outcome["failed"]:
            print(json.dumps(outcome["failed"], indent=1)[:3000])
        else:
            result["score"] = score(ws)
            duck = json.loads(DUCKDB_RESULT.read_text())
            got = result["score"]
            same = {k: got["model"][k] == float(duck["model"][k]) for k in MODEL_COLUMNS}
            for k in REVENUE_COLUMNS:
                same[k] = abs(got["revenue"][k] - float(duck["revenue"][k])) < 0.005
            result["matches_duckdb"] = same
            print(json.dumps(result["score"]), "\nmatches duckdb:", result["matches_duckdb"])
    RESULT.write_text(json.dumps(result, indent=2) + "\n")
    print(f"wrote {RESULT.relative_to(ROOT)}")


if __name__ == "__main__":
    main()
