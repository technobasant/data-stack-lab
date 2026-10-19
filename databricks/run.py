"""Loading the same daily files on Databricks six ways, and re-running one day on Delta.

Ten days of September's events are delivered as one file a day into a Unity Catalog
volume, with four things that happen to real deliveries:

  2 Sep   the day's file is re-sent under another name
  4 Sep   the upload stops at 40%; on 5 Sep the producer re-sends the complete file
          under the same name
  6 Sep   a third of the day's events arrive two days later, in their own file
  9 Sep   the day's load is run twice (an operator re-run after a timeout)

Six loaders run after each day's delivery, and their final tables are scored against
the events that were meant to arrive:

  insert_by_name           INSERT INTO ... SELECT FROM read_files('events-<day>*')
  copy_into                COPY INTO over the whole folder
  copy_into_merge          copy_into, then MERGE on event_id into a second table
  auto_loader              a streaming table over the folder (Auto Loader), in a
                           Lakeflow pipeline updated once per run
  auto_loader_overwrites   the same with cloudFiles.allowOverwrites = true, same pipeline
  auto_loader_merge        auto_loader_overwrites, then MERGE on event_id

Each loader's outcome is also predicted locally, from the documented semantics alone
(`plan`). CI checks the plan against results/databricks-plan.json with no workspace;
the workspace run (`load`) records what Databricks did in results/databricks.json and
fails if it differs from the prediction.

The re-run experiment (`rerun`) builds the daily rollup from the Spark project as a
Delta table partitioned by day, recomputes one day and writes it back five ways.

    python databricks/run.py --plan            # write results/databricks-plan.json
    python databricks/run.py --plan --check    # what CI runs
    python databricks/run.py                   # plan, then load and rerun on the workspace

The workspace run needs DATABRICKS_HOST, DATABRICKS_TOKEN and DATABRICKS_HTTP_PATH (a
SQL warehouse) in the environment.
"""

from __future__ import annotations

import argparse
import io
import json
import os
import re
import sys
import time
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("DATA_DIR", ROOT / "data"))
WORK = DATA / "databricks"
PLAN = ROOT / "results" / "databricks-plan.json"
RESULT = ROOT / "results" / "databricks.json"
OBSERVED = ROOT / "results" / "databricks-observed.json"

SCHEMA = os.environ.get("DATABRICKS_SCHEMA", "workspace.data_stack_lab")
CATALOG, SCHEMA_NAME = SCHEMA.split(".")
VOLUME = "/Volumes/" + SCHEMA.replace(".", "/")
LANDING = f"{VOLUME}/landing/events"
SOURCE = f"{VOLUME}/source"

DAYS = [date(2026, 9, 1) + timedelta(n) for n in range(10)]
DUPLICATE_DAY, TRUNCATED_DAY, LATE_DAY, RERUN_DAY = (date(2026, 9, d) for d in (2, 4, 6, 9))
ROLLUP_DAY = "2026-09-15"
COLUMNS = "event_id, user_id, account_id, event_type, platform, event_at, received_at"
# Both Auto Loader tables live in one Lakeflow pipeline, updated once per run: Free Edition
# allows one active pipeline per type, and a streaming table made with CREATE STREAMING
# TABLE is a pipeline of its own.
PIPELINE = "data-stack-lab-auto-loader"
PIPELINE_SOURCE = "/Workspace/Shared/data-stack-lab/auto_loader.sql"
STREAMS = ("auto_loader", "auto_loader_overwrites")
LOADERS = (
    "insert_by_name",
    "copy_into",
    "copy_into_merge",
    "auto_loader",
    "auto_loader_overwrites",
    "auto_loader_merge",
)


@dataclass(frozen=True)
class Delivery:
    name: str  # file name in the landing folder
    version: str  # which content: one parquet file under data/databricks/files/


def schedule() -> list[tuple[date, list[Delivery]]]:
    """Each load run in order, with the files delivered just before it."""
    runs = []
    for day in DAYS:
        name = f"events-{day}.parquet"
        files = [Delivery(name, f"{day}")]
        if day == DUPLICATE_DAY:
            files.append(Delivery(f"events-{day}-resent.parquet", f"{day}"))
        if day == TRUNCATED_DAY:
            files = [Delivery(name, f"{day}-first-40pct")]
        if day == TRUNCATED_DAY + timedelta(1):
            files.append(Delivery(f"events-{TRUNCATED_DAY}.parquet", f"{TRUNCATED_DAY}"))
        if day == LATE_DAY:
            files = [Delivery(name, f"{day}-on-time")]
        if day == LATE_DAY + timedelta(2):
            files.append(Delivery(f"events-{LATE_DAY}-late.parquet", f"{LATE_DAY}-late"))
        runs.append((day, files))
        if day == RERUN_DAY:
            runs.append((day, []))  # the same day's load again, nothing new delivered
    return runs


def build_files() -> dict[str, list[str]]:
    """Write every file version and return its event ids. Each event appears once across
    the ten days (its first delivery), so every duplicate in a table comes from loading."""
    files = WORK / "files"
    files.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect()
    con.execute("set TimeZone = 'UTC'")
    con.execute(
        f"""create table days as
        select event_id, user_id, account_id, event_type, platform,
               event_at::timestamptz as event_at, received_at::timestamptz as received_at,
               cast(received_at as date) as received_date
        from read_parquet('{DATA / "raw" / "events.parquet"}')
        where cast(received_at as date) between '{DAYS[0]}' and '{DAYS[-1]}'
        qualify row_number() over (partition by event_id order by received_at) = 1"""
    )
    nth = "row_number() over (order by event_id)"
    versions = {}
    for day in DAYS:
        one_day = f"(select * exclude (received_date) from days where received_date = '{day}')"
        versions[f"{day}"] = f"select * from {one_day}"
        if day == TRUNCATED_DAY:
            versions[f"{day}-first-40pct"] = (
                f"select * from {one_day} qualify {nth} <= floor(0.4 * count(*) over ())"
            )
        if day == LATE_DAY:
            versions[f"{day}-on-time"] = f"select * from {one_day} qualify {nth} % 3 <> 0"
            versions[f"{day}-late"] = f"select * from {one_day} qualify {nth} % 3 = 0"
    ids = {}
    for version, sql in versions.items():
        con.execute(
            f"copy ({sql} order by event_id) to '{files / version}.parquet' (format parquet)"
        )
        ids[version] = [r[0] for r in con.execute(f"select event_id from ({sql})").fetchall()]
    # Every day's complete file: what should be in the table after the last run.
    expected = "select event_id from days order by event_id"
    con.execute(f"copy ({expected}) to '{files / 'expected'}.parquet' (format parquet)")
    ids["expected"] = [r[0] for r in con.execute(expected).fetchall()]
    return ids


def predict(ids: dict[str, list[str]]) -> dict:
    """What each loader ends with, from its documented semantics.

    read_files reads what its glob matches when it runs. COPY INTO and Auto Loader skip
    a path they have loaded before, even if the file changed; with allowOverwrites,
    Auto Loader reads a changed file again, in full. MERGE inserts event ids it has not
    seen."""
    landing: dict[str, str] = {}  # file name -> version currently there
    read: dict[str, dict[str, str]] = {"copy": {}, "auto": {}, "auto_ow": {}}
    tables: dict[str, list[str]] = {name: [] for name in LOADERS}
    for day, files in schedule():
        for f in files:
            landing[f.name] = f.version
        for name, version in sorted(landing.items()):
            if name.startswith(f"events-{day}"):
                tables["insert_by_name"] += ids[version]
            for loader, seen in read.items():
                changed = loader == "auto_ow" and seen.get(name) not in (None, version)
                if name not in seen or changed:
                    seen[name] = version
                    target = {"copy": "copy_into", "auto": "auto_loader"}.get(
                        loader, "auto_loader_overwrites"
                    )
                    tables[target] += ids[version]
    tables["copy_into_merge"] = sorted(set(tables["copy_into"]))
    tables["auto_loader_merge"] = sorted(set(tables["auto_loader_overwrites"]))
    expected = set(ids["expected"])
    return {name: score(rows, expected) for name, rows in tables.items()}


def score(rows: list[str], expected: set[str]) -> dict:
    events = set(rows)
    return {
        "rows": len(rows),
        "events": len(events),
        "missing_events": len(expected - events),
        "duplicate_rows": len(rows) - len(events),
        "unexpected_events": len(events - expected),
    }


def plan() -> dict:
    ids = build_files()
    runs = schedule()
    return {
        "days": f"{DAYS[0]}..{DAYS[-1]}",
        "expected_events": len(ids["expected"]),
        "incidents": {
            "duplicate_file_rows": len(ids[f"{DUPLICATE_DAY}"]),
            "truncated_delivered": len(ids[f"{TRUNCATED_DAY}-first-40pct"]),
            "truncated_full": len(ids[f"{TRUNCATED_DAY}"]),
            "late_rows": len(ids[f"{LATE_DAY}-late"]),
            "rerun_day_rows": len(ids[f"{RERUN_DAY}"]),
        },
        "runs": [
            {"day": str(day), "delivered": [f"{f.name} <- {f.version}" for f in files]}
            for day, files in runs
        ],
        "predicted": predict(ids),
    }


class Workspace:
    """The SQL warehouse and the Files API, with Free Edition's limits handled."""

    def __init__(self) -> None:
        from databricks.sdk import WorkspaceClient

        from databricks import sql

        self.client = WorkspaceClient()
        self.files = self.client.files
        self.conn = sql.connect(
            server_hostname=os.environ["DATABRICKS_HOST"].removeprefix("https://"),
            http_path=os.environ["DATABRICKS_HTTP_PATH"],
            access_token=os.environ["DATABRICKS_TOKEN"],
        )

    def sql(self, statement: str) -> list[tuple]:
        with self.conn.cursor() as cur:
            cur.execute(statement)
            return [tuple(r) for r in cur.fetchall()] if cur.description else []

    def error(self, statement: str) -> str | None:
        """Run a statement expected to fail, and return its error class."""
        try:
            self.sql(statement)
        except Exception as e:
            found = re.search(r"\[([A-Z_.]+)\]", str(e))
            return found.group(1) if found else str(e).splitlines()[0]
        return None

    def put(self, local: Path, remote: str) -> None:
        self.files.upload(remote, io.BytesIO(local.read_bytes()), overwrite=True)

    def exists(self, remote: str, size: int) -> bool:
        try:
            return self.files.get_metadata(remote).content_length == size
        except Exception:
            return False

    def delete_pipeline(self) -> None:
        for p in self.client.pipelines.list_pipelines(filter=f"name LIKE '{PIPELINE}'"):
            self.client.pipelines.delete(p.pipeline_id)

    def create_pipeline(self) -> str:
        from databricks.sdk.service import pipelines, workspace

        self.client.workspace.mkdirs(os.path.dirname(PIPELINE_SOURCE))
        self.client.workspace.upload(
            PIPELINE_SOURCE,
            io.BytesIO(pipeline_sql().encode()),
            format=workspace.ImportFormat.RAW,
            overwrite=True,
        )
        return self.client.pipelines.create(
            name=PIPELINE,
            catalog=CATALOG,
            schema=SCHEMA_NAME,
            serverless=True,
            continuous=False,
            configuration={"pipelines.numUpdateRetryAttempts": "0"},
            libraries=[pipelines.PipelineLibrary(file=pipelines.FileLibrary(path=PIPELINE_SOURCE))],
        ).pipeline_id

    def update_pipeline(
        self, pipeline_id: str, attempts: int = 20, timeout_s: float = 1800
    ) -> None:
        """One triggered update: each streaming table reads the files it has not processed.
        Retried here, not by the pipeline, when Free Edition refuses compute. Each update is
        given up to `timeout_s` (default 30 minutes) to reach a terminal state; past that,
        this raises instead of polling forever."""
        from databricks.sdk.errors import ResourceConflict

        api = self.client.pipelines
        for _ in range(attempts):
            try:
                update_id = api.start_update(pipeline_id).update_id
            except ResourceConflict:  # an update is still running; let it finish
                time.sleep(30)
                continue
            deadline = time.monotonic() + timeout_s
            while (state := api.get_update(pipeline_id, update_id).update.state.value) not in (
                "COMPLETED",
                "FAILED",
                "CANCELED",
            ):
                if time.monotonic() > deadline:
                    raise RuntimeError(
                        f"pipeline update {update_id} did not reach a terminal state within "
                        f"{timeout_s / 60:.0f} minutes (last seen: {state!r})"
                    )
                time.sleep(10)
            if state == "COMPLETED":
                return
            time.sleep(60)
        raise RuntimeError(f"pipeline update failed {attempts} times")


def reset(ws: Workspace) -> None:
    ws.sql(f"create schema if not exists {SCHEMA}")
    ws.delete_pipeline()  # drops the streaming tables it owns
    for loader in LOADERS:
        ws.sql(f"drop table if exists {SCHEMA}.{loader}")
    ws.sql(f"drop table if exists {SCHEMA}.expected")
    ws.sql(f"drop volume if exists {SCHEMA}.landing")
    ws.sql(f"create volume {SCHEMA}.landing")
    ws.sql(f"create volume if not exists {SCHEMA}.source")


def pipeline_sql() -> str:
    """The pipeline's source: the two Auto Loader streaming tables."""
    tables = []
    for loader in STREAMS:
        overwrites = loader.endswith("overwrites")
        options = ", `cloudFiles.allowOverwrites` => 'true'" if overwrites else ""
        tables.append(
            f"create or refresh streaming table {loader} as\n"
            f"select {COLUMNS}, _metadata.file_name as file_name\n"
            f"from stream read_files('{LANDING}', format => 'parquet'{options});\n"
        )
    return "\n".join(tables)


def merge(ws: Workspace, target: str, source: str) -> None:
    ws.sql(
        f"""merge into {SCHEMA}.{target} t
        using (select {COLUMNS} from {SCHEMA}.{source}
               qualify row_number() over (partition by event_id order by received_at) = 1) s
        on t.event_id = s.event_id
        when not matched then insert *"""
    )


def run_loader(ws: Workspace, loader: str, day: date, pipeline_id: str) -> None:
    if loader == "insert_by_name":
        pattern = f"{LANDING}/events-{day}*.parquet"
        ws.sql(
            f"insert into {SCHEMA}.{loader} select {COLUMNS} "
            f"from read_files('{pattern}', format => 'parquet')"
        )
    elif loader == "copy_into":
        ws.sql(f"copy into {SCHEMA}.{loader} from '{LANDING}' fileformat = parquet")
    elif loader == "copy_into_merge":
        merge(ws, loader, "copy_into")
    elif loader == "auto_loader_merge":
        merge(ws, loader, "auto_loader_overwrites")
    elif loader == "auto_loader":  # one update refreshes both streaming tables
        ws.update_pipeline(pipeline_id)


def load(ws: Workspace) -> tuple[dict, dict]:
    reset(ws)
    columns = (
        "event_id string, user_id bigint, account_id bigint, event_type string, "
        "platform string, event_at timestamp, received_at timestamp"
    )
    for loader in ("insert_by_name", "copy_into", "copy_into_merge", "auto_loader_merge"):
        ws.sql(f"create table {SCHEMA}.{loader} ({columns})")
    files = WORK / "files"
    ws.put(files / "expected.parquet", f"{SOURCE}/expected.parquet")
    ws.sql(
        f"create or replace table {SCHEMA}.expected as select event_id "
        f"from read_files('{SOURCE}/expected.parquet', format => 'parquet')"
    )
    pipeline_id = ws.create_pipeline()
    seconds = {loader: 0.0 for loader in LOADERS}
    for day, delivered in schedule():
        for f in delivered:
            ws.put(files / f"{f.version}.parquet", f"{LANDING}/{f.name}")
        for loader in LOADERS:
            started = time.perf_counter()
            run_loader(ws, loader, day, pipeline_id)
            seconds[loader] += time.perf_counter() - started
        print(f"  loaded {day} ({len(delivered)} file(s) delivered)", flush=True)
    observed = {}
    for loader in LOADERS:
        t = f"{SCHEMA}.{loader}"
        rows, events, missing, unexpected = ws.sql(
            f"""select (select count(*) from {t}) as rows_,
                (select count(distinct event_id) from {t}) as events,
                (select count(*) from {SCHEMA}.expected e
                 left anti join {t} x using (event_id)) as missing,
                (select count(distinct event_id) from {t} x
                 left anti join {SCHEMA}.expected e using (event_id)) as unexpected"""
        )[0]
        observed[loader] = {
            "rows": rows,
            "events": events,
            "missing_events": missing,
            "duplicate_rows": rows - events,
            "unexpected_events": unexpected,
        }
    seconds["auto_loader_pipeline"] = seconds.pop("auto_loader") + seconds.pop(
        "auto_loader_overwrites"
    )
    return observed, {f"load_{k}_s": round(v, 1) for k, v in seconds.items()}


def rerun(ws: Workspace) -> dict:
    """Recompute one day of a 365-day rollup partitioned by day and write it back."""
    source = DATA / "raw" / "events.parquet"
    remote = f"{SOURCE}/events.parquet"
    if not ws.exists(remote, source.stat().st_size):
        ws.put(source, remote)
    base, day = f"{SCHEMA}.rollup", f"{SCHEMA}.rollup_one_day"
    ws.sql(
        f"""create or replace table {base} partitioned by (activity_date) as
        with first_delivery as (
            select *, row_number() over (partition by event_id order by received_at) as delivery
            from read_files('{remote}', format => 'parquet'))
        select to_date(event_at) as activity_date, account_id, event_type,
               count(*) as events, count(distinct user_id) as active_users
        from first_delivery where delivery = 1 group by all"""
    )
    ws.sql(f"create or replace table {day} as select * from {base} "
           f"where activity_date = '{ROLLUP_DAY}'")  # fmt: skip

    def state(table: str) -> dict:
        days, rows = ws.sql(f"select count(distinct activity_date), count(*) from {table}")[0]
        return {"days": days, "rows": rows}

    def fresh(name: str) -> str:
        table = f"{SCHEMA}.rerun_{name}"
        ws.sql(f"create or replace table {table} deep clone {base}")
        return table

    out: dict = {"before": state(base), "rows_rewritten": state(day)["rows"]}
    out["set_partition_overwrite_mode"] = ws.error(
        "set spark.sql.sources.partitionOverwriteMode = dynamic"
    )
    t = fresh("insert_overwrite")
    ws.sql(f"insert overwrite {t} select * from {day}")
    out["insert_overwrite"] = state(t)
    ws.sql(f"restore table {t} to version as of 0")
    out["insert_overwrite_then_restore"] = state(t)
    t = fresh("insert_overwrite_partition")
    ws.sql(f"insert overwrite {t} partition (activity_date) select * from {day}")
    out["insert_overwrite_partition"] = state(t)
    next_day = date.fromisoformat(ROLLUP_DAY) + timedelta(1)
    t = fresh("replace_where")
    ws.sql(f"insert into {t} replace where activity_date = '{ROLLUP_DAY}' select * from {day}")
    out["replace_where"] = state(t)
    out["replace_where_wrong_day"] = {
        "error": ws.error(
            f"insert into {t} replace where activity_date = '{next_day}' select * from {day}"
        ),
        **state(t),
    }
    t = fresh("replace_using")
    ws.sql(f"insert into {t} replace using (activity_date) select * from {day}")
    out["replace_using"] = state(t)
    return out


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--plan", action="store_true", help="only the local plan and prediction")
    ap.add_argument("--check", action="store_true", help="compare with the published files")
    args = ap.parse_args()

    planned = plan()
    text = json.dumps(planned, indent=2) + "\n"
    for name, p in planned["predicted"].items():
        print(f"predicted {name:<24} {p}")
    if args.check:
        if not PLAN.exists() or PLAN.read_text() != text:
            sys.exit(f"{PLAN.relative_to(ROOT)} does not match this run")
        print(f"matches {PLAN.relative_to(ROOT)}")
    else:
        PLAN.write_text(text)
    if args.plan:
        return

    ws = Workspace()
    version = ws.sql("select current_version().dbsql_version")[0][0]
    print(f"workspace: Databricks SQL {version}")
    loaded, load_times = load(ws)
    rerun_result = rerun(ws)
    result = {
        "databricks_sql": version,
        "loading": loaded,
        "matches_prediction": loaded == planned["predicted"],
        "rerun_day": ROLLUP_DAY,
        "rerun": rerun_result,
    }
    for name, o in loaded.items():
        print(f"observed  {name:<24} {o}")
    print(json.dumps(rerun_result, indent=2))
    RESULT.write_text(json.dumps(result, indent=2) + "\n")
    OBSERVED.write_text(
        json.dumps({"note": "one run, Free Edition; not compared", **load_times}, indent=2) + "\n"
    )
    if not result["matches_prediction"]:
        sys.exit("the workspace did not do what the documented semantics predict")


if __name__ == "__main__":
    main()
