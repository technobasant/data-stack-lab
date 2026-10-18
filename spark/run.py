"""Three things a PySpark batch job gets wrong by default, on the same event data.

The job: deduplicate the delivered events and roll them up per day, account and event
type, written as Parquet partitioned by day. Then:

  rerun     re-run one day with mode("overwrite") under each partitionOverwriteMode, and
            count what is left of the other days
  files     write the rollup four ways and count the files: September with adaptive query
            execution (AQE) off and on, then the whole year with AQE on, without and with
            repartitioning by the partition column
  pruning   for one day's query on a year, count the files that hold its rows,
            partitioned by day against unpartitioned

The AQE-off write uses September only: for a whole year it writes about 73,000 files,
which makes the same point and takes most of an hour.

Counts are deterministic and go in results/spark.json; CI checks them. Times are printed
and recorded as observations from one run on one machine, never compared.

    python spark/run.py            # write results/spark.json
    python spark/run.py --check    # fail if a count differs from the file
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import sys
import time
from collections import Counter
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("DATA_DIR", ROOT / "data"))
OUT = DATA / "spark"
RESULT = ROOT / "results" / "spark.json"
RERUN_DAY = "2026-09-15"
MONTH = ("2026-09-01", "2026-10-01")  # the files and pruning experiments


def session() -> SparkSession:
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("data-stack-lab")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "200")  # Spark's default, stated
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark


def rollup(spark: SparkSession, day: str | None = None, month: bool = False) -> DataFrame:
    """Daily usage per account and event type, from deduplicated events."""
    events = spark.read.parquet(str(DATA / "raw" / "events.parquet"))
    first = Window.partitionBy("event_id").orderBy("received_at")
    events = (
        events.withColumn("delivery", F.row_number().over(first))
        .where("delivery = 1")
        .withColumn("activity_date", F.to_date("event_at"))
    )
    if day:
        events = events.where(F.col("activity_date") == F.lit(day).cast("date"))
    if month:
        first_day = F.lit(MONTH[0]).cast("date")
        last_day = F.date_sub(F.lit(MONTH[1]).cast("date"), 1)
        events = events.where(F.col("activity_date").between(first_day, last_day))
    return events.groupBy("activity_date", "account_id", "event_type").agg(
        F.count("*").alias("events"), F.countDistinct("user_id").alias("active_users")
    )


def files_in(path: Path) -> list[Path]:
    return sorted(p for p in path.rglob("*.parquet"))


def partitions_in(path: Path) -> int:
    return sum(1 for p in path.glob("activity_date=*") if p.is_dir())


def timed(fn) -> float:
    started = time.perf_counter()
    fn()
    return round(time.perf_counter() - started, 2)


def experiment_rerun(spark: SparkSession) -> dict:
    """Write every day, then re-run one day with mode("overwrite")."""
    out = {}
    for mode in ("static", "dynamic"):
        path = OUT / f"rerun-{mode}"
        shutil.rmtree(path, ignore_errors=True)
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
        rollup(spark).repartition("activity_date").write.partitionBy("activity_date").parquet(
            str(path)
        )
        before_partitions = partitions_in(path)
        before_rows = spark.read.parquet(str(path)).count()
        spark.conf.set("spark.sql.sources.partitionOverwriteMode", mode)
        one_day = rollup(spark, RERUN_DAY)
        rerun_rows = one_day.count()
        one_day.repartition("activity_date").write.mode("overwrite").partitionBy(
            "activity_date"
        ).parquet(str(path))
        out[mode] = {
            "partitions_before": before_partitions,
            "rows_before": before_rows,
            "rows_rewritten": rerun_rows,
            "partitions_after": partitions_in(path),
            "rows_after": spark.read.parquet(str(path)).count(),
        }
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "static")
    return out


def experiment_files(spark: SparkSession) -> tuple[dict, dict]:
    """The rollup written four ways; files on disk and files per day."""
    variants = {  # name: (AQE enabled, whole year rather than September, repartition by day)
        "aqe_off_september": (False, False, False),
        "aqe_on_september": (True, False, False),
        "aqe_on_year": (True, True, False),
        "repartitioned_year": (True, True, True),
    }
    counts, times = {}, {}
    for name, (aqe, year, repartition) in variants.items():
        spark.conf.set("spark.sql.adaptive.enabled", str(aqe).lower())
        path = OUT / f"files-{name}"
        shutil.rmtree(path, ignore_errors=True)
        df = rollup(spark, month=not year)
        if repartition:
            df = df.repartition("activity_date")
        writer = df.write.partitionBy("activity_date")
        times[f"write_{name}_s"] = timed(lambda w=writer, p=path: w.parquet(str(p)))
        files = files_in(path)
        per_day = Counter(f.parent.name for f in files)
        sizes = sorted(f.stat().st_size for f in files)
        counts[name] = {
            "files": len(files),
            "days": len(per_day),
            "max_files_per_day": max(per_day.values()),
            "rows": spark.read.parquet(str(path)).count(),
            "median_file_kb": round(sizes[len(sizes) // 2] / 1024, 1),
        }
        times[f"scan_all_{name}_s"] = timed(
            lambda p=path: spark.read.parquet(str(p)).agg(F.sum("events")).collect()
        )
    spark.conf.unset("spark.sql.adaptive.enabled")
    return counts, times


def experiment_pruning(spark: SparkSession) -> tuple[dict, dict]:
    """Files holding one day's rows in a year: partitioned by day against unpartitioned."""
    spark.conf.set("spark.sql.adaptive.enabled", "false")
    flat = OUT / "pruning-unpartitioned"
    shutil.rmtree(flat, ignore_errors=True)
    rollup(spark).write.parquet(str(flat))
    spark.conf.unset("spark.sql.adaptive.enabled")
    layouts = {"partitioned_by_day": OUT / "files-repartitioned_year", "unpartitioned": flat}
    counts, times = {}, {}
    for name, path in layouts.items():
        day = spark.read.parquet(str(path)).where(
            F.col("activity_date") == F.lit(RERUN_DAY).cast("date")
        )
        counts[name] = {
            "files_in_layout": len(files_in(path)),
            "files_holding_the_day": day.select(F.input_file_name()).distinct().count(),
            "rows_for_the_day": day.count(),
        }
        times[f"one_day_query_{name}_s"] = timed(lambda d=day: d.agg(F.sum("events")).collect())
    return counts, times


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument(
        "--check", action="store_true", help="compare the counts with the published file"
    )
    args = ap.parse_args()
    OUT.mkdir(parents=True, exist_ok=True)
    spark = session()
    rerun = experiment_rerun(spark)
    files, file_times = experiment_files(spark)
    pruning, pruning_times = experiment_pruning(spark)
    result = {
        "spark": spark.version,
        "rerun_day": RERUN_DAY,
        "rerun": rerun,
        "files": files,
        "pruning": pruning,
    }
    observations = {"note": "one run, one machine; not compared", **file_times, **pruning_times}
    spark.stop()

    for mode, r in rerun.items():
        print(f"rerun {mode:<8} partitions {r['partitions_before']} -> {r['partitions_after']}, "
              f"rows {r['rows_before']:,} -> {r['rows_after']:,}")  # fmt: skip
    for name, c in files.items():
        print(f"files {name:<20} {c['files']:>6,} files over {c['days']} days, "
              f"up to {c['max_files_per_day']} a day, median {c['median_file_kb']} KB")  # fmt: skip
    for name, c in pruning.items():
        held = f"{c['files_holding_the_day']} of {c['files_in_layout']}"
        print(f"pruning {name:<20} {held} files hold the day")
    print("observed:", json.dumps(observations))

    text = json.dumps(result, indent=2) + "\n"
    if args.check:
        if not RESULT.exists() or RESULT.read_text() != text:
            sys.exit(f"{RESULT.relative_to(ROOT)} does not match this run")
        print(f"matches {RESULT.relative_to(ROOT)}")
    else:
        RESULT.write_text(text)
        (ROOT / "results" / "spark-observed.json").write_text(
            json.dumps(observations, indent=2) + "\n"
        )


if __name__ == "__main__":
    main()
