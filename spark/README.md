# PySpark: re-running a day, small files, and partition pruning

The Spark project of [data-stack-lab](..). One batch job, a daily usage rollup
(deduplicate the events, then count per day, account and event type, written as Parquet
partitioned by day), and three things it gets wrong with Spark's defaults. Method and
figures: [MEASUREMENTS](../MEASUREMENTS.md#pyspark-re-running-a-day-small-files-pruning).

PySpark 4.2.0 on Java 17, `local[4]`, `spark.sql.shuffle.partitions` at its default of 200.

## 1. Re-running one day deleted the other 364

The rollup is written for the whole year, 365 daily partitions. Then one day,
15 September, is recomputed and written back with `mode("overwrite")` and
`partitionBy("activity_date")`, the way a re-run or a fix usually is.

| `spark.sql.sources.partitionOverwriteMode` | Partitions after | Rows after |
|---|---:|---:|
| `static` (Spark's default) | **1 of 365** | 1,070 of 333,082 |
| `dynamic` | 365 of 365 | 333,082 |

With the default, an overwrite replaces the whole output path, so a job that wrote one
day removed every other day without an error. `dynamic` replaces only the partitions the
write contains. Set it in the job's configuration, not in someone's notebook; table
formats (Delta's `replaceWhere`, Iceberg's dynamic overwrite) make the same choice
explicit.

## 2. One file per day is not the default

| Write | Data | Files | Most files in one day | Median file |
|---|---|---:|---:|---:|
| AQE off | September | **5,970** | 200 | 1.5 KB |
| AQE on | September | 30 | 1 | 5.6 KB |
| AQE on | the year | **1,460** | 4 | 2.8 KB |
| AQE on, `repartition("activity_date")` first | the year | 365 | 1 | 5.0 KB |

Each task of the last shuffle writes one file into every day it holds rows for. With
adaptive query execution off, that is Spark's 200 shuffle partitions times the days.
AQE coalesces small shuffles: September's fit in one partition, so one file a day. The
year's did not, and AQE settled on four partitions (the four cores of `local[4]`), so
four files a day. Repartitioning by the partition column puts each day in one task
whatever the data size.

In this run the AQE-off write of September took 386 s against 5 s with AQE on, and the
year wrote in 23 s repartitioned against 78 s without (one run on one machine; recorded in
`results/spark-observed.json`, not compared).

## 3. Partitioning decides what a one-day query reads

| Layout of the year | Files | Files holding 15 September |
|---|---:|---:|
| partitioned by day | 365 | **1** |
| unpartitioned (200 files from the shuffle) | 200 | 199 |

Filtering on the partition column lets Spark skip every other directory. Without it, the
day's rows are spread across almost every file, and the query has to open them.

## Running it

```bash
make spark                                       # uv sync --group spark, then the three experiments
uv run --group spark python spark/run.py --check # what CI runs: the published counts, exactly
```

Needs Java 17 or later. The run takes about ten minutes, most of it the AQE-off write.
