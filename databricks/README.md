# Databricks: loading files that change, and re-running a day on Delta

The Databricks project of [data-stack-lab](..). Ten days of September's events are
delivered as daily files into a Unity Catalog volume and loaded six ways; then one day of
the yearly rollup from the [Spark project](../spark) is re-run on a Delta table five ways.
Method and figures: [MEASUREMENTS](../MEASUREMENTS.md#databricks-loading-files-that-change-re-running-a-day-on-delta).

Databricks Free Edition: one serverless SQL warehouse (2X-Small), Unity Catalog, a
serverless Lakeflow pipeline for Auto Loader. Everything is driven from
[`run.py`](run.py) through the SQL warehouse and the Databricks SDK.

## 1. The corrected file that was never read

The deliveries carry four things that happen to real ones:

| Day | What happened |
|---|---|
| 2 Sep | the day's file (2,904 events) is sent again under another name |
| 4 Sep | the upload stops at 40% (1,091 of 2,729 events); on 5 Sep the producer re-sends the complete file **under the same name** |
| 6 Sep | a third of the day's events (987) arrive two days later, in their own file |
| 9 Sep | the day's load is run twice |

Six loaders run after each day's delivery. After the tenth day, each table is compared
with the 29,095 events that were sent:

| Loader | Rows | Events missing | Duplicate rows |
|---|---:|---:|---:|
| `INSERT INTO … SELECT FROM read_files('events-<day>*')` | 32,149 | 2,625 | 5,679 |
| `COPY INTO` | 30,361 | **1,638** | 2,904 |
| `COPY INTO`, then `MERGE` on `event_id` | 27,457 | **1,638** | 0 |
| Auto Loader | 30,361 | **1,638** | 2,904 |
| Auto Loader, `cloudFiles.allowOverwrites` | 33,090 | 0 | 3,995 |
| Auto Loader, `allowOverwrites`, then `MERGE` | **29,095** | **0** | **0** |

`COPY INTO` and Auto Loader track files by path. The producer's fix arrived under the name
already loaded, so both skipped it, and the 1,638 events the truncated upload had lost
never reached the table. Databricks documents this: `COPY INTO` skips a loaded file "even
if the files have been modified since they were loaded", and Auto Loader processes a file
exactly once unless `cloudFiles.allowOverwrites` is set. With it set, Auto Loader read the
corrected file again, in full, so the 1,091 events of the first upload went in twice;
only a `MERGE` on the event key turned that into exactly the events sent.

Both file-tracking loaders handled the rest well: the late file was picked up whenever it
arrived and the second run on 9 Sep loaded nothing. The plain `INSERT` by file name missed
the late file and the correction, and loaded 9 Sep twice. No loader caught the resent
duplicate by itself; that took the `MERGE`.

The outcome of every loader is first predicted from the documented semantics alone
(`run.py --plan`, which CI re-checks exactly); the workspace run must reproduce the
prediction or it fails.

## 2. Re-running one day on Delta

The 365-day rollup (333,082 rows) as a Delta table partitioned by day; 15 September
(1,070 rows) recomputed and written back. Figures in
[`results/databricks.json`](../results/databricks.json), `rerun`:

| Write-back | Days left | Rows left |
|---|---:|---:|
| `INSERT OVERWRITE t SELECT …` | **1 of 365** | 1,070 |
| then `RESTORE TABLE t TO VERSION AS OF 0` | 365 | 333,082 |
| `INSERT OVERWRITE t PARTITION (activity_date) SELECT …` | **1 of 365** | 1,070 |
| `INSERT INTO t REPLACE WHERE activity_date = '2026-09-15' SELECT …` | 365 | 333,082 |
| the same rows, `REPLACE WHERE` naming 16 September | 365 (write refused) | 333,082 |
| `INSERT INTO t REPLACE USING (activity_date) SELECT …` | 365 | 333,082 |

A plain overwrite replaced the table with the one day, as `mode("overwrite")` did in Spark,
and naming the partition column in `PARTITION (activity_date)` did not change that: the
dynamic partition clause still replaced every partition. Unlike plain Parquet, the old
table was one `RESTORE` away, for as long as the files of the earlier version are kept.
`REPLACE WHERE` replaced exactly the day, and refused the write
(`DELTA_REPLACE_WHERE_MISMATCH`) when the rows did not match its predicate.

On a SQL warehouse `spark.sql.sources.partitionOverwriteMode` cannot be set at all, so the
fix from the Spark project is not available; Delta's own `REPLACE WHERE` and
`REPLACE USING` are.

## Running it

```bash
make databricks                                   # needs a workspace, about 30 minutes
uv run python databricks/run.py --plan --check    # what CI runs: deliveries and predictions
```

Needs `DATABRICKS_HOST`, `DATABRICKS_TOKEN` and `DATABRICKS_HTTP_PATH` (a SQL warehouse's
HTTP path) in the environment. The run creates the schema `workspace.data_stack_lab`
(override with `DATABRICKS_SCHEMA`), two volumes and one pipeline, and replaces them on
every run. On Free Edition a pipeline update can be refused while another serverless
resource winds down; the run waits and retries.
