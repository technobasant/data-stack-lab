# data-stack-lab

Hands-on data engineering projects, one per tool: dbt, Airflow, data quality, Spark and Databricks so far; Snowflake, Kafka, AWS and BigQuery next.

[![ci](https://github.com/technobasant/data-stack-lab/actions/workflows/ci.yml/badge.svg)](https://github.com/technobasant/data-stack-lab/actions/workflows/ci.yml)

Every project uses the same synthetic SaaS data, runs with one command, and has its results checked in CI. The Databricks project needs a workspace, so CI checks its prediction and the recorded run instead of re-running it.

## Projects

| Project | What it builds | Result |
|---|---|---|
| [`dbt/`](dbt) | A Kimball star schema: a Type 2 account dimension from dbt snapshots, invoice and daily-usage facts, enforced contracts, 46 data tests and 3 unit tests. Runs unchanged on dbt Core 1.12 and dbt v2. | Attributing revenue by each account's *current* plan (Type 1) put **15.1%** of a year's revenue on the wrong plan, including $10,700 on the free plan. The Type 2 dimension put 5,048 of 5,048 invoices on the plan they billed. |
| [`airflow/`](airflow) | A daily Airflow 3 DAG that lands each day's event files and runs three dbt incremental models that differ only in how they pick what is new. September is backfilled four ways and every table checked. | Backfilled newest-first, both high-water-mark models lost **96.56%** of the month with every run green; the model that loads the run's data interval lost nothing in any order. On Airflow 3's default `@daily` that same model loaded **0 of 89,508** events, also with every run green. |
| [`quality/`](quality) | A month of daily deliveries with nine realistic defects (a duplicate file, nulls, a renamed column, a truncated upload, local time labelled UTC, a new event type, a silent platform, orphan keys, a late third), checked in dbt by contract, row tests, batch tests and control totals, under a block and a quarantine policy. | Blocking let **0 of 3,198** bad rows through and held 19,545 good ones; quarantine let 87 through and held 12,865. The three losses that left no bad rows (3,362 rows never delivered) were caught only by control totals, volume and per-platform share. |
| [`spark/`](spark) | A PySpark 4.2 batch job (a daily usage rollup written as Parquet partitioned by day) and three defaults it trips over: re-running one day, the files a partitioned write produces, and partition pruning. | Re-running one day with Spark's default `static` overwrite left **1 of 365** daily partitions; `dynamic` kept all 365. A partitioned write produced 5,970 files for a month with AQE off and 1,460 for the year with AQE on; repartitioning by the partition column gave one file a day. |
| [`databricks/`](databricks) | Ten days of event files delivered into a Unity Catalog volume with a resent duplicate, a truncated upload corrected under the same name, a late file and a repeated run, loaded six ways (`INSERT … read_files`, `COPY INTO`, Auto Loader in a Lakeflow pipeline, each with and without `MERGE`); then one day of the Spark rollup re-run on Delta five ways, and the dbt project built there. | `COPY INTO` and Auto Loader never read the corrected file: **1,638 events** stayed missing. Only Auto Loader with `allowOverwrites` followed by a `MERGE` on the event key ended with exactly the 29,095 events sent. On Delta, `INSERT OVERWRITE`, even with `PARTITION (activity_date)`, left 1 of 365 days, and `RESTORE` brought them back. The dbt project built on Databricks after one change (contract types `varchar` → `string`) and gave the published revenue figures exactly. |

## The data

1,000 accounts on four plans, a year of invoices and 900k product events, including late and duplicate events. Because it is generated, the correct answer is known, so each project can prove its result instead of just running.

[`datagen/saas.py`](datagen/saas.py), seed 7, October 2025 to September 2026: generated in about five seconds, identical on every run, and nothing in it is real.

| Source | Rows | As delivered |
|---|---:|---|
| `accounts` | 703 → 1,000 | a full extract of the application table on the first of each month, 13 extracts. Current state only: no history. |
| `plans` | 4 | free, team $49, business $199, enterprise $999 a month |
| `users` | 11,963 | names and e-mail addresses (all under `.example`); 3% typed in capitals with a trailing space |
| `invoices` | 5,048 | the billing export: account and amount, **not** the plan billed |
| `events` | 913,388 | 0.8% redelivered with the same `event_id`; 2.9% arrive over an hour late |

The generator also writes what a warehouse never gets: every account change with its timestamp (2,291) and the plan each invoice billed. Each project checks its results against those files.

## Quick start

Needs [uv](https://docs.astral.sh/uv/). No database server: dbt runs on DuckDB.

```bash
make setup     # uv venv + dependencies
make all       # generate the data, build the warehouse, check the published figures, run the mutants
make docs      # dbt docs with the lineage graph, under dbt/target

DBT="uvx --from dbt-oss==2.0.8 dbt" make all    # the same project on dbt v2
make airflow   # the Airflow project: September backfilled four ways through the DAGs
make quality   # the data-quality project: September delivered with nine defects, two policies
make spark     # the Spark project: re-running a day, small files, pruning (needs Java 17+)
make databricks  # the Databricks project (needs a workspace; see databricks/README.md)
```

`make all` is what CI runs for the dbt project on both dbt engines; `make airflow`, `make quality` and `make spark` are what it runs for the others. Each takes a few minutes; the Spark run about ten. For Databricks, CI runs `databricks/run.py --plan --check`, which needs no workspace.

## Results: dimensional modelling in dbt

All figures are from [`results/dbt-scd2.json`](results/dbt-scd2.json), written by [`scripts/measure.py`](scripts/measure.py). CI regenerates the data, rebuilds the warehouse and fails if a single number differs. Method and caveats are in [MEASUREMENTS.md](MEASUREMENTS.md).

### Type 1 rewrites history

The same 5,048 invoices, summed by the plan each one billed and by the plan its account is on today:

| Plan | Billed (truth) | Type 2 dimension | Type 1 (today's plan) | Type 1 error |
|---|---:|---:|---:|---:|
| free | $0 | $0 | $10,700 | n/a |
| team | $140,581 | $140,581 | $157,008 | +11.7% |
| business | $325,166 | $325,166 | $340,030 | +4.6% |
| enterprise | $544,455 | $544,455 | $502,464 | −7.7% |

Type 1 put 873 invoices and **$152,227 (15.1%)** of $1,010,202 on a plan they did not bill. Totals still match, so no check on totals sees the error. Every later plan change drags past revenue with it. The largest single flow is $60,939 billed on enterprise that Type 1 reports under business, because those accounts have since downgraded. Accounts now on the free plan take $10,700 of paid history with them.

### What monthly snapshots miss

A snapshot only sees what an extract shows. Of the 570 changes made during the year, 533 (93.5%) survived to a month-end extract. The other 37 were made and replaced between two extracts; 26 of them were trial upgrades that ended before the next extract. For invoices, billed on the same day the extract lands, that cost nothing (0 of 5,048 wrong). For usage it is small but real: 0.08% of 906,015 events are counted against the wrong plan, against 13.65% with Type 1. To see more, extract more often or read the change log (CDC). A dimension built from snapshots cannot recover what no extract showed.

The timestamp strategy also recorded 33 versions identical to the one before. A trial that started and ended between two extracts still moved `updated_at`.

### Tests that fail when they should

[`scripts/mutants.py`](scripts/mutants.py) breaks the models four ways and builds each break twice: once with the unit tests (hand-written rows) and once with the data tests alone (the real data). Results are recorded per engine in [`results/`](results), and CI fails if they change.

| Break | Unit tests, dbt Core 1.12 | Unit tests, dbt v2 2.0.8 | Data tests on the real data |
|---|---|---|---|
| Invoices joined to today's account version | caught | caught | **all pass**: keys valid, every invoice present, revenue reconciles to the cent |
| First version dated from the snapshot instead of signup | caught | **missed** | caught: `not_null` on `fct_daily_usage.account_key` |
| Current version left with a NULL end | caught | **missed** | caught: `not_null` on `dim_account.valid_to` |
| Redelivered events not deduplicated | caught | caught | caught: `unique` on `event_id` |

Two lessons. First, a wrong-but-plausible join produces valid keys and correct totals. Only a test that states which version an invoice should get, on rows written by hand, can see it.

Second, a passing unit test depends on the engine running it. dbt v2 never compares timestamp columns in a unit test's expected rows. The model can produce 2025-06-10 where the test expects 2025-03-01, and the test still passes, while strings, integers and booleans in the same row are compared. This is reported as [dbt-labs/dbt#15894](https://github.com/dbt-labs/dbt/issues/15894) (open; this repo reproduces it on 2.0.8 with DuckDB). The data tests still caught both breaks here. Without them, both would have shipped.

### Same project, two engines

dbt v2 (the Rust engine, `dbt-oss` 2.0.8, Apache-2.0) ran the project after one fix: `loaded_at_field` must sit under `config:`, which dbt Core 1.12 accepts either way. Both engines build the same 11 models, pass the same 46 data tests and 3 unit tests, and give byte-identical measurements. CI runs both. The difference is the timestamp gap in v2's unit tests, above.

## Results: incremental loads and backfills in Airflow

Figures from [`results/airflow-backfill.json`](results/airflow-backfill.json); the method is in [MEASUREMENTS.md](MEASUREMENTS.md#airflow-incremental-loads-and-backfills) and the project in [`airflow/`](airflow).

One DAG lands a day of event files and runs three incremental models: one takes events newer than the newest event time already loaded, one takes those that arrived after the newest arrival already loaded, and one takes exactly what arrived in the run's data interval. September, 89,508 events, was backfilled four ways:

| Scenario | 30 runs | Event-time mark | Arrival-time mark | Data interval |
|---|---|---:|---:|---:|
| In order | all green | lost 2,034 (2.27%) | 0 | 0 |
| The 10 Sep file lands two days late; the day is re-run | re-run green | lost 4,841 (5.41%) | lost 2,889 (3.23%) | 0 |
| Newest day first (`--run-backwards`) | all green | lost 86,432 (96.56%) | lost 86,432 (96.56%) | 0 |
| Airflow 3's default `@daily` | all green | lost 4,919 (5.50%) | lost 2,996 (3.35%) | **lost 89,508 (100%)** |

A high-water mark remembers how far it got, not what it has seen, so any day processed after a later one is skipped, and nothing fails. Loading by the data interval is right in every order, but in Airflow 3 `@daily` became a `CronTriggerTimetable` whose interval starts and ends at the same instant: an empty window, thirty times, with nothing marked failed. `saas_daily` asks for `CronDataIntervalTimetable` explicitly, and a test pins both schedules. The in-order backfill was also run on a real scheduler (`airflow standalone`, `airflow backfill create`), and its tables matched the harness exactly.

## Results: data quality

Figures from [`results/quality.json`](results/quality.json); the method is in [MEASUREMENTS.md](MEASUREMENTS.md#data-quality-nine-defects-two-policies) and the project in [`quality/`](quality).

| Defect | Caught by | Block | Quarantine |
|---|---|---|---|
| duplicate file | nothing failed; deduplication absorbed it, and a redelivery warning fired | promoted | promoted |
| 5% nulls in a key | `not_null` | held | 147 rows quarantined |
| renamed column | the enforced contract | held | held |
| upload stopped at 40% | manifest, volume | held | held |
| local time labelled UTC | event after arrival | held | quarantined; **87 bad rows promoted** |
| a new, legitimate event type | `accepted_values` | held | **92 good rows quarantined** |
| a platform goes silent | per-platform share (volume moved 19%, inside the band) | held | held |
| 2% orphan keys | `relationships` | held | 59 rows quarantined |
| a third arrives two days late | volume, in one run of two | **promoted** | held |

No check fired on any of the 20 clean days. Missing data and wrong data need different checks, a fixed volume band is a coin toss at its edge, and no rule can tell a defect from a product change.

## Results: Spark

Counts from [`results/spark.json`](results/spark.json); the method is in [MEASUREMENTS.md](MEASUREMENTS.md#pyspark-re-running-a-day-small-files-pruning) and the project in [`spark/`](spark).

| Experiment | Result |
|---|---|
| Re-run one day with `mode("overwrite")`, default `static` | **1 of 365** partitions left; 333,082 rows became 1,070 |
| The same, `dynamic` | 365 of 365 left |
| Write a month, AQE off | 5,970 files, up to 200 a day, median 1.5 KB |
| Write the year, AQE on | 1,460 files, up to 4 a day |
| Write the year, repartitioned by day | 365 files, one a day |
| Files holding one day's rows | 1 of 365 partitioned; 199 of 200 unpartitioned |

## Results: Databricks

From [`results/databricks.json`](results/databricks.json), one run on Databricks Free Edition (Databricks SQL 2026.39); the prediction it matched is [`results/databricks-plan.json`](results/databricks-plan.json). Method in [MEASUREMENTS.md](MEASUREMENTS.md#databricks-loading-files-that-change-re-running-a-day-on-delta), project in [`databricks/`](databricks).

| Loader, after ten days (29,095 events sent) | Rows | Events missing | Duplicate rows |
|---|---:|---:|---:|
| `INSERT … read_files` by file name | 32,149 | 2,625 | 5,679 |
| `COPY INTO` | 30,361 | **1,638** | 2,904 |
| `COPY INTO` + `MERGE` | 27,457 | **1,638** | 0 |
| Auto Loader | 30,361 | **1,638** | 2,904 |
| Auto Loader, `allowOverwrites` | 33,090 | 0 | 3,995 |
| Auto Loader, `allowOverwrites` + `MERGE` | 29,095 | 0 | 0 |

| Re-running one day of a 365-day Delta table | Days left |
|---|---:|
| `INSERT OVERWRITE` | 1 of 365; `RESTORE` brought back 365 |
| `INSERT OVERWRITE … PARTITION (activity_date)` | 1 of 365 |
| `REPLACE WHERE activity_date = …` | 365 |
| `REPLACE USING (activity_date)` | 365 |
| `SET spark.sql.sources.partitionOverwriteMode` | not available on a SQL warehouse |

| The dbt project on Databricks (dbt-databricks 1.12.7) | Result |
|---|---|
| Models, data tests, unit tests | 11, 46 and 3, all passing, after one change: contract types `varchar` → `string` |
| The published revenue figures | identical: 5,048 of 5,048 invoices on the plan they billed; $152,227 on the wrong plan with Type 1 |

## Decisions

- **Snapshot the source, not a staging model.** The snapshot is the only record of history, so it keeps exactly what arrived. Cleaning happens downstream, where it can be changed and rebuilt.
- **Timestamp strategy, dated by the application's `updated_at`.** Versions are dated by when the change happened, not by when the snapshot ran. The cost is the identical versions above. The check strategy compares column values instead and would skip them, but by default it dates each version to the snapshot run.
- **The first version reaches back to signup.** An account that changed before its first extract is first seen after signup, and activity in that gap would find no version. Its first known state is the best available answer, and `version_number = 1` marks it.
- **Open versions end at 9999-12-31, not NULL.** As-of joins become a plain range predicate on every warehouse.
- **Facts join with a LEFT JOIN plus a `not_null` test on the key.** An unmatched row fails the build instead of disappearing.
- **No dbt packages.** The three custom generic tests (validity windows that neither overlap nor leave gaps, and a composite grain) take under 50 lines. Without packages the project runs offline and on dbt v2 unchanged.
- **Portable SQL.** Cross-database macros (`dbt.date_spine`, `dbt.date_trunc`, `dbt.split_part`) and no `QUALIFY`, so the Snowflake, Databricks and BigQuery projects can run this dbt project, not a copy of it.
- **Personal data stops at staging.** Marts carry the e-mail domain, never the address or the name.

## Layout

```
datagen/            the seeded generator shared by every project
dbt/                the dbt project: staging, snapshots, marts, tests; models/airflow for the Airflow project
airflow/            the DAGs and the backfill harness
quality/            the data-quality harness; dbt/models/quality holds the checks
spark/              the PySpark job and its three experiments
databricks/         six loaders on a Unity Catalog volume, and a re-run on Delta
scripts/            warehouse.py (load + replay + build), measure.py, mutants.py
results/            published measurements; CI checks them
tests/              the generator's rules, the DAGs, the recorded Databricks run (pytest)
```

## Licence

Apache-2.0. See [LICENSE](LICENSE) and [NOTICE](NOTICE).
