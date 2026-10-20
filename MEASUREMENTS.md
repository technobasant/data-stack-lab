# Measurements

Every figure in the README comes from this file. The figures here fall into three
tiers:

1. **Regenerated and checked in CI**, from a fresh dataset, on every push
   ([`.github/workflows/ci.yml`](.github/workflows/ci.yml)):
   [`results/dbt-scd2.json`](results/dbt-scd2.json) (`scripts/measure.py --check`),
   [`results/mutants-core.json`](results/mutants-core.json) and
   [`results/mutants-v2.json`](results/mutants-v2.json) (`scripts/mutants.py --check`,
   both dbt engines), [`results/airflow-backfill.json`](results/airflow-backfill.json)
   (`airflow/backfill.py --check`), [`results/quality.json`](results/quality.json)
   (`quality/run.py --check`), [`results/spark.json`](results/spark.json)
   (`spark/run.py --check`), and
   [`results/databricks-plan.json`](results/databricks-plan.json)
   (`databricks/run.py --plan --check`, the locally-predicted part only). If any of
   these figures differed from a fresh run, CI would fail.
2. **Recorded once, from a run that needs a service or account CI does not have**, and
   not regenerated since: [`results/databricks.json`](results/databricks.json) and
   [`results/dbt-databricks.json`](results/dbt-databricks.json) (both need a
   Databricks workspace), and
   [`results/airflow-scheduler-run.json`](results/airflow-scheduler-run.json) (needs a
   real `airflow standalone` scheduler, not `dag.test()`; written by
   [`airflow/scheduler_run.py`](airflow/scheduler_run.py)). `tests/test_databricks.py`
   (part of `pytest -q`, which CI runs on every push) checks the first of these
   against `results/databricks-plan.json`'s prediction, and the second against
   `results/dbt-scd2.json` (itself regenerated and checked, separately, by the dbt
   job), so neither can silently drift from tier 1 even though neither is
   regenerated itself. `results/airflow-scheduler-run.json` records its own
   comparison with the harness (`identical_to_in_order_dag_test`), but CI does not run it.
3. **Timings, observed on one run, never compared**: the dbt Core/v2 end-to-end
   seconds in [Timing](#6-timing) below, the Spark write times in
   [`results/spark-observed.json`](results/spark-observed.json), and the Databricks
   loader and dbt-build seconds in
   [`results/databricks-observed.json`](results/databricks-observed.json) and the dbt
   section below. These depend on the host and are not claimed to be reproducible.

Runs were made on 2026-10-18 unless stated otherwise.

## Environment

| | |
|---|---|
| Host | Apple M5 (4 performance + 6 efficiency cores), 24 GiB, macOS 27.0.1 |
| Python | 3.13.15, managed by uv 0.12.23 from `uv.lock` |
| dbt | dbt Core 1.12.6 with dbt-duckdb 1.11.0; dbt v2 as `dbt-oss` 2.0.8 |
| Warehouse | DuckDB 1.5.6, one file (`data/warehouse.duckdb`) |
| Generator | NumPy 2.5.4, PyArrow 26.0.0 |
| Airflow | Apache Airflow 3.3.2 (the `airflow` dependency group), SQLite metadata database |
| Databricks | Free Edition: Databricks SQL 2026.39 on the serverless Starter Warehouse (2X-Small), serverless Lakeflow pipelines (run on 2026-10-19) |

Only the timings (tier 3) depend on the host. Every tier-1 figure is a count over
generated data (fixed seed) and is identical on any machine; CI (Ubuntu) reproduces those files byte
for byte. The tier-2 files were recorded on this host and are not reproduced anywhere.

## Dataset

[`datagen/saas.py`](datagen/saas.py), scale `lab`, seed 7. Billing period 1 October
2025 to 30 September 2026; the oldest accounts signed up in January 2023.

| Parameter | Value |
|---|---|
| Accounts | 1,000; 70% signed up before the period, 30% during it |
| Plan at signup | free 45%, team 35%, business 15%, enterprise 5% |
| Plan events | 0.8 a year per account (Poisson). From free: upgrade 75%, churn 25%. Team: up 50%, down 25%, churn 25%. Business: up 40%, down 35%, churn 25%. Enterprise: down 60%, churn 40%. One tier at a time. |
| Trials | 20% of upgrades revert 3–25 days later if nothing else changed first |
| Country moves | 0.05 a year per account |
| Users | by signup plan: free 1–3, team 2–12, business 8–40, enterprise 25–150 |
| Activity | a user is active on a day with probability 0.10 × plan factor (free 0.6, team 1.0, business 1.2, enterprise 1.4), only on days the account was active from start to end and the user already existed; 1 + Poisson(2.5) events per active day |
| Delivery | delay 1–119 s, except 2.2% that arrive 1–72 h late and 0.3% that arrive 3–10 days late; 0.8% delivered twice, the copy 1 s–2 h later |

Rows: 13 account extracts (703 accounts on 1 Oct 2025, growing to 1,000), 4 plans,
11,963 users, 5,048 invoices, 913,388 delivered events (906,015 distinct), 2,291
account changes in the truth file (including 1,000 signups).

## How the warehouse is built

[`scripts/warehouse.py`](scripts/warehouse.py) replaces `raw.accounts` with each
month's extract in turn and runs `dbt snapshot` after each, 13 times, so the
snapshot holds only what the extracts showed. It then loads the other sources and
runs `dbt build --exclude resource_type:snapshot`: 11 models, 46 data tests, 3 unit
tests. Invoices are billed at 00:00 UTC on the first of the month, the same instant
the extract is taken.

## 1. Revenue by plan

Source: [`results/dbt-scd2.json`](results/dbt-scd2.json), `revenue`.

- **Truth**: the plan the generator billed, from `truth/invoice_plans.parquet`. The
  warehouse never sees it.
- **Type 2**: the plan on the `dim_account` version that `fct_invoices` attached to
  the invoice (as-of join on `invoiced_at`).
- **Type 1**: the plan on the account's current version, which is what a dimension
  overwritten in place would report.

| Plan | Truth | Type 2 | Type 1 | Type 1 vs truth |
|---|---:|---:|---:|---:|
| free | $0 | $0 | $10,700 | n/a |
| team | $140,581 | $140,581 | $157,008 | +11.7% |
| business | $325,166 | $325,166 | $340,030 | +4.6% |
| enterprise | $544,455 | $544,455 | $502,464 | −7.7% |
| **total** | $1,010,202 | $1,010,202 | $1,010,202 | |

Type 2 matched the truth on 5,048 of 5,048 invoices. Type 1 matched on 4,175, and
put $152,227 (15.07%) of revenue on another plan. Revenue billed on one plan and
reported on another under Type 1, largest flows first (`revenue.wrong_plan_flows` in
[`results/dbt-scd2.json`](results/dbt-scd2.json)):

| Billed on | Type 1 reports under | Invoices | Revenue |
|---|---|---:|---:|
| enterprise | business | 61 | $60,939 |
| business | team | 172 | $34,228 |
| business | enterprise | 121 | $24,079 |
| team | business | 274 | $13,426 |
| team | free | 194 | $9,506 |
| enterprise | team | 7 | $6,993 |
| team | enterprise | 38 | $1,862 |
| business | free | 6 | $1,194 |

Of the $152,227, $11,366 (84 invoices, `revenue.churned_wrong_plan`) belongs to
accounts that have since churned. A churned account keeps its last plan, so churn
alone moves nothing; those accounts changed plan before they left.

## 2. What monthly snapshots saw

Source: `snapshot_coverage` (`model`, `by_change`, `missed_replaced_by`) in
[`results/dbt-scd2.json`](results/dbt-scd2.json).

A change is counted as seen if a snapshot version starts at its exact timestamp (the
timestamp strategy dates versions from `updated_at`). Signups are new accounts, not
changes, and are left out.

| Change | Made in the period | Never seen |
|---|---:|---:|
| upgrade | 245 | 30 |
| downgrade | 108 | 6 |
| trial ended | 48 | 1 |
| churn | 142 | 0 |
| country move | 27 | 0 |
| **total** | **570** | **37** (6.5%) |

What replaced the missed changes before the next extract (`missed_replaced_by`): a
trial ending (26), an upgrade (6 after a downgrade, 2 after an upgrade), a downgrade
(2), churn (1).

`dim_account` holds 1,521 versions of 1,000 accounts. 33 versions have the same plan,
status and country as the version before: the account changed and changed back
between two extracts, which still moved `updated_at`.

## 3. Usage by plan

Source: `usage`.

Each of the 906,015 deduplicated events is attributed three ways: the `dim_account`
version in effect at `event_at` (Type 2, the same as-of predicate as
`fct_daily_usage`), the current version (Type 1), and the truth (DuckDB `ASOF JOIN`
on the generator's change history). Events on the wrong plan: Type 2, 734 (0.08%);
Type 1, 123,627 (13.65%). The Type 2 errors are events that happened inside a state
no extract saw.

## 4. Mutants

Sources: [`results/mutants-core.json`](results/mutants-core.json),
[`results/mutants-v2.json`](results/mutants-v2.json).

[`scripts/mutants.py`](scripts/mutants.py) applies each break to a copy of the
project and builds it against a copy of the warehouse twice: `dbt build` with
everything, and again with `--exclude resource_type:unit_test`. A failing unit test
skips its model, so the first run reports unit tests and the second reports what
the data tests alone would have caught.

| Break | Core 1.12 unit tests | v2 2.0.8 unit tests | Data tests |
|---|---|---|---|
| `fct_invoices` joins the current version | `an_invoice_takes_the_version_in_effect_when_billed` | same | none |
| first version dated from the snapshot | `first_version_reaches_back_to_signup` | none | `not_null_fct_daily_usage_account_key` |
| open version ends NULL | `first_version_reaches_back_to_signup` | none | `not_null_dim_account_valid_to` |
| no deduplication | `a_redelivered_event_counts_once` | same | `unique_stg_tracking__events_event_id` |

## 5. dbt v2 does not compare timestamps in unit tests

On dbt v2 (`dbt-oss` 2.0.8, DuckDB adapter, macOS arm64), a unit test passes when an
expected timestamp is wrong. Changing one expected `received_at` in
`a_redelivered_event_counts_once` from 2026-01-07 to 1999-01-07: dbt Core 1.12.6
fails the test; v2 passes it. On v2, a wrong expected string, integer or boolean (tried
in `first_version_reaches_back_to_signup`) does fail the test. The SQL v2 compiles for the test returns both rows,
2026-01-07 as actual and 1999-01-07 as expected, so the gap is in v2's comparison of
the fetched rows, not in the query.

This is [dbt-labs/dbt#15894](https://github.com/dbt-labs/dbt/issues/15894), open
since August 2026, where another user has reproduced it on DuckDB with 2.0.5 and
2.0.6. `mutants-v2.json` records the current behaviour; when a fix lands, CI's
`--check` fails on v2 and the record and this section get updated.

## 6. Timing

End to end on the host above (13 snapshot runs and one build, each a separate `dbt`
process): dbt Core 1.12.6, 83 s; dbt v2 2.0.8, 24 s. Each Core invocation starts a
Python interpreter and parses the project; v2 is a native binary. One run each: an
observation, not a benchmark.

## Limits

- The data is generated and the rates above are assumptions. The 15.1% is what this
  business's churn and plan changes produce, not a constant: more plan changes,
  more error.
- Invoices fall on the instant the extract is taken, which is why snapshot gaps cost
  invoices nothing here. A billing cycle that is not aligned with the extracts would
  inherit the same gaps that usage does.
- One price per plan, no seats, discounts, proration or refunds.
- The first-version stretch assumes an account looked, before its first extract, as
  it did at that extract. That is an assumption, labelled by `version_number = 1`.
- Not DuckDB only any more: this dbt project also builds on Databricks, after one
  change (contract types `varchar` → `string`); see
  [The dbt project on Databricks](#databricks-loading-files-that-change-re-running-a-day-on-delta)
  below. It is written to run on Snowflake and BigQuery too, through their adapters;
  those runs belong to those projects and are not claimed here.

## Airflow: incremental loads and backfills

Source: [`results/airflow-backfill.json`](results/airflow-backfill.json), written by
[`airflow/backfill.py`](airflow/backfill.py); CI re-runs it and fails on any difference.
Runs were made on 2026-10-18.

**Setup.** The delivered events are split into one landing partition per day of
arrival. Each scenario starts from its own DuckDB file holding everything that arrived
before 1 September, loaded in full into the three incremental models
([`dbt/models/airflow/`](dbt/models/airflow)). It then runs the DAG once per day of
September through `dag.test()`, which executes the real `land` and `transform` tasks
in-process; the run for a day is fired at the midnight after it, as the scheduler
fires it. All three models use `delete+insert` on `event_id` and keep the first
delivery within a batch, so the only difference between them is the filter that
chooses the batch.

**Scoring.** The truth is every distinct `event_id` whose first delivery arrived before
1 October: 89,508 of them arrived in September, 2,291 of those more than an hour after
they happened. A table's "lost" count is truth events absent from it. No table held a
duplicate in any scenario.

| Scenario | Runs | `events_by_event_time` | `events_by_arrival` | `events_by_interval` |
|---|---|---:|---:|---:|
| `in_order`: 1 → 30 Sep | 30 of 30 succeeded | 2,034 lost (2.27%), all over an hour late | 0 | 0 |
| `late_file`: the 10 Sep partition is withheld until after the 12th's run; the 10th is then re-run | 30 of 31; the first run for the 10th failed at `land` | 4,841 (5.41%) | 2,889 (3.23%) | 0 |
| `backwards`: 30 → 1 Sep | 30 of 30 | 86,432 (96.56%) | 86,432 (96.56%) | 0 |
| `default_cron`: `saas_daily_default_cron`, 1 → 30 Sep | 30 of 30 | 4,919 (5.50%) | 2,996 (3.35%), and 72 rows from 1 Oct | 89,508 (100%) |

What the counts are made of:

- `in_order`, event time: 2,034 of the 2,291 late arrivals. An event is dropped when an
  event that happened later was loaded first.
- `late_file`, arrival time: 2,889 of the 2,890 events whose first delivery arrived on
  10 September. The re-run found its high-water mark already at the 12th. The one it
  kept was redelivered on the 11th and loaded then.
- `backwards`: both watermarks kept only 30 September's 3,076 arrivals, the first day
  processed; every earlier day was below the mark.
- `default_cron`: `"@daily"` is a `CronTriggerTimetable` in Airflow 3
  (`[scheduler] create_cron_data_intervals = False`), so each run's data interval
  starts and ends at the instant it fires. `events_by_interval` loaded nothing in 30
  successful runs. `land` takes its day from `data_interval_start`, which on this
  schedule is the day that has just begun, so the run for 1 September loaded 2
  September's file: the two watermark models never received 1 September's 2,996
  arrivals, and the last run loaded 72 events that arrived on 1 October.

`tests/test_dags.py` pins both timetables: one-day windows for `saas_daily`, zero-length
for the default, so a change in Airflow's default fails CI before it changes a result.

**On a real scheduler.** The `in_order` scenario also runs on `airflow standalone`
(Airflow 3.3.2, SQLite, LocalExecutor) with `airflow backfill create --dag-id saas_daily
--from-date 2026-09-01 --to-date 2026-09-30 --max-active-runs 1`, by
[`airflow/scheduler_run.py`](airflow/scheduler_run.py) (`make airflow-scheduler`). All 30
backfill runs succeeded, and all three tables matched the harness exactly, on the first
run on 18 October and again on 19 October (3 min 39 s and 12 min 44 s; timings depend on
the host). Unpausing the DAG also makes the scheduler create a run for the latest interval,
which fails at `land` because no file exists for that day. The script queues the backfill
before it starts the scheduler: SQLite takes one writer, and a backfill created while the
scheduler was writing failed with `database is locked`, after which the scheduler's loop
crashed on a run that had been rolled back. Record:
[`results/airflow-scheduler-run.json`](results/airflow-scheduler-run.json). CI does not run
it.

**Limits.** One month, one file per day, no partial files: a file is either there or not.
The late file is withheld whole and delivered whole. The backwards order is the
`--run-backwards` order applied to `dag.test()`; on the real scheduler only the
in-order backfill was run. Event lateness comes from the generator's rates (2.2% late
by 1–72 hours, 0.3% by 3–10 days), so the event-time losses scale with them.


## Data quality: nine defects, two policies

Source: [`results/quality.json`](results/quality.json), written by
[`quality/run.py`](quality/run.py); CI re-runs it from a fresh dataset and fails on any
difference. Runs were made on 2026-10-18.

**Deliveries.** Each day's events are the landing partition for that day of arrival (the
same split as the Airflow project). August is delivered clean and accepted, to give the
batch checks seven days of history. Each day of September is then delivered with its
defect applied: rows chosen for a defect are every 20th (nulls), 33rd (new event type)
or 50th (orphan accounts) by event id, or every third for the late third. Every delivered
row carries a label: bad if the defect made it wrong (a null key, a shifted timestamp, an
unknown account), good otherwise. Rows a defect removed (the truncated upload, the
silent platform, the late third) are counted separately as missing from the day's
delivery; the late third's rows are not lost, only late (delivered two days on, with
29 September's own events).

**Checks** ([`dbt/models/quality/`](dbt/models/quality),
[`dbt/tests/generic/quality_checks.sql`](dbt/tests/generic/quality_checks.sql)). The
batch model selects named columns under an enforced contract and deduplicates (first
delivery wins; events accepted on earlier days are dropped). Row-level tests: not null on
`event_id`, `account_id` and `event_at`; unique `event_id`; `account_id` in the accounts
table; `event_type` and `platform` in their allowed sets; `event_at` no more than five
minutes after `received_at`. Batch-level tests: volume within 30% of the median of the
last seven accepted days; each platform's share within 0.08 of its share over those
days; rows per file equal to the producer's manifest, counted before deduplication. A
warning, not a failure, when redeliveries exceed 5% of the day.

**Policies.** Block holds the whole day on any failure. Quarantine sets aside the rows a
failed row-level test selects and promotes the rest; a contract or batch-level failure
still holds the day. A held day is not promoted; the harness does not model a person
fixing it.

| Day | Defect | Failed checks | Block | Quarantine |
|---|---|---|---|---|
| 3 Sep | file delivered twice (6,274 raw rows, 3,113 new events) | none; redelivery warning | promoted | promoted |
| 6 Sep | 147 of 2,963 events with no account | `not_null` (account_id) | held | 147 quarantined |
| 9 Sep | `event_type` renamed | the contract (model build failed) | held | held |
| 12 Sep | 1,709 rows cut from the upload | `matches_manifest`, `volume_near_baseline` | held | held |
| 15 Sep | all 2,992 event times shifted +5 h | `happened_before_arrival` | held | 2,905 quarantined, 87 promoted |
| 18 Sep | 92 events of a new, legitimate type | `accepted_values` (event_type) | held | 92 quarantined |
| 21 Sep | 613 iOS events never sent | `platform_share_near_baseline` | held | held |
| 24 Sep | 59 events with an unknown account | `relationships` | held | 59 quarantined |
| 27 Sep | 1,040 events withheld until the 29th | block: none; quarantine: `volume_near_baseline` | promoted | held |
| 29 Sep | the 1,040 arrive with the day's own events | `volume_near_baseline` | held | held |

| | Block | Quarantine |
|---|---:|---:|
| Bad rows delivered | 3,198 | 3,198 |
| Bad rows promoted | 0 | 87 |
| Good rows not promoted | 19,545 | 12,865 |
| Rows missing from the day's delivery (1,040 of them delivered two days late) | 3,362 | 3,362 |
| Days held | 8 | 5 |
| Clean days (20) with a failure or a warning | 0 | 0 |

Notes on the figures:

- The 87 bad rows promoted under quarantine are timestamp-shifted events that had
  already arrived more than five hours after they happened, so the shift did not put
  them after their arrival.
- On 27 September the delivered count was 2,092. Under block the median of the last
  seven accepted days was 2,972 (29.6% above it, inside the 30% band); under quarantine,
  which had accepted 18 and 24 September, it was 2,997 (30.2%, outside). The difference
  is which earlier days each policy had promoted. The medians themselves are not stored
  in `results/quality.json` (only `volume_near_baseline`'s pass or fail is, and CI checks
  that); verified here from the run's own `quality_state.accepted_daily` table.
- 29 September is counted as the late defect's consequence, not as a clean day.

**Limits.** Generated events with no weekly or seasonal pattern, so the volume and share
bands see an unusually flat baseline and the false-alarm count (0 of 20) is optimistic.
One defect per day, each applied whole. The harness does not model a person repairing
and re-releasing a held day.

## PySpark: re-running a day, small files, pruning

Source: [`results/spark.json`](results/spark.json), written by
[`spark/run.py`](spark/run.py); CI re-runs it and fails if a count differs. Times are in
[`results/spark-observed.json`](results/spark-observed.json): one run on the host above,
recorded and never compared. Runs were made on 2026-10-18 with PySpark 4.2.0 on Zulu
OpenJDK 17.0.20, `local[4]`, `spark.sql.shuffle.partitions` = 200, session time zone UTC.

**The job.** Read `raw/events.parquet`, keep the first delivery of each `event_id`, and
count events and distinct users per `activity_date` (the event's UTC date), `account_id`
and `event_type`: 333,082 rows over 365 days.

**Re-running one day.** The year is written with `partitionBy("activity_date")`. Then
15 September (1,070 rows) is recomputed alone and written back with `mode("overwrite")`.

| `partitionOverwriteMode` | Partitions before → after | Rows before → after |
|---|---|---|
| `static` (default) | 365 → 1 | 333,082 → 1,070 |
| `dynamic` | 365 → 365 | 333,082 → 333,082 |

**Files written.** The same rollup, written with `partitionBy("activity_date")`:

| Variant | Days | Files | Max files in a day | Median file | Write (observed) |
|---|---:|---:|---:|---:|---:|
| AQE off | 30 (September) | 5,970 | 200 | 1.5 KB | 386 s |
| AQE on | 30 | 30 | 1 | 5.6 KB | 4.7 s |
| AQE on | 365 | 1,460 | 4 | 2.8 KB | 78 s |
| AQE on, `repartition("activity_date")` | 365 | 365 | 1 | 5.0 KB | 23 s |

The AQE-off variant runs on September only: on the year it writes about 73,000 files and
takes most of an hour. A full scan of the September AQE-off output took 11.8 s against
0.12 s for the AQE-on output (observed).

**Pruning.** Rows for 15 September in the year's output: partitioned by day (365 files),
1 file holds them; unpartitioned (written with AQE off, 200 files), 199 files hold them.
"Holding" is counted with `input_file_name()` over the filtered rows.

**Limits.** One machine, local mode: no network shuffle and no object store, where small
files cost far more (one request each). The rollup is small, so absolute times are
seconds; the counts are the result, and they follow from Spark's partitioning rules
rather than from this machine.

## Databricks: loading files that change, re-running a day on Delta

Source: [`results/databricks.json`](results/databricks.json), written by
[`databricks/run.py`](databricks/run.py) in one run on 2026-10-19 on Databricks Free
Edition: Databricks SQL 2026.39 on the serverless Starter Warehouse (2X-Small), Unity
Catalog, a serverless Lakeflow pipeline for Auto Loader, driven from the host above with
databricks-sql-connector 4.6.0 and databricks-sdk 0.153.0. The workspace run cannot be
repeated in CI. What CI does check: [`results/databricks-plan.json`](results/databricks-plan.json),
the deliveries and every loader's outcome predicted from the documented semantics
(`run.py --plan --check`, exact), and that the recorded run equals the prediction
(`tests/test_databricks.py`). Times are in
[`results/databricks-observed.json`](results/databricks-observed.json), never compared.

**Deliveries.** Events received 1–10 September, each event once (its first delivery), one
file a day, 29,095 events in all. Files are written with UTC timestamps
(`isAdjustedToUTC`), so they read as `TIMESTAMP`. Four incidents:

| Day | Incident | Size |
|---|---|---:|
| 2 Sep | the day's file sent again as `events-2026-09-02-resent.parquet` | 2,904 events |
| 4 Sep | `events-2026-09-04.parquet` holds the first 40% of the day | 1,091 of 2,729 |
| 5 Sep | the complete file re-sent under the same name, overwriting it | 2,729 |
| 6 Sep | every third event held back, delivered 8 Sep as `events-2026-09-06-late.parquet` | 987 |
| 9 Sep | the load runs twice; nothing new between the runs | 2,775 in the day |

**Loaders,** run in this order after each delivery:

- `insert_by_name`: `INSERT INTO … SELECT … FROM read_files('…/events-<run day>*.parquet')`.
- `copy_into`: `COPY INTO … FROM '<landing folder>' FILEFORMAT = PARQUET`.
- `copy_into_merge`: `MERGE` from `copy_into`, first delivery per `event_id`, insert when
  not matched.
- `auto_loader` and `auto_loader_overwrites`: two streaming tables over the landing folder
  (`read_files` in `STREAM`, i.e. Auto Loader), the second with
  `cloudFiles.allowOverwrites = true`, in one Lakeflow pipeline (triggered, serverless,
  automatic retries off) updated once per run.
- `auto_loader_merge`: the same `MERGE`, from `auto_loader_overwrites`.

**Scoring.** Each final table against the 29,095 expected `event_id`s: rows, distinct
events, expected events absent, and rows minus distinct events. No loader produced an
event that was not sent.

| Loader | Rows | Events | Missing | Duplicate rows |
|---|---:|---:|---:|---:|
| `insert_by_name` | 32,149 | 26,470 | 2,625 | 5,679 |
| `copy_into` | 30,361 | 27,457 | 1,638 | 2,904 |
| `copy_into_merge` | 27,457 | 27,457 | 1,638 | 0 |
| `auto_loader` | 30,361 | 27,457 | 1,638 | 2,904 |
| `auto_loader_overwrites` | 33,090 | 29,095 | 0 | 3,995 |
| `auto_loader_merge` | 29,095 | 29,095 | 0 | 0 |

Accounting: 1,638 = the 60% of 4 September the truncated upload lost (2,729 − 1,091);
2,625 = 1,638 + the late 987; 2,904 = the resent file; 5,679 = 2,904 + 9 September loaded
twice (2,775); 3,995 = 2,904 + the first 1,091 rows read again with the corrected file.

**The semantics relied on.** `COPY INTO`: "Files in the source location that have already
been loaded are skipped. This is true even if the files have been modified since they
were loaded" ([docs](https://docs.databricks.com/aws/en/sql/language-manual/delta-copy-into)).
Auto Loader: "With the default setting (`cloudFiles.allowOverwrites` = `false`), files are
processed exactly once"; with it set to `true`, "Auto Loader is guaranteed to process the
latest version of the file"
([FAQ](https://docs.databricks.com/aws/en/ingestion/cloud-object-storage/auto-loader/faq)).

**Re-running one day on Delta.** `rollup`: the Spark project's rollup (first delivery per
event, then events and distinct users per UTC day, account and event type) as a Delta table
`PARTITIONED BY (activity_date)`: 365 days, 333,082 rows. `rollup_one_day`: 15 September
alone, 1,070 rows. Each write-back starts from a `DEEP CLONE` of `rollup`.

| Statement | Days after | Rows after |
|---|---:|---:|
| `SET spark.sql.sources.partitionOverwriteMode = dynamic` | `CONFIG_NOT_AVAILABLE` | |
| `INSERT OVERWRITE t SELECT * FROM rollup_one_day` | 1 | 1,070 |
| then `RESTORE TABLE t TO VERSION AS OF 0` | 365 | 333,082 |
| `INSERT OVERWRITE t PARTITION (activity_date) SELECT * FROM rollup_one_day` | 1 | 1,070 |
| `INSERT INTO t REPLACE WHERE activity_date = '2026-09-15' SELECT …` | 365 | 333,082 |
| `REPLACE WHERE activity_date = '2026-09-16'` with 15 September's rows | refused: `DELTA_REPLACE_WHERE_MISMATCH.INVARIANT_VIOLATION` | 333,082 |
| `INSERT INTO t REPLACE USING (activity_date) SELECT …` | 365 | 333,082 |

**Observed.** Over the 11 runs the Lakeflow pipeline's updates took 891 s in total, the
`INSERT`, `COPY INTO` and each `MERGE` 23–26 s each in total. Free Edition allows one
active pipeline per type, and a streaming table created with `CREATE STREAMING TABLE`
is a pipeline of its own; with two such tables, refreshes were refused for minutes at a
time ("You've hit the limit for serverless compute for free usage"), which is why both
streaming tables share one pipeline.

**Limits.** Small files (about 3,000 events a day) and one workspace on Free Edition. The
counts follow from the loaders' documented semantics and match a prediction made without
the workspace; they do not depend on scale. Timings do, and are not compared. File
notification mode, `cloudFiles.backfillInterval` and Auto Loader's handling of files
changed mid-write were not tested.

**The dbt project on Databricks.** Source:
[`results/dbt-databricks.json`](results/dbt-databricks.json), written by
[`databricks/dbt_build.py`](databricks/dbt_build.py) on 2026-10-19 with dbt-databricks 1.12.7
(dbt Core 1.12.5, run through `uvx` because the adapter pins databricks-sdk below 0.118) on the
same serverless SQL warehouse, catalog `dsl`. The raw parquet files were read with
`read_files` as written (timestamps arrive as `TIMESTAMP_NTZ`); each of the 13 extracts
replaced `dsl.raw.accounts` before its `dbt snapshot`; then `dbt build --exclude
resource_type:snapshot`. The revenue and model questions of `scripts/measure.py` were asked of
the Databricks marts with the same SQL.

| | DuckDB, published | Databricks |
|---|---:|---:|
| Models / data tests / unit tests passing | 11 / 46 / 3 | 11 / 46 / 3 |
| Accounts, account versions, versions identical to the previous one | 1,000, 1,521, 33 | 1,000, 1,521, 33 |
| Invoices, revenue | 5,048, $1,010,202 | 5,048, $1,010,202 |
| Invoices on the plan they billed: Type 2, Type 1 | 5,048, 4,175 | 5,048, 4,175 |
| Revenue on the wrong plan: Type 2, Type 1 | $0, $152,227 | $0, $152,227 |

The first run failed in `dim_account`'s contract check on `cast(null as varchar)`
(`DATATYPE_MISSING_SIZE`). The 13 contract columns declared `varchar` now declare `string`;
with that, every DuckDB and dbt v2 result and mutant record in this file is unchanged
(re-checked with `measure.py --check` and `mutants.py --check` on both engines). Observed: a
snapshot run took 22–31 s on the 2X-Small warehouse against about 2.5 s on DuckDB locally, and
the build 97 s against 3.6 s; the warehouse runs each statement remotely, so most of that is
per-statement latency, not work.

