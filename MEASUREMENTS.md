# Measurements

Every figure in the README comes from this file, and every figure here comes from a
file under [`results/`](results) that CI regenerates and compares. Runs were made on
2026-10-18.

## Environment

| | |
|---|---|
| Host | Apple M5 (4 performance + 6 efficiency cores), 24 GiB, macOS 27.0.1 |
| Python | 3.13.15, managed by uv 0.12.23 from `uv.lock` |
| dbt | dbt Core 1.12.6 with dbt-duckdb 1.11.0; dbt v2 as `dbt-oss` 2.0.8 |
| Warehouse | DuckDB 1.5.6, one file (`data/warehouse.duckdb`) |
| Generator | NumPy 2.5.4, PyArrow 26.0.0 |
| Airflow | Apache Airflow 3.3.2 (the `airflow` dependency group), SQLite metadata database |

Only the timings depend on the host. Every other figure is a count over seeded data
and is identical on any machine; CI (Ubuntu) reproduces the files byte for byte.

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
account changes in the truth file.

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
reported on another under Type 1, largest flows first (computed from
`marts.revenue_by_plan`):

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

Of the $152,227, $11,366 (84 invoices) belongs to accounts that have since churned.
A churned account keeps its last plan, so churn alone moves nothing; those accounts
changed plan before they left.

## 2. What monthly snapshots saw

Source: `snapshot_coverage` and `model`.

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

What replaced the missed changes before the next extract: a trial ending (26), an
upgrade (6 after a downgrade, 2 after an upgrade), a downgrade (2), churn (1).

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

- The data is synthetic and the rates above are assumptions. The 15.1% is what this
  business's churn and plan changes produce, not a constant: more plan changes,
  more error.
- Invoices fall on the instant the extract is taken, which is why snapshot gaps cost
  invoices nothing here. A billing cycle that is not aligned with the extracts would
  inherit the same gaps that usage does.
- One price per plan, no seats, discounts, proration or refunds.
- The first-version stretch assumes an account looked, before its first extract, as
  it did at that extract. That is an assumption, labelled by `version_number = 1`.
- So far only DuckDB. The project is written to run on Snowflake, Databricks and
  BigQuery through their adapters; those runs belong to those modules and are not
  claimed here.

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

**On a real scheduler.** The `in_order` scenario was also run once on `airflow
standalone` (Airflow 3.3.2, SQLite, LocalExecutor) with `airflow backfill create
--dag-id saas_daily --from-date 2026-09-01 --to-date 2026-09-30 --max-active-runs 1`.
All 30 backfill runs succeeded in 3 min 39 s, and all three tables matched the harness
exactly. Unpausing the DAG also made the scheduler create a run for the latest
interval, 17 October, which failed at `land` because no file exists for that day.
Record: [`results/airflow-scheduler-run.json`](results/airflow-scheduler-run.json).
That run is not repeated in CI.

**Limits.** One month, one file per day, no partial files: a file is either there or not.
The late file is withheld whole and delivered whole. The backwards order is the
`--run-backwards` order applied to `dag.test()`; on the real scheduler only the
in-order backfill was run. Event lateness comes from the generator's rates (2.2% late
by 1–72 hours, 0.3% by 3–10 days), so the event-time losses scale with them.

