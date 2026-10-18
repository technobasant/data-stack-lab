# Data quality: nine defects, three layers of checks, two policies

The data-quality project of [data-stack-lab](..). A month of daily event deliveries,
September 2026, with one realistic defect every three days, checked in dbt
([`../dbt/models/quality/`](../dbt/models/quality)) and promoted under two policies.
Every delivered row is labelled good or bad by what the defect did to it, so each
policy can be scored. Method and figures:
[MEASUREMENTS](../MEASUREMENTS.md#data-quality-nine-defects-two-policies).

## The checks

| Layer | Checks | Catches |
|---|---|---|
| Contract | the batch model selects its columns by name, under an enforced dbt contract | a renamed or missing column |
| Rows | not null; unique after deduplication; account exists; event type and platform allowed; event not after its arrival | bad values in some rows |
| The day as a whole | volume within 30% of the last 7 accepted days; each platform's share within 8 points; rows per file equal the producer's manifest | rows that are missing, not wrong |
| Warning only | redeliveries above 5% of the day | a duplicate file that deduplication already made harmless |

Delivery is at least once, so 0.8% of events arrive twice on an ordinary day. The batch
model deduplicates first; a `unique` test on raw deliveries would fail every day and soon
be ignored.

## What each defect did, and what caught it

| Day | Defect | Caught by | Block | Quarantine |
|---|---|---|---|---|
| 3 Sep | the day's file delivered twice | nothing failed; redelivery warning | promoted; duplicates absorbed | the same |
| 6 Sep | 5% of events with no account (147 rows) | `not_null` | held | 147 quarantined, 2,816 promoted |
| 9 Sep | `event_type` renamed to `type` | the contract | held | held |
| 12 Sep | upload stopped at 40% (1,709 rows missing) | manifest, volume | held | held |
| 15 Sep | event times in UTC+5 labelled UTC (2,992 rows) | event after arrival | held | quarantined, but **87 bad rows promoted** |
| 18 Sep | a new, legitimate event type on 3% | `accepted_values` | held | **92 good rows quarantined** |
| 21 Sep | the iOS SDK stops sending (613 rows missing) | platform share | held | held |
| 24 Sep | 2% of events with an unknown account (59 rows) | `relationships` | held | 59 quarantined |
| 27 Sep | a third of the day arrives two days late (1,040 rows) | volume, in one run of two | **promoted** | held |
| 29 Sep | the late third arrives with the day's own events | volume | held | held |

Over the month:

| | Block | Quarantine |
|---|---:|---:|
| Bad rows promoted, of 3,198 delivered | **0** | 87 |
| Good rows not promoted | 19,545 | 12,865 |
| Days held for a person | 8 | 5 |
| Rows never delivered (not detectable as bad rows) | 3,362 | 3,362 |
| Clean days with an alarm | 0 of 20 | 0 of 20 |

## What it shows

- **Missing data needs different checks from wrong data.** The truncated upload, the
  silent platform and the late third left no bad rows to find. Only the manifest, the
  volume band and the per-platform share saw them.
- **A total can hide a loss that a share shows.** iOS going silent cut the day's
  volume by 19%, inside the band; it moved iOS's own share from about 18% to 0.
- **A fixed band is a coin toss at its edge.** The late third was 29.6% below one
  policy's baseline and 30.2% below the other's, because the two policies had held
  different earlier days. The 30% band missed it once and caught it once.
- **Quarantine trades a few bad rows for many good ones.** It kept 6,680 more good rows
  flowing than blocking did, and let 87 rows with wrong timestamps through: events that
  had already arrived hours late, so a five-hour shift did not put them in the future.
- **Rule checks cannot tell a defect from a product change.** The new event type was
  legitimate; blocking held the day, quarantine set aside 92 good rows. A contract change
  needs a process, not a tighter check.

## Running it

```bash
make quality                              # writes results/quality.json
uv run python quality/run.py --check      # what CI runs: the published figures, exactly
```

Each policy gets its own DuckDB file under `data/quality/`. August is accepted clean to
give the batch checks a baseline. The run takes about three minutes; it needs neither
Airflow nor the other projects' outputs.
