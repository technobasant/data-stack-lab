"""Score the warehouse against what the generator knows, and write results/dbt-scd2.json.

Three questions:
  1. Revenue by plan: does the Type 2 dimension give every invoice the plan it billed?
     How much revenue does a Type 1 dimension (today's plan) move, and to which plans?
  2. Snapshot coverage: of the changes made during the year, how many did monthly
     extracts ever show?
  3. Usage by plan: what share of events lands on the wrong plan with each approach?

    python scripts/measure.py            # print and write results/dbt-scd2.json
    python scripts/measure.py --check    # fail if the result differs from the file
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

import duckdb

ROOT = Path(__file__).resolve().parents[1]
DATA = Path(os.environ.get("DATA_DIR", ROOT / "data"))
WAREHOUSE = Path(os.environ.get("DUCKDB_PATH", DATA / "warehouse.duckdb"))
RESULT = ROOT / "results" / "dbt-scd2.json"
PERIOD = ("2025-10-01", "2026-10-01")


def one(con: duckdb.DuckDBPyConnection, sql: str) -> dict:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return dict(zip(names, cur.fetchone(), strict=True))


def rows(con: duckdb.DuckDBPyConnection, sql: str) -> list[dict]:
    cur = con.execute(sql)
    names = [d[0] for d in cur.description]
    return [dict(zip(names, r, strict=True)) for r in cur.fetchall()]


def measure() -> dict:
    con = duckdb.connect(str(WAREHOUSE), read_only=True)
    truth = DATA / "truth"
    con.execute(f"create temp view history as select * from '{truth}/account_history.parquet'")
    con.execute(f"create temp view invoice_plans as select * from '{truth}/invoice_plans.parquet'")
    con.execute("create temp view plans as select * from marts.dim_plan")
    manifest = json.loads((DATA / "manifest.json").read_text())

    model = one(
        con,
        """
        with v as (
            select *, lag(plan_id) over w as prev_plan, lag(status) over w as prev_status,
                   lag(country) over w as prev_country
            from marts.dim_account
            window w as (partition by account_id order by valid_from)
        )
        select count(distinct account_id) as accounts,
               count(*) as account_versions,
               -- a change made and undone between two extracts still moves updated_at,
               -- so the timestamp strategy records a version that looks like the last one
               count(*) filter (where version_number > 1 and plan_id = prev_plan
                                  and status = prev_status and country = prev_country)
                   as versions_same_as_previous
        from v
        """,
    )

    # 1. Revenue. billed = Type 2 on the invoice's version; truth = the generator's plan.
    con.execute(
        """
        create temp view attributed as
        select f.invoice_id, f.amount_usd, billed.plan_name as type2_plan,
               cur.plan_name as type1_plan, cur.status as current_status,
               p.plan_name as true_plan
        from marts.fct_invoices f
        join marts.dim_account billed on billed.account_key = f.account_key
        join marts.dim_account cur on cur.account_id = f.account_id and cur.is_current
        join invoice_plans t on t.invoice_id = f.invoice_id
        join plans p on p.plan_id = t.plan_id
        """
    )
    revenue = one(
        con,
        """
        select count(*) as invoices,
               sum(amount_usd)::double as revenue_usd,
               count(*) filter (where type2_plan = true_plan) as type2_invoices_right,
               count(*) filter (where type1_plan = true_plan) as type1_invoices_right,
               coalesce(sum(amount_usd) filter (where type2_plan <> true_plan), 0)::double
                   as type2_usd_on_wrong_plan,
               sum(amount_usd) filter (where type1_plan <> true_plan)::double
                   as type1_usd_on_wrong_plan
        from attributed
        """,
    )
    revenue["type1_share_on_wrong_plan"] = round(
        revenue["type1_usd_on_wrong_plan"] / revenue["revenue_usd"], 4
    )
    by_plan = rows(
        con,
        """
        with t as (select true_plan as plan, sum(amount_usd) as usd from attributed group by 1),
             one as (select type1_plan as plan, sum(amount_usd) as usd from attributed group by 1),
             two as (select type2_plan as plan, sum(amount_usd) as usd from attributed group by 1)
        select p.plan_name as plan,
               coalesce(t.usd, 0)::double as true_usd,
               coalesce(two.usd, 0)::double as type2_usd,
               coalesce(one.usd, 0)::double as type1_usd
        from plans p
        left join t on t.plan = p.plan_name
        left join one on one.plan = p.plan_name
        left join two on two.plan = p.plan_name
        order by p.plan_id
        """,
    )
    for r in by_plan:
        r["type1_vs_true"] = (
            round((r["type1_usd"] - r["true_usd"]) / r["true_usd"], 4) if r["true_usd"] else None
        )
    revenue["by_plan"] = by_plan

    # Type 2 matches the truth on every invoice, so "billed on" below is also the truth.
    # Revenue billed on one plan and reported on another under Type 1, largest flows first.
    revenue["wrong_plan_flows"] = rows(
        con,
        """
        select type2_plan as billed_plan, type1_plan as current_plan,
               count(*) as invoices, sum(amount_usd)::double as revenue_usd
        from attributed
        where type2_plan <> type1_plan
        group by 1, 2
        order by revenue_usd desc
        """,
    )
    # Of the revenue Type 1 moves, how much belongs to accounts that have since churned
    # (and so keep their last plan: it was a plan change before the churn that moved it).
    revenue["churned_wrong_plan"] = one(
        con,
        """
        select count(*) as invoices, sum(amount_usd)::double as revenue_usd
        from attributed
        where type2_plan <> type1_plan and current_status = 'churned'
        """,
    )

    # 2. Which of the year's changes did a monthly extract ever show? A change is seen if
    # some version starts at its exact time (the timestamp strategy dates versions from
    # updated_at). Signups are new accounts, not changes, so they are left out.
    coverage = one(
        con,
        f"""
        with changes as (
            select account_id, changed_at, change from history
            where change <> 'signup'
              and changed_at >= '{PERIOD[0]}' and changed_at < '{PERIOD[1]}'
        )
        select count(*) as changes,
               count(v.account_id) as seen,
               count(*) filter (where change = 'trial_ended') as trial_endings,
               count(v.account_id) filter (where change = 'trial_ended') as trial_endings_seen
        from changes c
        left join snapshots.snap_accounts v
          on v.account_id = c.account_id and v.dbt_valid_from = c.changed_at
        """,
    )
    coverage["seen_share"] = round(coverage["seen"] / coverage["changes"], 4)

    # The same question, by the kind of change, in the order MEASUREMENTS.md reports them.
    coverage["by_change"] = rows(
        con,
        f"""
        with changes as (
            select account_id, changed_at, change from history
            where change <> 'signup'
              and changed_at >= '{PERIOD[0]}' and changed_at < '{PERIOD[1]}'
        )
        select c.change,
               count(*) as made,
               count(*) filter (where v.account_id is null) as never_seen
        from changes c
        left join snapshots.snap_accounts v
          on v.account_id = c.account_id and v.dbt_valid_from = c.changed_at
        group by 1
        order by case c.change
            when 'upgrade' then 1 when 'downgrade' then 2 when 'trial_ended' then 3
            when 'churn' then 4 when 'moved' then 5 end
        """,
    )
    # What a missed change looked like by the next extract: the next change the account
    # made before any snapshot caught up, which is what the snapshot actually recorded
    # in its place.
    coverage["missed_replaced_by"] = rows(
        con,
        f"""
        with changes as (
            select account_id, changed_at, change,
                   row_number() over (partition by account_id order by changed_at) as rn
            from history
            where change <> 'signup'
        ),
        period_changes as (
            select * from changes
            where changed_at >= '{PERIOD[0]}' and changed_at < '{PERIOD[1]}'
        ),
        missed as (
            select c.account_id, c.change, c.rn
            from period_changes c
            left join snapshots.snap_accounts v
              on v.account_id = c.account_id and v.dbt_valid_from = c.changed_at
            where v.account_id is null
        )
        select n.change as replaced_by, m.change as missed_change, count(*) as changes
        from missed m
        join changes n on n.account_id = m.account_id and n.rn = m.rn + 1
        group by 1, 2
        order by 1, 2
        """,
    )

    # 3. Usage: every deduplicated event against the plan it truly happened on.
    usage = one(
        con,
        """
        with ev as (
            select e.event_id, e.account_id, e.event_at, a.plan_name as type2_plan,
                   cur.plan_name as type1_plan
            from staging.stg_tracking__events e
            join marts.dim_account a
              on a.account_id = e.account_id
             and e.event_at >= a.valid_from and e.event_at < a.valid_to
            join marts.dim_account cur on cur.account_id = e.account_id and cur.is_current
        ),
        truth as (
            select ev.*, p.plan_name as true_plan
            from ev
            asof join history h
              on h.account_id = ev.account_id and h.changed_at <= ev.event_at
            join plans p on p.plan_id = h.plan_id
        )
        select count(*) as events,
               count(*) filter (where type2_plan <> true_plan) as type2_events_wrong,
               count(*) filter (where type1_plan <> true_plan) as type1_events_wrong
        from truth
        """,
    )
    usage["type2_share_wrong"] = round(usage["type2_events_wrong"] / usage["events"], 4)
    usage["type1_share_wrong"] = round(usage["type1_events_wrong"] / usage["events"], 4)

    delivered = manifest["rows"]["events"]
    events = {
        "delivered": delivered,
        "unique": usage["events"],
        "duplicate_share": round(1 - usage["events"] / delivered, 4),
    }
    return {
        "dataset": {"scale": manifest["scale"], "seed": manifest["seed"]},
        "model": model,
        "revenue": revenue,
        "snapshot_coverage": coverage,
        "usage": usage,
        "events": events,
    }


def summary(m: dict) -> str:
    r, c, u = m["revenue"], m["snapshot_coverage"], m["usage"]
    lines = [
        f"accounts {m['model']['accounts']:,}, versions {m['model']['account_versions']:,} "
        f"({m['model']['versions_same_as_previous']} identical to the one before)",
        f"invoices {r['invoices']:,}, revenue ${r['revenue_usd']:,.0f}",
        f"  Type 2 right plan: {r['type2_invoices_right']:,}/{r['invoices']:,}",
        f"  Type 1 right plan: {r['type1_invoices_right']:,}/{r['invoices']:,}; "
        f"${r['type1_usd_on_wrong_plan']:,.0f} ({r['type1_share_on_wrong_plan']:.1%}) "
        "on the wrong plan",
    ]
    for p in r["by_plan"]:
        delta = "" if p["type1_vs_true"] is None else f" ({p['type1_vs_true']:+.1%})"
        lines.append(
            f"    {p['plan']:<11} true ${p['true_usd']:>9,.0f}   Type 1 ${p['type1_usd']:>9,.0f}"
            + delta
        )
    lines.append(
        f"  wrong-plan flows: {len(r['wrong_plan_flows'])}; of those, churned accounts carry "
        f"${r['churned_wrong_plan']['revenue_usd']:,.0f} ({r['churned_wrong_plan']['invoices']} "
        "invoices)"
    )
    lines += [
        f"changes in the year {c['changes']}, seen by monthly snapshots {c['seen']} "
        f"({c['seen_share']:.1%}); trial endings seen {c['trial_endings_seen']}/"
        f"{c['trial_endings']}",
    ]
    for row in c["by_change"]:
        lines.append(
            f"    {row['change']:<12} made {row['made']:>3}  never seen {row['never_seen']:>2}"
        )
    lines.append(
        f"events {u['events']:,}: wrong plan Type 2 {u['type2_share_wrong']:.2%}, "
        f"Type 1 {u['type1_share_wrong']:.2%}"
    )
    return "\n".join(lines)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="compare with the published file")
    args = ap.parse_args()
    m = measure()
    print(summary(m))
    text = json.dumps(m, indent=2) + "\n"
    if args.check:
        if not RESULT.exists() or RESULT.read_text() != text:
            sys.exit(f"{RESULT.relative_to(ROOT)} does not match this run")
        print(f"matches {RESULT.relative_to(ROOT)}")
    else:
        RESULT.parent.mkdir(exist_ok=True)
        RESULT.write_text(text)


if __name__ == "__main__":
    main()
