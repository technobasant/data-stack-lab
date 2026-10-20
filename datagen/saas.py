"""Reproducible generated data for one B2B subscription SaaS business scenario.

Every project in this repository runs on this dataset, so figures line up across
tools. Nothing in it is real: people, companies and e-mail addresses are generated,
and every domain sits under the reserved `.example` top-level domain.

The business
  plans     free, team ($49 a month), business ($199), enterprise ($999)
  accounts  companies that sign up, upgrade, downgrade, try a higher plan and
            drop back, now and then move country, and churn
  users     the people in those accounts
  invoices  one per paid account on the first of each month, billed in advance
  events    product usage, delivered at least once: some arrive late, some twice

What lands in --out
  raw/accounts/extract_date=YYYY-MM-DD.parquet  the app database's accounts table,
                                                 extracted in full on the first of
                                                 each month (13 extracts)
  raw/{plans,users,invoices,events}.parquet      the other sources, as delivered
  truth/account_history.parquet                  every account change and its time
  truth/invoice_plans.parquet                    the plan each invoice billed
  manifest.json                                  scale, seed and row counts

The warehouse only ever sees raw/. The truth files are what the generator knows
and a real warehouse never would; the measurements score the models against them.

    python -m datagen --scale lab --seed 7 --out data
"""

from __future__ import annotations

import argparse
import heapq
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from decimal import Decimal
from pathlib import Path

import duckdb
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq

DAY = 86_400
MONEY = pa.decimal128(10, 2)
YEAR = 365 * DAY


def _epoch(year: int, month: int, day: int = 1) -> int:
    return int(datetime(year, month, day, tzinfo=UTC).timestamp())


HISTORY_START = _epoch(2023, 1)  # the oldest accounts signed up then
# Month starts from 1 Oct 2025 to 1 Oct 2026: twelve billing months, thirteen extracts.
MONTH_STARTS = [_epoch(2025 + (9 + k) // 12, (9 + k) % 12 + 1) for k in range(13)]
PERIOD_START, PERIOD_END = MONTH_STARTS[0], MONTH_STARTS[-1]
PERIOD_DAYS = (PERIOD_END - PERIOD_START) // DAY

PLANS = [(1, "free", 0), (2, "team", 49), (3, "business", 199), (4, "enterprise", 999)]
PRICE = {plan_id: price for plan_id, _, price in PLANS}
SIGNUP_PLAN_SHARE = [0.45, 0.35, 0.15, 0.05]

# What happens at each plan event, by current plan: (upgrade, downgrade, churn).
TRANSITIONS = {
    1: (0.75, 0.00, 0.25),
    2: (0.50, 0.25, 0.25),
    3: (0.40, 0.35, 0.25),
    4: (0.00, 0.60, 0.40),
}
PLAN_EVENTS_PER_YEAR = 0.8
TRIAL_SHARE = 0.20  # upgrades that are undone 3-25 days later
COUNTRY_MOVES_PER_YEAR = 0.05

COUNTRIES = ["US", "GB", "DE", "IN", "NP", "FR", "NL", "BR", "CA", "AU", "SG", "AE", "JP", "ZA"]
COUNTRY_SHARE = np.array([30, 10, 8, 10, 3, 5, 4, 5, 6, 5, 4, 3, 4, 3], dtype=float)
COUNTRY_SHARE /= COUNTRY_SHARE.sum()
INDUSTRIES = ["software", "retail", "healthcare", "finance", "education", "logistics", "media"]
ADJECTIVES = """Amber Bright Cedar Delta Ember Fable Granite Harbor Indigo Juniper Kite Lumen
    Maple North Onyx Pine Quartz River Summit Tidal Vale Willow Zephyr""".split()
NOUNS = """Analytics Labs Works Systems Health Logistics Studio Foods Robotics Finance Media
    Energy Retail Travel Learning""".split()
FIRST = """Aarav Maya Liam Sita Noah Elena Kenji Amara Lucas Priya Omar Sofia Ravi Hana Mateo
    Zara Ethan Anika Leo Chloe""".split()
LAST = """Shrestha Garcia Smith Tanaka Okafor Muller Rossi Khan Silva Nguyen Dubois Patel
    Kowalski Jensen Haddad Moreau Rai Costa""".split()

USERS_BY_PLAN = {1: (1, 4), 2: (2, 13), 3: (8, 41), 4: (25, 151)}  # [low, high) at signup
ACTIVITY_BY_PLAN = np.array([0.0, 0.6, 1.0, 1.2, 1.4])  # indexed by plan_id
EVENT_TYPES = """login view_dashboard create_report share_report export_csv invite_user
    api_call""".split()
EVENT_SHARE = np.array([22, 38, 9, 5, 6, 2, 18], dtype=float)
EVENT_SHARE /= EVENT_SHARE.sum()
PLATFORMS = ["web", "ios", "android"]
PLATFORM_SHARE = [0.70, 0.18, 0.12]
EXTRA_EVENTS_PER_ACTIVE_DAY = 2.5  # Poisson mean on top of the first event
LATE_SHARE, VERY_LATE_SHARE = 0.025, 0.003  # arrive 1-72 h late / 3-10 days late
DUPLICATE_SHARE = 0.008  # delivered twice
MESSY_EMAIL_SHARE = 0.03  # upper case and a trailing space, as typed


@dataclass(frozen=True)
class Scale:
    accounts: int
    active_rate: float  # chance a user on a team plan is active on a given day


SCALES = {"tiny": Scale(40, 0.10), "lab": Scale(1_000, 0.10)}


def _ts(seconds: np.ndarray | list[int]) -> pa.Array:
    """Unix seconds -> naive UTC timestamps. Every timestamp in the dataset is UTC."""
    micros = np.asarray(seconds, dtype=np.int64) * 1_000_000
    return pa.array(micros, type=pa.timestamp("us"))


def _uuids(rng: np.random.Generator, n: int) -> list[str]:
    raw = rng.bytes(16 * n).hex()
    return [
        f"{raw[i : i + 8]}-{raw[i + 8 : i + 12]}-{raw[i + 12 : i + 16]}-"
        f"{raw[i + 16 : i + 20]}-{raw[i + 20 : i + 32]}"
        for i in range(0, 32 * n, 32)
    ]


def account_history(
    rng: np.random.Generator, account_id: int, signup: int, plan: int, country: str
) -> list[tuple]:
    """Every change to one account from signup to the end of the period, in order."""
    rows = [(account_id, signup, plan, "active", country, "signup")]
    queue: list[tuple] = []
    seq = 0
    for kind, rate in (("plan", PLAN_EVENTS_PER_YEAR), ("move", COUNTRY_MOVES_PER_YEAR)):
        t = signup
        while True:
            t += int(rng.exponential(YEAR / rate)) + 1
            if t >= PERIOD_END:
                break
            heapq.heappush(queue, (t, seq, kind, None))
            seq += 1
    while queue:
        t, _, kind, payload = heapq.heappop(queue)
        if kind == "move":
            moved_to = COUNTRIES[int(rng.integers(len(COUNTRIES)))]
            if moved_to == country:
                continue
            country, change = moved_to, "moved"
        elif kind == "revert":
            before, after = payload
            if plan != after:  # changed again since the trial started
                continue
            plan, change = before, "trial_ended"
        else:
            up, down, _ = TRANSITIONS[plan]
            r = rng.random()
            if r < up:
                if rng.random() < TRIAL_SHARE:
                    revert_at = t + int(rng.integers(3 * DAY, 25 * DAY))
                    if revert_at < PERIOD_END:
                        heapq.heappush(queue, (revert_at, seq, "revert", (plan, plan + 1)))
                        seq += 1
                plan, change = plan + 1, "upgrade"
            elif r < up + down:
                plan, change = plan - 1, "downgrade"
            else:
                rows.append((account_id, t, plan, "churned", country, "churn"))
                break
        rows.append((account_id, t, plan, "active", country, change))
    return rows


def generate(scale: str = "lab", seed: int = 7, out: Path = Path("data")) -> dict:
    s = SCALES[scale]
    rng = np.random.default_rng(seed)
    n = s.accounts

    # Accounts: ids are issued in signup order, like an auto-increment key.
    existing = rng.random(n) < 0.7
    signup = np.sort(
        np.where(
            existing,
            rng.integers(HISTORY_START, PERIOD_START, n),
            rng.integers(PERIOD_START, PERIOD_END - 15 * DAY, n),
        )
    )
    first_plan = rng.choice([1, 2, 3, 4], size=n, p=SIGNUP_PLAN_SHARE)
    first_country = rng.choice(COUNTRIES, size=n, p=COUNTRY_SHARE)
    industry = rng.choice(INDUSTRIES, size=n)
    names = [
        f"{ADJECTIVES[a]} {NOUNS[b]}"
        for a, b in zip(
            rng.integers(len(ADJECTIVES), size=n), rng.integers(len(NOUNS), size=n), strict=True
        )
    ]
    ids = np.arange(1, n + 1)
    domains = [
        f"{name.lower().replace(' ', '-')}-{i}.example" for name, i in zip(names, ids, strict=True)
    ]

    history: list[tuple] = []
    for i in range(n):
        rows = account_history(
            rng, int(ids[i]), int(signup[i]), int(first_plan[i]), str(first_country[i])
        )
        history += [(*row, seq) for seq, row in enumerate(rows, start=1)]
    hist = pa.table(
        {
            "account_id": pa.array([h[0] for h in history], pa.int64()),
            "changed_at": _ts([h[1] for h in history]),
            "plan_id": pa.array([h[2] for h in history], pa.int64()),
            "status": [h[3] for h in history],
            "country": [h[4] for h in history],
            "change": [h[5] for h in history],
            "change_seq": pa.array([h[6] for h in history], pa.int64()),
        }
    )
    accounts = pa.table(
        {
            "account_id": pa.array(ids, pa.int64()),
            "account_name": names,
            "domain": domains,
            "industry": industry.tolist(),
            "signup_at": _ts(signup),
        }
    )

    con = duckdb.connect()
    con.register("history", hist)
    con.register("accounts", accounts)
    state_at = """
        select a.account_id, a.account_name, a.domain, a.industry, h.country, h.plan_id,
               h.status, a.signup_at, h.changed_at as updated_at
        from accounts a
        join (
            select *, row_number() over (partition by account_id
                                         order by change_seq desc) as rn
            from history where changed_at <= make_timestamp(?::bigint * 1000000)
        ) h on h.account_id = a.account_id and h.rn = 1
        order by a.account_id
    """
    (out / "raw" / "accounts").mkdir(parents=True, exist_ok=True)
    (out / "truth").mkdir(parents=True, exist_ok=True)
    counts: dict[str, int] = {}
    invoice_rows = []
    for k, month_start in enumerate(MONTH_STARTS):
        extract = con.execute(state_at, [month_start]).to_arrow_table()
        day = datetime.fromtimestamp(month_start, UTC).date().isoformat()
        pq.write_table(extract, out / "raw" / "accounts" / f"extract_date={day}.parquet")
        counts[f"accounts@{day}"] = extract.num_rows
        if k < len(MONTH_STARTS) - 1:  # bill the twelve months that start inside the period
            for account_id, plan_id, status in zip(
                *(extract.column(c).to_pylist() for c in ("account_id", "plan_id", "status")),
                strict=True,
            ):
                if status == "active" and PRICE[plan_id] > 0:
                    invoice_rows.append((month_start, account_id, plan_id))

    invoice_ids = [f"INV-{i:07d}" for i in range(1, len(invoice_rows) + 1)]
    invoices = pa.table(
        {
            "invoice_id": invoice_ids,
            "account_id": pa.array([r[1] for r in invoice_rows], pa.int64()),
            "invoiced_at": _ts([r[0] for r in invoice_rows]),
            # The billing export carries money, not the plan: that is the warehouse's job.
            "amount_usd": pa.array([Decimal(PRICE[r[2]]) for r in invoice_rows], MONEY),
            "currency": ["USD"] * len(invoice_rows),
        }
    )
    invoice_truth = pa.table(
        {"invoice_id": invoice_ids, "plan_id": pa.array([r[2] for r in invoice_rows], pa.int64())}
    )

    # Users: how many an account has depends on the plan it signed up on.
    user_account, user_created = [], []
    for i in range(n):
        lo, hi = USERS_BY_PLAN[int(first_plan[i])]
        k = int(rng.integers(lo, hi))
        horizon = max(int(signup[i]) + 1, min(int(signup[i]) + 180 * DAY, PERIOD_END - DAY))
        created = np.concatenate(
            [[signup[i] + 60], rng.integers(int(signup[i]) + 60, horizon, k - 1)]
        )
        user_account += [int(ids[i])] * k
        user_created += np.sort(created).tolist()
    n_users = len(user_account)
    user_ids = np.arange(1, n_users + 1)
    first = rng.choice(FIRST, size=n_users)
    last = rng.choice(LAST, size=n_users)
    emails = [
        f"{f.lower()}.{la.lower()}{u}@{domains[a - 1]}"
        for f, la, u, a in zip(first, last, user_ids, user_account, strict=True)
    ]
    messy = rng.random(n_users) < MESSY_EMAIL_SHARE
    emails = [e.upper() + " " if m else e for e, m in zip(emails, messy, strict=True)]
    is_first = np.r_[True, np.diff(user_account) != 0]
    users = pa.table(
        {
            "user_id": pa.array(user_ids, pa.int64()),
            "account_id": pa.array(user_account, pa.int64()),
            "email": emails,
            "full_name": [f"{f} {la}" for f, la in zip(first, last, strict=True)],
            "role": np.where(is_first, "admin", "member").tolist(),
            "created_at": _ts(user_created),
        }
    )

    # Events: a user is active on a day if the account was active all day and the user
    # existed before it started; busier plans are more active.
    day_start = PERIOD_START + DAY * np.arange(PERIOD_DAYS)
    by_account: dict[int, list[tuple]] = {}
    for h in history:
        by_account.setdefault(h[0], []).append(h)
    user_created_arr = np.asarray(user_created)
    user_account_arr = np.asarray(user_account)
    parts = []
    for account_id in ids.tolist():
        rows = by_account[account_id]
        h_ts = np.array([r[1] for r in rows])
        h_plan = np.array([r[2] for r in rows])
        h_active = np.array([r[3] == "active" for r in rows])
        at_start = np.searchsorted(h_ts, day_start, side="right") - 1
        at_end = np.searchsorted(h_ts, day_start + DAY - 1, side="right") - 1
        up_all_day = (at_start >= 0) & h_active[at_end] & h_active[at_start]
        p_day = np.where(up_all_day, s.active_rate * ACTIVITY_BY_PLAN[h_plan[at_start]], 0.0)
        members = np.flatnonzero(user_account_arr == account_id)
        exists = user_created_arr[members][:, None] < day_start[None, :]
        active = (rng.random((len(members), PERIOD_DAYS)) < p_day[None, :]) & exists
        u, d = np.nonzero(active)
        per_day = 1 + rng.poisson(EXTRA_EVENTS_PER_ACTIVE_DAY, size=len(u))
        u, d = np.repeat(u, per_day), np.repeat(d, per_day)
        hour = np.clip(rng.normal(14, 3.5, size=len(u)), 0, 23.999).astype(np.int64)
        when = day_start[d] + hour * 3600 + rng.integers(0, 3600, size=len(u))
        parts.append((user_ids[members][u], np.full(len(u), account_id), when))
    ev_user = np.concatenate([p[0] for p in parts])
    ev_account = np.concatenate([p[1] for p in parts])
    ev_at = np.concatenate([p[2] for p in parts])
    m = len(ev_at)
    ev_type = rng.choice(EVENT_TYPES, size=m, p=EVENT_SHARE)
    ev_platform = rng.choice(PLATFORMS, size=m, p=PLATFORM_SHARE)
    delay = rng.integers(1, 120, size=m)
    lateness = rng.random(m)
    delay = np.where(lateness < LATE_SHARE, rng.integers(3600, 72 * 3600, size=m), delay)
    delay = np.where(lateness < VERY_LATE_SHARE, rng.integers(3 * DAY, 10 * DAY, size=m), delay)
    ev_id = np.array(_uuids(rng, m))
    received = ev_at + delay
    dup = np.flatnonzero(rng.random(m) < DUPLICATE_SHARE)
    pick = np.concatenate([np.arange(m), dup])
    received = np.concatenate([received, received[dup] + rng.integers(1, 7200, size=len(dup))])
    order = np.lexsort((ev_id[pick], received))  # the order they were received in
    pick, received = pick[order], received[order]
    events = pa.table(
        {
            "event_id": ev_id[pick].tolist(),
            "user_id": pa.array(ev_user[pick], pa.int64()),
            "account_id": pa.array(ev_account[pick], pa.int64()),
            "event_type": ev_type[pick].tolist(),
            "platform": ev_platform[pick].tolist(),
            "event_at": _ts(ev_at[pick]),
            "received_at": _ts(received),
        }
    )

    plans = pa.table(
        {
            "plan_id": pa.array([p[0] for p in PLANS], pa.int64()),
            "plan_name": [p[1] for p in PLANS],
            "monthly_price_usd": pa.array([Decimal(p[2]) for p in PLANS], MONEY),
        }
    )
    sources = {"plans": plans, "users": users, "invoices": invoices, "events": events}
    for name, table in sources.items():
        pq.write_table(table, out / "raw" / f"{name}.parquet", compression="zstd")
        counts[name] = table.num_rows
    pq.write_table(hist, out / "truth" / "account_history.parquet")
    pq.write_table(invoice_truth, out / "truth" / "invoice_plans.parquet")
    counts["account_history"] = hist.num_rows
    manifest = {"scale": scale, "seed": seed, "rows": counts}
    (out / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
    return manifest


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    ap.add_argument("--scale", choices=sorted(SCALES), default="lab")
    ap.add_argument("--seed", type=int, default=7)
    ap.add_argument("--out", type=Path, default=Path("data"))
    args = ap.parse_args(argv)
    manifest = generate(args.scale, args.seed, args.out)
    for name, rows in manifest["rows"].items():
        print(f"{name:28} {rows:>10,}")
