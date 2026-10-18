"""The generator's promises: the same seed gives the same data, and the business rules
the models rely on hold in every file."""

from __future__ import annotations

from pathlib import Path

import duckdb
import pyarrow.parquet as pq
import pytest

from datagen.saas import MONTH_STARTS, generate


@pytest.fixture(scope="module")
def tiny(tmp_path_factory: pytest.TempPathFactory) -> Path:
    out = tmp_path_factory.mktemp("tiny")
    generate("tiny", seed=7, out=out)
    return out


@pytest.fixture(scope="module")
def con(tiny: Path) -> duckdb.DuckDBPyConnection:
    con = duckdb.connect()
    for name in ("plans", "users", "invoices", "events"):
        con.execute(f"create view {name} as select * from '{tiny}/raw/{name}.parquet'")
    con.execute(f"create view history as select * from '{tiny}/truth/account_history.parquet'")
    con.execute(f"create view billed as select * from '{tiny}/truth/invoice_plans.parquet'")
    con.execute(
        "create view extracts as select *, "
        "split_part(parse_filename(filename, true), '=', 2) as extract "
        f"from read_parquet('{tiny}/raw/accounts/*.parquet', filename = true)"
    )
    return con


def scalar(con: duckdb.DuckDBPyConnection, sql: str):
    return con.execute(sql).fetchone()[0]


def test_same_seed_same_data(tiny: Path, tmp_path: Path) -> None:
    generate("tiny", seed=7, out=tmp_path)
    for f in sorted(tiny.rglob("*.parquet")):
        assert pq.read_table(f).equals(pq.read_table(tmp_path / f.relative_to(tiny))), f


def test_another_seed_other_data(tiny: Path, tmp_path: Path) -> None:
    generate("tiny", seed=8, out=tmp_path)
    assert not pq.read_table(tiny / "raw/events.parquet").equals(
        pq.read_table(tmp_path / "raw/events.parquet")
    )


def test_thirteen_monthly_extracts(tiny: Path) -> None:
    assert len(list((tiny / "raw/accounts").glob("*.parquet"))) == len(MONTH_STARTS) == 13


def test_plans_move_one_tier_at_a_time(con) -> None:
    jumps = scalar(
        con,
        """
        select count(*) from (
            select plan_id - lag(plan_id) over (partition by account_id order by change_seq) d
            from history
        ) where abs(d) > 1
        """,
    )
    assert jumps == 0


def test_nothing_happens_after_churn(con) -> None:
    after = scalar(
        con,
        """
        with churn as (select account_id, changed_at from history where change = 'churn')
        select count(*) from history h join churn c using (account_id)
        where h.changed_at > c.changed_at
        """,
    )
    assert after == 0
    events_after = scalar(
        con,
        """
        with churn as (select account_id, changed_at from history where change = 'churn')
        select count(*) from events e join churn c using (account_id)
        where e.event_at >= c.changed_at
        """,
    )
    assert events_after == 0


def test_extracts_show_state_as_of_their_date(con) -> None:
    assert scalar(con, "select count(*) from extracts where updated_at > extract::timestamp") == 0
    assert scalar(con, "select count(*) from extracts where signup_at > extract::timestamp") == 0


def test_invoices_bill_active_paid_accounts_at_the_true_price(con) -> None:
    wrong = scalar(
        con,
        """
        select count(*) from invoices i
        join billed b using (invoice_id)
        join plans p using (plan_id)
        asof join history h on h.account_id = i.account_id and h.changed_at <= i.invoiced_at
        where h.status <> 'active' or h.plan_id <> b.plan_id
           or p.monthly_price_usd = 0 or i.amount_usd <> p.monthly_price_usd
        """,
    )
    assert wrong == 0
    assert scalar(con, "select count(*) from invoices") > 0


def test_events_only_from_existing_users_of_their_account(con) -> None:
    orphans = scalar(
        con,
        """
        select count(*) from events e left join users u using (user_id)
        where u.user_id is null or u.account_id <> e.account_id or e.event_at < u.created_at
        """,
    )
    assert orphans == 0


def test_duplicates_are_redeliveries_of_the_same_event(con) -> None:
    differing = scalar(
        con,
        """
        select count(*) from (
            select event_id from events group by event_id
            having count(*) > 1 and (count(distinct event_at) > 1
                                     or count(distinct user_id) > 1
                                     or count(distinct event_type) > 1)
        )
        """,
    )
    assert differing == 0
    assert scalar(con, "select count(*) - count(distinct event_id) from events") > 0


def test_nothing_real_in_the_contact_details(con) -> None:
    assert scalar(con, "select count(*) from users where trim(email) not ilike '%.example'") == 0
