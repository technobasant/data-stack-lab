"""The DAGs load, are wired as intended, and the two schedules give the run windows the
Airflow module's results depend on. Skipped unless the `airflow` group is installed."""

from __future__ import annotations

import os
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

pytest.importorskip("airflow")


@pytest.fixture(scope="module")
def dags(tmp_path_factory: pytest.TempPathFactory) -> dict:
    os.environ["AIRFLOW_HOME"] = str(tmp_path_factory.mktemp("airflow-home"))
    os.environ["AIRFLOW__CORE__LOAD_EXAMPLES"] = "False"
    sys.path.insert(0, str(ROOT / "airflow" / "dags"))
    import saas_daily

    return {d.dag_id: d for d in (saas_daily.saas_daily, saas_daily.saas_daily_default_cron)}


def test_both_dags_load(dags: dict) -> None:
    assert set(dags) == {"saas_daily", "saas_daily_default_cron"}


@pytest.mark.parametrize("dag_id", ["saas_daily", "saas_daily_default_cron"])
def test_land_then_transform(dags: dict, dag_id: str) -> None:
    dag = dags[dag_id]
    assert set(dag.task_dict) == {"land", "transform"}
    assert dag.task_dict["land"].downstream_task_ids == {"transform"}


@pytest.mark.parametrize("dag_id", ["saas_daily", "saas_daily_default_cron"])
def test_one_run_at_a_time_with_retries(dags: dict, dag_id: str) -> None:
    dag = dags[dag_id]
    assert dag.max_active_runs == 1
    assert dag.catchup is False
    assert all(t.retries == 2 for t in dag.tasks)


def test_saas_daily_covers_the_day_that_just_ended(dags: dict) -> None:
    fired = datetime(2026, 9, 11, tzinfo=UTC)
    interval = dags["saas_daily"].timetable.infer_manual_data_interval(run_after=fired)
    assert (interval.start, interval.end) == (datetime(2026, 9, 10, tzinfo=UTC), fired)


def test_default_daily_has_no_window(dags: dict) -> None:
    """Airflow 3's "@daily" is a CronTriggerTimetable. If this starts failing, the default
    has changed and the Airflow module's default_cron results need re-running."""
    from airflow.timetables.trigger import CronTriggerTimetable

    assert type(dags["saas_daily_default_cron"].timetable).__name__ == "CronTriggerTimetable"
    fired = datetime(2026, 9, 11, tzinfo=UTC)
    interval = CronTriggerTimetable("0 0 * * *", timezone="UTC").infer_manual_data_interval(
        run_after=fired
    )
    assert interval.end - interval.start == timedelta(0)
