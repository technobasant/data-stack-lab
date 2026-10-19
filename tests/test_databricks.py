"""The recorded Databricks run did what the documented semantics predict. The workspace
run cannot happen in CI; this checks the two published files agree."""

import json
from pathlib import Path

RESULTS = Path(__file__).resolve().parents[1] / "results"


def test_workspace_run_matches_prediction() -> None:
    plan = json.loads((RESULTS / "databricks-plan.json").read_text())
    run = json.loads((RESULTS / "databricks.json").read_text())
    assert run["loading"] == plan["predicted"]
    assert run["matches_prediction"] is True


def test_only_auto_loader_with_overwrites_and_merge_is_exact() -> None:
    run = json.loads((RESULTS / "databricks.json").read_text())
    exact = [
        name
        for name, r in run["loading"].items()
        if r["missing_events"] == 0 and r["duplicate_rows"] == 0
    ]
    assert exact == ["auto_loader_merge"]


def test_the_dbt_project_on_databricks_matches_duckdb() -> None:
    run = json.loads((RESULTS / "dbt-databricks.json").read_text())
    duck = json.loads((RESULTS / "dbt-scd2.json").read_text())
    assert all(run["matches_duckdb"].values())
    assert (
        run["score"]["revenue"]["type1_usd_on_wrong_plan"]
        == duck["revenue"]["type1_usd_on_wrong_plan"]
    )
    assert run["build"]["statuses"]["model"] == {"success": 11}
    assert run["build"]["statuses"]["test"] == {"pass": 46}
