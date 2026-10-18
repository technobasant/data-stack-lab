"""Break the models on purpose and check that the build catches every break.

Passing tests prove little until they have been seen to fail. Each mutant below is a
mistake that is easy to make in a Type 2 model. Each is applied to a copy of the
project and built twice against a copy of the warehouse: once with the unit tests
(logic checked on hand-written rows) and once with the data tests alone (checks on
the real data). Every mutant must be caught by at least one of the two.

    python scripts/mutants.py            # after scripts/warehouse.py
    python scripts/mutants.py --check    # also compare with results/mutants-<engine>.json

Which tests catch which mutant is recorded per dbt engine, because the engines
differ: dbt v2 2.0.8 never compares timestamp columns in a unit test's expected rows
(dbt-labs/dbt#15894), so two breaks that dbt Core's unit tests catch pass v2's. The
check fails when that changes, in either direction.

Exits non-zero if any mutant survives, or with --check if the record differs.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent))
from warehouse import DBT_DIR, WAREHOUSE, dbt_command  # noqa: E402


@dataclass(frozen=True)
class Mutant:
    name: str
    file: str
    find: str
    replace: str


MUTANTS = [
    Mutant(
        "invoices joined to today's version (Type 1)",
        "models/marts/fct_invoices.sql",
        """        and i.invoiced_at >= a.valid_from
        and i.invoiced_at < a.valid_to""",
        "        and a.is_current",
    ),
    Mutant(
        "first version dated from the snapshot, not signup",
        "models/marts/dim_account.sql",
        "case when v.version_number = 1 then v.signup_at else v.dbt_valid_from end",
        "v.dbt_valid_from",
    ),
    Mutant(
        "open version left with a NULL end",
        "models/marts/dim_account.sql",
        "coalesce(v.dbt_valid_to, cast('9999-12-31 00:00:00' as timestamp))",
        "v.dbt_valid_to",
    ),
    Mutant(
        "redelivered events not deduplicated",
        "models/staging/stg_tracking__events.sql",
        "where delivery = 1",
        "where delivery >= 1",
    ),
]


def build(mutant: Mutant, workdir: Path, data_tests_only: bool) -> dict[str, list[str]]:
    """Build the mutated project; return the failed unit tests and data tests."""
    project = workdir / "dbt"
    shutil.copytree(DBT_DIR, project, ignore=shutil.ignore_patterns("target", "logs"))
    path = project / mutant.file
    source = path.read_text()
    if mutant.find not in source:
        raise SystemExit(f"mutant '{mutant.name}': pattern not found in {mutant.file}")
    path.write_text(source.replace(mutant.find, mutant.replace))
    warehouse = workdir / "warehouse.duckdb"
    shutil.copy(WAREHOUSE, warehouse)
    exclude = ["resource_type:snapshot"] + (["resource_type:unit_test"] if data_tests_only else [])
    subprocess.run(
        [
            *dbt_command(),
            "build",
            "--exclude",
            *exclude,
            "--project-dir",
            ".",
            "--profiles-dir",
            ".",
        ],
        cwd=project,
        env={**os.environ, "DUCKDB_PATH": str(warehouse)},
        capture_output=True,
        text=True,
    )
    results = json.loads((project / "target" / "run_results.json").read_text())["results"]
    failed: dict[str, list[str]] = {"unit_test": [], "test": []}
    for r in results:
        # unit_test.saas.<model>.<name> and test.saas.<name>.<hash>
        kind, _, *rest = r["unique_id"].split(".")
        if r["status"] in ("fail", "error") and kind in failed:
            failed[kind].append(rest[-1] if kind == "unit_test" else rest[0])
    return {kind: sorted(names) for kind, names in failed.items()}


def engine() -> str:
    """'core' or 'v2', from `dbt --version`."""
    out = subprocess.run([*dbt_command(), "--version"], capture_output=True, text=True).stdout
    return "v2" if "dbt-oss" in out or "dbt-fusion" in out else "core"


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--check", action="store_true", help="compare with the published record")
    args = ap.parse_args()
    record_path = Path(__file__).resolve().parents[1] / "results" / f"mutants-{engine()}.json"
    record = []
    survivors = 0
    for mutant in MUTANTS:
        with tempfile.TemporaryDirectory() as tmp:
            unit = build(mutant, Path(tmp), data_tests_only=False)["unit_test"]
        with tempfile.TemporaryDirectory() as tmp:
            data = build(mutant, Path(tmp), data_tests_only=True)["test"]
        caught = bool(unit or data)
        survivors += not caught
        print(f"{'caught' if caught else 'SURVIVED':8}  {mutant.name}")
        print(f"          unit tests failing: {', '.join(unit) or 'none'}")
        print(f"          data tests failing: {', '.join(data) or 'none'}")
        record.append(
            {"mutant": mutant.name, "unit_tests_failing": unit, "data_tests_failing": data}
        )
    if survivors:
        sys.exit(f"{survivors} mutant(s) survived")
    text = json.dumps(record, indent=2) + "\n"
    if args.check:
        if not record_path.exists() or record_path.read_text() != text:
            sys.exit(f"{record_path.name} does not match this run")
        print(f"matches {record_path.name}")
    else:
        record_path.write_text(text)


if __name__ == "__main__":
    main()
