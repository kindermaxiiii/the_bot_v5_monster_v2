import csv
from pathlib import Path


FORBIDDEN_COLUMNS = {
    "final_pipeline",
    "final_pipeline_reason",
    "promoted",
    "promoted_at",
    "promoted_source",
    "promotion_status",
    "live_staking_allowed",
    "level3_live_staking_allowed",
}


def test_research_candidates_ledger_has_no_promoted_or_runtime_pipeline_columns():
    path = Path("data/pipeline/api_sports/research_ledger/research_candidates_ledger.csv")

    if not path.exists():
        return

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])

    assert columns.isdisjoint(FORBIDDEN_COLUMNS)


def test_research_candidates_ledger_rows_do_not_mark_promoted():
    path = Path("data/pipeline/api_sports/research_ledger/research_candidates_ledger.csv")

    if not path.exists():
        return

    forbidden_values = {"promoted", "production", "live", "real_stake", "stake_live"}

    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            lowered = {str(v).strip().lower() for v in row.values() if v is not None}
            assert lowered.isdisjoint(forbidden_values)
