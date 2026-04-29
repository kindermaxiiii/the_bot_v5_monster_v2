import json
from pathlib import Path


def test_latest_live_decisions_never_enable_live_staking():
    path = Path("data/pipeline/api_sports/decision_bridge_live/latest_live_decisions.json")

    if not path.exists():
        return

    payload = json.loads(path.read_text(encoding="utf-8"))
    decisions = payload.get("decisions", [])

    violations = []
    for row in decisions:
        p = row.get("payload") or {}
        if p.get("live_staking_allowed") is True:
            violations.append(row.get("decision_key") or p.get("fixture_id"))
        if p.get("level3_live_staking_allowed") is True:
            violations.append(row.get("decision_key") or p.get("fixture_id"))

    assert violations == []


def test_final_pipeline_audit_confirms_live_staking_disabled():
    path = Path("data/pipeline/api_sports/decision_bridge_live/latest_final_pipeline_audit.json")

    if not path.exists():
        return

    payload = json.loads(path.read_text(encoding="utf-8"))

    assert payload.get("live_staking_allowed_true_count") == 0
    assert payload.get("invariant_live_staking_disabled") is True
