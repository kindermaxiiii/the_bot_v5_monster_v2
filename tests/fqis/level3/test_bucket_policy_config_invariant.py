import json
from pathlib import Path


def test_bucket_policy_config_is_safe_dry_run_only():
    path = Path("config/fqis_bucket_policy.json")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))

    assert payload["dry_run"] is True
    assert payload["enforce_quarantine"] is False
    assert payload["ledger_mutation_allowed"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["mode"] == "DRY_RUN_ONLY"


def test_bucket_policy_thresholds_are_institutional_safe():
    path = Path("config/fqis_bucket_policy.json")
    payload = json.loads(path.read_text(encoding="utf-8-sig"))

    assert int(payload["min_settled"]) >= 20
    assert float(payload["kill_roi"]) <= -0.20
    assert float(payload["watch_roi"]) <= 0.0
