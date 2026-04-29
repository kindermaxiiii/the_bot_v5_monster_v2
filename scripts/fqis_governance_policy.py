from __future__ import annotations

import json
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "fqis_bucket_policy.json"


def read_policy_config(path: Path = CONFIG) -> dict[str, Any]:
    return json.loads(path.read_text(encoding="utf-8-sig"))


def policy_snapshot(config: dict[str, Any]) -> dict[str, Any]:
    return {
        "version": config.get("version"),
        "mode": config.get("mode"),
        "dry_run": config.get("dry_run"),
        "enforce_quarantine": config.get("enforce_quarantine"),
        "ledger_mutation_allowed": config.get("ledger_mutation_allowed"),
        "live_staking_allowed": config.get("live_staking_allowed"),
    }


def policy_safety_reasons(config: dict[str, Any]) -> list[str]:
    reasons: list[str] = []
    if config.get("dry_run") is not True:
        reasons.append("CONFIG_DRY_RUN_NOT_TRUE")
    if config.get("enforce_quarantine") is not False:
        reasons.append("CONFIG_ENFORCE_QUARANTINE_NOT_FALSE")
    if config.get("ledger_mutation_allowed") is not False:
        reasons.append("CONFIG_LEDGER_MUTATION_NOT_FALSE")
    if config.get("live_staking_allowed") is not False:
        reasons.append("CONFIG_LIVE_STAKING_NOT_FALSE")
    return reasons


def require_dry_run_policy(config: dict[str, Any], context: str) -> None:
    reasons = policy_safety_reasons(config)
    if reasons:
        raise RuntimeError(f"{context} requires dry-run policy: {', '.join(reasons)}")
