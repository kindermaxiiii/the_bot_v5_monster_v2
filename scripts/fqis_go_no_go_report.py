from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
CONFIG = ROOT / "config" / "fqis_bucket_policy.json"
DAILY_AUDIT = ROOT / "data" / "pipeline" / "api_sports" / "audit" / "latest_daily_audit_report.json"
FINAL_PIPELINE = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_final_pipeline_audit.json"
OUT = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_go_no_go_report.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, encoding: str = "utf-8") -> dict[str, Any]:
    try:
        if not path.exists():
            return {}
        return json.loads(path.read_text(encoding=encoding))
    except Exception as exc:
        return {"error": str(exc), "path": str(path)}


def main() -> int:
    config = read_json(CONFIG, encoding="utf-8-sig")
    daily = read_json(DAILY_AUDIT)
    final_pipeline = read_json(FINAL_PIPELINE)

    verdict = daily.get("verdict") or {}
    reasons: list[str] = []

    promotion_allowed = verdict.get("promotion_allowed") is True
    live_staking_allowed = config.get("live_staking_allowed") is True
    simulation_only = not live_staking_allowed

    if config.get("dry_run") is not True:
        reasons.append("CONFIG_DRY_RUN_NOT_TRUE")
    if config.get("enforce_quarantine") is not False:
        reasons.append("CONFIG_ENFORCE_QUARANTINE_NOT_FALSE")
    if config.get("ledger_mutation_allowed") is not False:
        reasons.append("CONFIG_LEDGER_MUTATION_NOT_FALSE")
    if not promotion_allowed:
        reasons.append("PROMOTION_NOT_ALLOWED")
    if not live_staking_allowed:
        reasons.append("LIVE_STAKING_NOT_ALLOWED")
    if final_pipeline.get("invariant_live_staking_disabled") is not True:
        reasons.append("LIVE_STAKING_INVARIANT_NOT_CONFIRMED")
    if final_pipeline.get("live_staking_allowed_true_count", 0) != 0:
        reasons.append("LIVE_STAKING_TRUE_COUNT_NONZERO")

    for flag in verdict.get("flags") or []:
        reasons.append(str(flag))

    go_no_go_state = "LIVE_READY" if promotion_allowed and live_staking_allowed and not reasons else "NO_GO_DRY_RUN_ONLY"
    if go_no_go_state == "LIVE_READY" and not live_staking_allowed:
        go_no_go_state = "NO_GO_DRY_RUN_ONLY"
        reasons.append("LIVE_READY_BLOCKED_BY_LIVE_STAKING_FALSE")

    payload = {
        "status": "READY",
        "generated_at_utc": utc_now(),
        "go_no_go_state": go_no_go_state,
        "reasons": sorted(set(reasons)),
        "promotion_allowed": promotion_allowed,
        "live_staking_allowed": live_staking_allowed,
        "simulation_only": simulation_only,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
