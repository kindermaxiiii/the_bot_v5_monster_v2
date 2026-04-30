from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from fqis_governance_policy import policy_safety_reasons, policy_snapshot, read_policy_config


ROOT = Path(__file__).resolve().parents[1]
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
    config = read_policy_config()
    daily = read_json(DAILY_AUDIT)
    final_pipeline = read_json(FINAL_PIPELINE)

    verdict = daily.get("verdict") or {}
    reasons: list[str] = []

    promotion_allowed = verdict.get("promotion_allowed") is True
    live_staking_allowed = config.get("live_staking_allowed") is True
    simulation_only = not live_staking_allowed

    reasons.extend(policy_safety_reasons(config))
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

    go_no_go_state = "NO_GO_DRY_RUN_ONLY"

    payload = {
        "status": "READY",
        "generated_at_utc": utc_now(),
        "go_no_go_state": go_no_go_state,
        "policy": policy_snapshot(config),
        "reasons": sorted(set(reasons)),
        "promotion_allowed": promotion_allowed,
        "live_staking_allowed": live_staking_allowed,
        "simulation_only": simulation_only,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
