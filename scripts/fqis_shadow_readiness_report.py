from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
ORCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator"
RESEARCH_DIR = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger"

FULL_CYCLE = ORCH_DIR / "latest_full_cycle_report.json"
GO_NO_GO = ORCH_DIR / "latest_go_no_go_report.json"
POST_QUARANTINE = RESEARCH_DIR / "latest_post_quarantine_pnl_simulation.json"
BUCKET_POLICY = RESEARCH_DIR / "latest_bucket_policy_audit.json"
BUCKET_QUARANTINE = RESEARCH_DIR / "latest_bucket_quarantine_dry_run.json"
CLV_HORIZON = RESEARCH_DIR / "latest_clv_horizon_audit.json"
RESEARCH_PERFORMANCE = RESEARCH_DIR / "latest_research_performance_report.json"
OUT = ORCH_DIR / "latest_shadow_readiness_report.json"

ALLOWED_GO_NO_GO_STATES = {"NO_GO_DRY_RUN_ONLY", "PAPER_READY", "SHADOW_READY"}


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def read_json(path: Path, label: str) -> tuple[dict[str, Any], list[str]]:
    if not path.exists():
        return {}, [f"MISSING_{label}"]
    try:
        return json.loads(path.read_text(encoding="utf-8")), []
    except Exception as exc:
        return {}, [f"INVALID_{label}: {exc}"]


def fnum(value: Any, default: float | None = None) -> float | None:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def bucket_policy_summary(bucket_policy: dict[str, Any]) -> dict[str, int]:
    counts = {
        "killed_bucket_count": 0,
        "kept_bucket_count": 0,
        "watchlist_bucket_count": 0,
        "insufficient_sample_bucket_count": 0,
    }
    for meta in (bucket_policy.get("buckets") or {}).values():
        action = meta.get("action")
        if action == "KILL_OR_QUARANTINE_BUCKET":
            counts["killed_bucket_count"] += 1
        elif action == "KEEP_RESEARCH_BUCKET":
            counts["kept_bucket_count"] += 1
        elif action == "WATCHLIST_BUCKET":
            counts["watchlist_bucket_count"] += 1
        elif action == "INSUFFICIENT_SAMPLE_KEEP_RESEARCH":
            counts["insufficient_sample_bucket_count"] += 1
    return counts


def main() -> int:
    inputs = {
        "FULL_CYCLE": FULL_CYCLE,
        "GO_NO_GO": GO_NO_GO,
        "POST_QUARANTINE": POST_QUARANTINE,
        "BUCKET_POLICY": BUCKET_POLICY,
        "BUCKET_QUARANTINE": BUCKET_QUARANTINE,
        "CLV_HORIZON": CLV_HORIZON,
        "RESEARCH_PERFORMANCE": RESEARCH_PERFORMANCE,
    }

    loaded: dict[str, dict[str, Any]] = {}
    reasons: list[str] = []
    for label, path in inputs.items():
        payload, read_reasons = read_json(path, label)
        loaded[label] = payload
        reasons.extend(read_reasons)

    full_cycle = loaded["FULL_CYCLE"]
    go_no_go = loaded["GO_NO_GO"]
    post_quarantine = loaded["POST_QUARANTINE"]
    bucket_policy = loaded["BUCKET_POLICY"]
    bucket_quarantine = loaded["BUCKET_QUARANTINE"]
    clv_horizon = loaded["CLV_HORIZON"]

    full_status = full_cycle.get("status")
    go_no_go_state = go_no_go.get("go_no_go_state")
    live_staking_allowed = go_no_go.get("live_staking_allowed")
    simulation_only = go_no_go.get("simulation_only")
    ledger_preserved = (full_cycle.get("invariants") or {}).get("research_candidates_ledger_preserved")

    baseline = post_quarantine.get("baseline") or {}
    post = post_quarantine.get("post_quarantine") or {}
    baseline_pnl = fnum(baseline.get("pnl"))
    baseline_roi = fnum(baseline.get("roi"))
    post_pnl = fnum(post.get("pnl"))
    post_roi = fnum(post.get("roi"))

    near_close = (((clv_horizon.get("summary") or {}).get("horizons") or {}).get("near_close") or {})
    clv_near_close = {
        "avg": fnum(near_close.get("avg")),
        "positive_rate": fnum(near_close.get("positive_rate")),
    }

    if full_status != "READY":
        reasons.append("FULL_CYCLE_NOT_READY")
    if go_no_go_state not in ALLOWED_GO_NO_GO_STATES:
        reasons.append("GO_NO_GO_STATE_NOT_SHADOW_COMPATIBLE")
    if live_staking_allowed is not False:
        reasons.append("LIVE_STAKING_ALLOWED_NOT_FALSE")
    if simulation_only is not True:
        reasons.append("SIMULATION_ONLY_NOT_TRUE")
    if ledger_preserved is not True:
        reasons.append("RESEARCH_CANDIDATES_LEDGER_NOT_PRESERVED")
    if post_pnl is None or post_pnl <= 0:
        reasons.append("POST_QUARANTINE_PNL_NOT_POSITIVE")
    if post_roi is None or post_roi <= 0:
        reasons.append("POST_QUARANTINE_ROI_NOT_POSITIVE")
    if bucket_quarantine.get("mode") != "DRY_RUN_ONLY_NO_LEDGER_MUTATION":
        reasons.append("BUCKET_QUARANTINE_NOT_DRY_RUN_NO_MUTATION")

    shadow_state = "SHADOW_READY" if not reasons else "SHADOW_BLOCKED"
    status = "READY" if shadow_state == "SHADOW_READY" else "BLOCKED"

    payload = {
        "status": status,
        "generated_at_utc": utc_now(),
        "shadow_state": shadow_state,
        "reasons": sorted(set(reasons)),
        "baseline": {
            "pnl": baseline_pnl,
            "roi": baseline_roi,
        },
        "post_quarantine": {
            "pnl": post_pnl,
            "roi": post_roi,
        },
        "clv_near_close": clv_near_close,
        "bucket_policy_summary": bucket_policy_summary(bucket_policy),
        "can_publish_to_discord_paper_only": shadow_state == "SHADOW_READY",
        "can_execute_real_bets": False,
        "can_mutate_ledger": False,
        "can_enable_live_staking": False,
    }

    OUT.parent.mkdir(parents=True, exist_ok=True)
    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
