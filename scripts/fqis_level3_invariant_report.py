from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]

LIVE_DECISIONS = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
FINAL_PIPELINE_AUDIT = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_final_pipeline_audit.json"
RESEARCH_LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def ledger_clean() -> bool:
    if not RESEARCH_LEDGER.exists():
        return True

    forbidden_columns = {
        "final_pipeline",
        "final_pipeline_reason",
        "promoted",
        "promoted_at",
        "promoted_source",
        "promotion_status",
        "live_staking_allowed",
        "level3_live_staking_allowed",
    }

    with RESEARCH_LEDGER.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        columns = set(reader.fieldnames or [])

    return columns.isdisjoint(forbidden_columns)


def main() -> int:
    live = load_json(LIVE_DECISIONS)
    audit = load_json(FINAL_PIPELINE_AUDIT)

    decisions = live.get("decisions", [])
    summary = live.get("summary") or {}

    live_staking_true = 0
    missing_final_pipeline = 0

    for row in decisions:
        payload = row.get("payload") or {}
        if payload.get("live_staking_allowed") is True or payload.get("level3_live_staking_allowed") is True:
            live_staking_true += 1
        if not payload.get("final_pipeline"):
            missing_final_pipeline += 1

    report = {
        "status": "READY",
        "decisions_total": len(decisions),
        "level3_fixtures_inspected": summary.get("level3_fixtures_inspected", 0),
        "level3_trade_ready": summary.get("level3_trade_ready", 0),
        "final_pipeline_counts": audit.get("final_pipeline_counts", {}),
        "gate_state_counts": audit.get("level3_gate_state_counts", {}),
        "live_staking_true_count": live_staking_true,
        "missing_final_pipeline_count": missing_final_pipeline,
        "ledger_isolation_clean": ledger_clean(),
        "invariants": {
            "no_live_staking": live_staking_true == 0,
            "all_decisions_routed": missing_final_pipeline == 0,
            "research_ledger_not_contaminated": ledger_clean(),
        },
    }

    print(json.dumps(report, indent=2, ensure_ascii=False, sort_keys=True))

    ok = all(report["invariants"].values())
    return 0 if ok else 2


if __name__ == "__main__":
    raise SystemExit(main())
