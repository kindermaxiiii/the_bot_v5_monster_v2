from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_alpha_audit.json"
OUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_policy_audit.json"
CONFIG = ROOT / "config" / "fqis_bucket_policy.json"


def main() -> int:
    payload = json.loads(INPUT.read_text(encoding="utf-8"))
    config = json.loads(CONFIG.read_text(encoding="utf-8-sig"))
    buckets = payload.get("buckets") or {}

    min_settled = int(config.get("min_settled", 20))
    kill_roi = float(config.get("kill_roi", -0.20))
    watch_roi = float(config.get("watch_roi", 0.00))

    policies = {}
    for bucket, m in buckets.items():
        settled = int(m.get("settled") or 0)
        roi = m.get("roi")

        if settled < min_settled or roi is None:
            action = "INSUFFICIENT_SAMPLE_KEEP_RESEARCH"
        elif roi <= kill_roi:
            action = "KILL_OR_QUARANTINE_BUCKET"
        elif roi <= watch_roi:
            action = "WATCHLIST_BUCKET"
        else:
            action = "KEEP_RESEARCH_BUCKET"

        policies[bucket] = {
            "action": action,
            "settled": settled,
            "roi": roi,
            "pnl": m.get("pnl"),
            "win_rate": m.get("win_rate"),
        }

    out = {
        "status": "READY",
        "policy": {
            "version": config.get("version"),
            "mode": config.get("mode"),
            "dry_run": config.get("dry_run"),
            "enforce_quarantine": config.get("enforce_quarantine"),
            "ledger_mutation_allowed": config.get("ledger_mutation_allowed"),
            "live_staking_allowed": config.get("live_staking_allowed"),
            "min_settled": min_settled,
            "kill_roi": kill_roi,
            "watch_roi": watch_roi,
        },
        "buckets": policies,
    }

    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(out, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

