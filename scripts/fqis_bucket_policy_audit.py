from __future__ import annotations

import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_alpha_audit.json"
OUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_policy_audit.json"

MIN_SETTLED = 20
KILL_ROI = -0.20
WATCH_ROI = 0.00


def main() -> int:
    payload = json.loads(INPUT.read_text(encoding="utf-8"))
    buckets = payload.get("buckets") or {}

    policies = {}
    for bucket, m in buckets.items():
        settled = int(m.get("settled") or 0)
        roi = m.get("roi")

        if settled < MIN_SETTLED or roi is None:
            action = "INSUFFICIENT_SAMPLE_KEEP_RESEARCH"
        elif roi <= KILL_ROI:
            action = "KILL_OR_QUARANTINE_BUCKET"
        elif roi <= WATCH_ROI:
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
            "min_settled": MIN_SETTLED,
            "kill_roi": KILL_ROI,
            "watch_roi": WATCH_ROI,
        },
        "buckets": policies,
    }

    OUT.write_text(json.dumps(out, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(out, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
