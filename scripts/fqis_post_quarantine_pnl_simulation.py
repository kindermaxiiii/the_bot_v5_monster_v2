from __future__ import annotations

import csv
import json
from pathlib import Path

from fqis_governance_policy import policy_snapshot, read_policy_config, require_dry_run_policy


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
POLICY_AUDIT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_policy_audit.json"
OUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_post_quarantine_pnl_simulation.json"


def f(x):
    try:
        if x is None or x == "":
            return None
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def main() -> int:
    config = read_policy_config()
    require_dry_run_policy(config, "fqis post-quarantine simulation")
    policy = json.loads(POLICY_AUDIT.read_text(encoding="utf-8"))
    bucket_actions = {
        bucket: meta.get("action")
        for bucket, meta in (policy.get("buckets") or {}).items()
    }

    baseline_settled = 0
    baseline_pnl = 0.0

    post_settled = 0
    post_pnl = 0.0

    quarantined_settled = 0
    quarantined_pnl = 0.0

    with LEDGER.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            pnl = f(row.get("pnl_unit"))
            if pnl is None:
                continue

            bucket = row.get("research_bucket") or "UNKNOWN"
            action = bucket_actions.get(bucket, "NO_POLICY")

            baseline_settled += 1
            baseline_pnl += pnl

            if action == "KILL_OR_QUARANTINE_BUCKET":
                quarantined_settled += 1
                quarantined_pnl += pnl
            else:
                post_settled += 1
                post_pnl += pnl

    payload = {
        "status": "READY",
        "mode": "SIMULATION_ONLY_NO_LEDGER_MUTATION",
        "policy": policy_snapshot(config),
        "baseline": {
            "settled": baseline_settled,
            "pnl": round(baseline_pnl, 6),
            "roi": round(baseline_pnl / baseline_settled, 6) if baseline_settled else None,
        },
        "post_quarantine": {
            "settled": post_settled,
            "pnl": round(post_pnl, 6),
            "roi": round(post_pnl / post_settled, 6) if post_settled else None,
        },
        "removed_by_quarantine": {
            "settled": quarantined_settled,
            "pnl": round(quarantined_pnl, 6),
            "roi": round(quarantined_pnl / quarantined_settled, 6) if quarantined_settled else None,
        },
        "delta": {
            "pnl_improvement": round(post_pnl - baseline_pnl, 6),
            "roi_improvement": round(
                (post_pnl / post_settled if post_settled else 0)
                - (baseline_pnl / baseline_settled if baseline_settled else 0),
                6,
            ),
        },
    }

    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
