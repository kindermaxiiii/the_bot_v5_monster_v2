from __future__ import annotations

import csv
import json
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
CONFIG = ROOT / "config" / "fqis_bucket_policy.json"
POLICY_AUDIT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_policy_audit.json"
OUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_post_quarantine_pnl_simulation.json"


def f(x):
    try:
        if x is None or x == "":
            return None
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def load_policy_config() -> dict:
    config = json.loads(CONFIG.read_text(encoding="utf-8-sig"))
    if config.get("dry_run") is not True:
        raise RuntimeError("fqis post-quarantine simulation requires dry_run=true")
    if config.get("enforce_quarantine") is not False:
        raise RuntimeError("fqis post-quarantine simulation cannot enforce quarantine")
    if config.get("ledger_mutation_allowed") is not False:
        raise RuntimeError("fqis post-quarantine simulation cannot mutate the ledger")
    if config.get("live_staking_allowed") is not False:
        raise RuntimeError("fqis post-quarantine simulation cannot allow live staking")
    return config


def main() -> int:
    config = load_policy_config()
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
        "policy_config": {
            "version": config.get("version"),
            "mode": config.get("mode"),
            "dry_run": config.get("dry_run"),
            "enforce_quarantine": config.get("enforce_quarantine"),
            "ledger_mutation_allowed": config.get("ledger_mutation_allowed"),
            "live_staking_allowed": config.get("live_staking_allowed"),
        },
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
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
