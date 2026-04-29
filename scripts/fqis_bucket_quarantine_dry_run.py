from __future__ import annotations

import csv
import json
from collections import Counter
from pathlib import Path

from fqis_governance_policy import policy_snapshot, read_policy_config, require_dry_run_policy


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
POLICY_AUDIT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_policy_audit.json"
OUT = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_quarantine_dry_run.json"


def main() -> int:
    config = read_policy_config()
    require_dry_run_policy(config, "fqis bucket quarantine dry run")
    policy = json.loads(POLICY_AUDIT.read_text(encoding="utf-8"))
    bucket_actions = {
        bucket: meta.get("action")
        for bucket, meta in (policy.get("buckets") or {}).items()
    }

    rows_total = 0
    would_keep = 0
    would_quarantine = 0
    action_counts = Counter()
    bucket_counts = Counter()

    with LEDGER.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            rows_total += 1
            bucket = row.get("research_bucket") or "UNKNOWN"
            action = bucket_actions.get(bucket, "NO_POLICY")
            action_counts[action] += 1

            if action == "KILL_OR_QUARANTINE_BUCKET":
                would_quarantine += 1
                bucket_counts[bucket] += 1
            else:
                would_keep += 1

    payload = {
        "status": "READY",
        "mode": "DRY_RUN_ONLY_NO_LEDGER_MUTATION",
        "policy": policy_snapshot(config),
        "rows_total": rows_total,
        "would_keep": would_keep,
        "would_quarantine": would_quarantine,
        "would_quarantine_rate": round(would_quarantine / rows_total, 6) if rows_total else 0.0,
        "action_counts": dict(sorted(action_counts.items())),
        "quarantined_bucket_counts": dict(sorted(bucket_counts.items())),
    }

    OUT.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
