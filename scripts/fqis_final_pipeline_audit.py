from __future__ import annotations

import json
from collections import Counter
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
INPUT = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_live_decisions.json"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live" / "latest_final_pipeline_audit.json"


def main() -> int:
    payload = json.loads(INPUT.read_text(encoding="utf-8"))
    rows = payload.get("decisions", [])

    pipelines = Counter()
    gate_states = Counter()
    staking_true = 0

    for row in rows:
        p = row.get("payload") or {}
        pipelines[str(p.get("final_pipeline") or "MISSING")] += 1
        gate_states[str(p.get("level3_gate_state") or "MISSING")] += 1
        if p.get("live_staking_allowed") is True or p.get("level3_live_staking_allowed") is True:
            staking_true += 1

    audit = {
        "source": str(INPUT),
        "decisions_total": len(rows),
        "final_pipeline_counts": dict(sorted(pipelines.items())),
        "level3_gate_state_counts": dict(sorted(gate_states.items())),
        "live_staking_allowed_true_count": staking_true,
        "invariant_live_staking_disabled": staking_true == 0,
    }

    OUT_JSON.write_text(json.dumps(audit, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(audit, indent=2, ensure_ascii=False, sort_keys=True))
    return 0 if staking_true == 0 else 2


if __name__ == "__main__":
    raise SystemExit(main())
