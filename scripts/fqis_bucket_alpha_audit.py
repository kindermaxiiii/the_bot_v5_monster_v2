from __future__ import annotations

import csv
import json
from collections import defaultdict
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_bucket_alpha_audit.json"


def f(x):
    try:
        if x is None or x == "":
            return None
        return float(str(x).replace(",", "."))
    except Exception:
        return None


def main() -> int:
    buckets = defaultdict(lambda: {
        "rows": 0,
        "settled": 0,
        "wins": 0,
        "losses": 0,
        "pushes": 0,
        "pnl": 0.0,
        "near_close_clv_sum": 0.0,
        "near_close_clv_n": 0,
    })

    with LEDGER.open("r", encoding="utf-8-sig", newline="") as handle:
        for row in csv.DictReader(handle):
            bucket = row.get("research_bucket") or "UNKNOWN"
            b = buckets[bucket]
            b["rows"] += 1

            pnl = f(row.get("pnl_unit"))
            status = str(row.get("result_status") or "").upper()
            if pnl is not None:
                b["settled"] += 1
                b["pnl"] += pnl
                if status == "WIN":
                    b["wins"] += 1
                elif status == "LOSS":
                    b["losses"] += 1
                elif status == "PUSH":
                    b["pushes"] += 1

            clv = f(row.get("near_close_clv"))
            if clv is not None:
                b["near_close_clv_sum"] += clv
                b["near_close_clv_n"] += 1

    out = {}
    for bucket, b in buckets.items():
        settled = b["settled"]
        clv_n = b["near_close_clv_n"]
        out[bucket] = {
            "rows": b["rows"],
            "settled": settled,
            "wins": b["wins"],
            "losses": b["losses"],
            "pushes": b["pushes"],
            "pnl": round(b["pnl"], 6),
            "roi": round(b["pnl"] / settled, 6) if settled else None,
            "win_rate": round(b["wins"] / settled, 6) if settled else None,
            "avg_near_close_clv": round(b["near_close_clv_sum"] / clv_n, 6) if clv_n else None,
            "near_close_clv_n": clv_n,
        }

    payload = {
        "status": "READY",
        "buckets": dict(sorted(out.items(), key=lambda kv: (kv[1]["roi"] is None, kv[1]["roi"] or -999))),
    }

    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")
    print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
