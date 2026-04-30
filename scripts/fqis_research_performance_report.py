from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

LEDGER_CSV = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_performance_report.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_performance_report.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(str(x).replace(",", ".").strip())
    except Exception:
        return default


def read_rows(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def is_settled(row: dict[str, Any]) -> bool:
    return str(row.get("settlement_status") or "").upper() == "SETTLED"


def row_pnl(row: dict[str, Any]) -> float:
    return fnum(row.get("pnl_unit"), 0.0)


def row_result(row: dict[str, Any]) -> str:
    return str(row.get("result_status") or "").upper()


def first_by_key(rows: list[dict[str, Any]], key_name: str) -> list[dict[str, Any]]:
    out = {}
    for r in sorted(rows, key=lambda x: str(x.get("observed_at_utc") or "")):
        key = str(r.get(key_name) or "")
        if not key:
            key = "|".join([
                str(r.get("fixture_id") or ""),
                str(r.get("market_key") or ""),
                str(r.get("side") or ""),
                str(r.get("line") or ""),
                str(r.get("selection") or ""),
            ])

        if key not in out:
            out[key] = r

    return list(out.values())


def match_level_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Anti-illusion rule:
    One economic thesis per match/market/side/line.
    We keep the first observed row, not the best row.
    """
    out = {}

    for r in sorted(rows, key=lambda x: str(x.get("observed_at_utc") or "")):
        key = "|".join([
            str(r.get("fixture_id") or r.get("match") or ""),
            str(r.get("market_key") or ""),
            str(r.get("side") or ""),
            str(r.get("line") or ""),
        ])

        if key not in out:
            out[key] = r

    return list(out.values())


def metric_pack(rows: list[dict[str, Any]]) -> dict[str, Any]:
    settled = [r for r in rows if is_settled(r)]
    wins = [r for r in settled if row_result(r) == "WIN"]
    losses = [r for r in settled if row_result(r) == "LOSS"]
    pushes = [r for r in settled if row_result(r) == "PUSH"]

    pnl = sum(row_pnl(r) for r in settled)
    n = len(settled)

    avg_edge = sum(fnum(r.get("edge")) for r in rows) / len(rows) if rows else 0.0
    avg_ev = sum(fnum(r.get("expected_value")) for r in rows) / len(rows) if rows else 0.0

    avg_clv = None
    clv_rows = [r for r in rows if str(r.get("clv_decimal") or "").strip() != ""]
    if clv_rows:
        avg_clv = sum(fnum(r.get("clv_decimal")) for r in clv_rows) / len(clv_rows)

    return {
        "rows": len(rows),
        "settled": n,
        "wins": len(wins),
        "losses": len(losses),
        "pushes": len(pushes),
        "win_rate": round(len(wins) / n, 6) if n else 0.0,
        "pnl_unit": round(pnl, 6),
        "roi_unit": round(pnl / n, 6) if n else 0.0,
        "avg_edge": round(avg_edge, 6),
        "avg_ev": round(avg_ev, 6),
        "avg_clv_decimal": None if avg_clv is None else round(avg_clv, 6),
    }


def bucket_report(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_bucket = defaultdict(list)

    for r in rows:
        by_bucket[str(r.get("research_bucket") or "UNKNOWN")].append(r)

    report = []

    for bucket, bucket_rows in by_bucket.items():
        signal_rows = first_by_key(bucket_rows, "signal_key")
        match_rows = match_level_rows(bucket_rows)

        item = {
            "bucket": bucket,
            "snapshot": metric_pack(bucket_rows),
            "signal": metric_pack(signal_rows),
            "match": metric_pack(match_rows),
            "recommendation": recommend_bucket(bucket, signal_rows, match_rows),
        }
        report.append(item)

    report.sort(
        key=lambda x: (
            x["signal"]["settled"],
            x["signal"]["roi_unit"],
            x["signal"]["pnl_unit"],
        ),
        reverse=True,
    )

    return report


def recommend_bucket(bucket: str, signal_rows: list[dict[str, Any]], match_rows: list[dict[str, Any]]) -> str:
    sig = metric_pack(signal_rows)
    mat = metric_pack(match_rows)

    signals = sig["settled"]
    matches = mat["settled"]
    roi = sig["roi_unit"]
    clv = sig["avg_clv_decimal"]

    bucket_upper = str(bucket or "").upper()
    toxic_under = (
        "UNDER_0_5" in bucket_upper
        or "UNDER_1_5" in bucket_upper
    )

    if signals < 100 or matches < 50:
        return "KEEP_RESEARCH_INSUFFICIENT_SAMPLE"

    if toxic_under and (signals < 2000 or matches < 1000):
        return "KEEP_QUARANTINE_LOW_UNDER_SAMPLE_TOO_SMALL"

    if clv is None:
        return "KEEP_RESEARCH_NO_CLV"

    if clv <= 0:
        return "KEEP_RESEARCH_CLV_NOT_POSITIVE"

    if roi < 0:
        return "KEEP_RESEARCH_NEGATIVE_ROI"

    if signals >= 1000 and matches >= 500 and clv > 0 and roi >= 0:
        return "PROMOTE_TO_SHADOW_REVIEW"

    if signals >= 500 and matches >= 250 and clv > 0:
        return "WATCHLIST_REVIEW"

    return "KEEP_RESEARCH"


def concentration_report(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_match = defaultdict(list)

    for r in rows:
        by_match[str(r.get("match") or r.get("fixture_id") or "UNKNOWN")].append(r)

    out = []

    for match, raw_rows in by_match.items():
        raw_settled = [r for r in raw_rows if is_settled(r)]
        raw_pnl = sum(row_pnl(r) for r in raw_settled)

        economic_rows = match_level_rows(raw_rows)
        economic_settled = [r for r in economic_rows if is_settled(r)]
        economic_pnl = sum(row_pnl(r) for r in economic_settled)

        out.append({
            "match": match,
            "raw_rows": len(raw_rows),
            "raw_settled": len(raw_settled),
            "raw_pnl_unit": round(raw_pnl, 6),
            "economic_rows": len(economic_rows),
            "economic_settled": len(economic_settled),
            "economic_pnl_unit": round(economic_pnl, 6),
        })

    out.sort(
        key=lambda x: (
            abs(x["economic_pnl_unit"]),
            abs(x["raw_pnl_unit"]),
        ),
        reverse=True,
    )
    return out


def write_markdown(payload: dict[str, Any]) -> None:
    summary = payload["summary"]
    buckets = payload["buckets"]
    concentration = payload["concentration"]

    lines = []
    lines.append("# FQIS Research Performance Report")
    lines.append("")
    lines.append("## Executive Summary")
    lines.append("")
    lines.append(f"- Generated at UTC: `{payload['generated_at_utc']}`")
    lines.append(f"- Raw snapshot rows: **{summary['snapshot']['rows']}**")
    lines.append(f"- Signal-level rows: **{summary['signal']['rows']}**")
    lines.append(f"- Match-level rows: **{summary['match']['rows']}**")
    lines.append(f"- Snapshot ROI: **{summary['snapshot']['roi_unit']}**")
    lines.append(f"- Signal ROI: **{summary['signal']['roi_unit']}**")
    lines.append(f"- Match ROI: **{summary['match']['roi_unit']}**")
    lines.append(f"- Signal PnL: **{summary['signal']['pnl_unit']}u**")
    lines.append("")
    lines.append("> Research only. Snapshot-level is diagnostic. Signal-level is the primary economic unit. Match-level controls duplication.")
    lines.append("")

    lines.append("## Global Performance")
    lines.append("")
    lines.append("| Level | Rows | Settled | Wins | Losses | Pushes | Win rate | PnL | ROI | Avg edge | Avg EV | Avg CLV |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|---:|")
    for level in ["snapshot", "signal", "match"]:
        m = summary[level]
        clv = "" if m["avg_clv_decimal"] is None else f"{m['avg_clv_decimal']:.6f}"
        lines.append(
            f"| {level.upper()} | {m['rows']} | {m['settled']} | {m['wins']} | {m['losses']} | {m['pushes']} | {m['win_rate']:.2%} | {m['pnl_unit']} | {m['roi_unit']:.2%} | {m['avg_edge']:.2%} | {m['avg_ev']:.2%} | {clv} |"
        )

    lines.append("")
    lines.append("## Bucket Performance")
    lines.append("")
    lines.append("| Bucket | Snapshot rows | Signal settled | Match settled | Signal PnL | Signal ROI | Avg EV | Recommendation |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|---|")
    for b in buckets:
        lines.append(
            "| {bucket} | {snap_rows} | {sig_settled} | {match_settled} | {pnl} | {roi:.2%} | {ev:.2%} | {rec} |".format(
                bucket=b["bucket"],
                snap_rows=b["snapshot"]["rows"],
                sig_settled=b["signal"]["settled"],
                match_settled=b["match"]["settled"],
                pnl=b["signal"]["pnl_unit"],
                roi=b["signal"]["roi_unit"],
                ev=b["signal"]["avg_ev"],
                rec=b["recommendation"],
            )
        )

    lines.append("")
    lines.append("## Concentration By Match")
    lines.append("")
    lines.append("| Match | Raw rows | Raw settled | Raw PnL | Economic rows | Economic settled | Economic PnL |")
    lines.append("|---|---:|---:|---:|---:|---:|---:|")
    for r in concentration[:30]:
        safe_match = str(r["match"]).replace("|", "/")
        lines.append(
            f"| {safe_match} | {r['raw_rows']} | {r['raw_settled']} | {r['raw_pnl_unit']} | {r['economic_rows']} | {r['economic_settled']} | {r['economic_pnl_unit']} |"
        )

    lines.append("")
    lines.append("## Verdict")
    lines.append("")

    if summary["signal"]["settled"] < 100:
        lines.append("KEEP_RESEARCH. Sample far too small. No promotion possible.")
    elif any(b["recommendation"].startswith("PROMOTE") for b in buckets):
        lines.append("Promotion review possible for at least one bucket, subject to CLV, calibration, and committee validation.")
    else:
        lines.append("KEEP_RESEARCH. No bucket satisfies promotion requirements.")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    rows = read_rows(LEDGER_CSV)

    signal_rows = first_by_key(rows, "signal_key")
    match_rows = match_level_rows(rows)

    payload = {
        "mode": "FQIS_RESEARCH_PERFORMANCE_REPORT",
        "generated_at_utc": utc_now(),
        "ledger_csv": str(LEDGER_CSV),
        "summary": {
            "snapshot": metric_pack(rows),
            "signal": metric_pack(signal_rows),
            "match": metric_pack(match_rows),
        },
        "buckets": bucket_report(rows),
        "concentration": concentration_report(rows),
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    write_markdown(payload)

    print(json.dumps({
        "status": "READY",
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
        "snapshot_rows": payload["summary"]["snapshot"]["rows"],
        "signal_rows": payload["summary"]["signal"]["rows"],
        "match_rows": payload["summary"]["match"]["rows"],
        "signal_roi": payload["summary"]["signal"]["roi_unit"],
        "signal_pnl": payload["summary"]["signal"]["pnl_unit"],
    }, indent=2, ensure_ascii=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
