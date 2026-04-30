from __future__ import annotations

import csv
import json
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

LEDGER_CSV = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_fixture_level_research_report.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_fixture_level_research_report.json"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(str(x).replace(",", ".").strip())
    except Exception:
        return default


def read_rows() -> list[dict[str, Any]]:
    if not LEDGER_CSV.exists():
        return []

    with LEDGER_CSV.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def is_settled(row: dict[str, Any]) -> bool:
    return str(row.get("settlement_status") or "").upper() == "SETTLED"


def result(row: dict[str, Any]) -> str:
    return str(row.get("result_status") or "").upper()


def fixture_key(row: dict[str, Any]) -> str:
    return str(row.get("fixture_id") or row.get("match") or "UNKNOWN")


def fixture_level_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """
    Official anti-illusion rule:
    keep the first settled research row per fixture.
    This prevents one match from creating 20+ fake independent wins.
    """
    settled = [r for r in rows if is_settled(r)]
    settled.sort(key=lambda r: str(r.get("observed_at_utc") or ""))

    out: dict[str, dict[str, Any]] = {}

    for row in settled:
        key = fixture_key(row)
        if key not in out:
            out[key] = row

    return list(out.values())


def metric_pack(rows: list[dict[str, Any]]) -> dict[str, Any]:
    settled = [r for r in rows if is_settled(r)]
    wins = [r for r in settled if result(r) == "WIN"]
    losses = [r for r in settled if result(r) == "LOSS"]
    pushes = [r for r in settled if result(r) == "PUSH"]

    pnl = sum(fnum(r.get("pnl_unit")) for r in settled)
    n = len(settled)

    return {
        "rows": len(rows),
        "settled": n,
        "wins": len(wins),
        "losses": len(losses),
        "pushes": len(pushes),
        "pnl_unit": round(pnl, 6),
        "roi_unit": round(pnl / n, 6) if n else 0.0,
        "win_rate": round(len(wins) / n, 6) if n else 0.0,
    }


def concentration(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    by_match: dict[str, list[dict[str, Any]]] = defaultdict(list)

    for row in rows:
        if is_settled(row):
            by_match[str(row.get("match") or row.get("fixture_id") or "UNKNOWN")].append(row)

    out = []

    for match, match_rows in by_match.items():
        fixture_rows = fixture_level_rows(match_rows)

        out.append({
            "match": match,
            "snapshot_rows": len(match_rows),
            "snapshot_pnl": round(sum(fnum(r.get("pnl_unit")) for r in match_rows), 6),
            "fixture_rows": len(fixture_rows),
            "fixture_pnl": round(sum(fnum(r.get("pnl_unit")) for r in fixture_rows), 6),
            "first_selection": fixture_rows[0].get("selection") if fixture_rows else "",
            "first_odds": fixture_rows[0].get("odds_decimal") if fixture_rows else "",
            "first_result": fixture_rows[0].get("result_status") if fixture_rows else "",
        })

    out.sort(key=lambda r: abs(fnum(r["snapshot_pnl"])), reverse=True)
    return out


def side_distribution(rows: list[dict[str, Any]]) -> dict[str, Any]:
    total = len(rows)
    under = sum(1 for r in rows if str(r.get("side") or "").upper() == "UNDER")
    over = sum(1 for r in rows if str(r.get("side") or "").upper() == "OVER")

    return {
        "rows_total": total,
        "under_rows": under,
        "over_rows": over,
        "under_share": round(under / total, 6) if total else 0.0,
        "over_share": round(over / total, 6) if total else 0.0,
    }


def write_markdown(payload: dict[str, Any]) -> None:
    snapshot = payload["snapshot"]
    fixture = payload["fixture"]
    side = payload["side_distribution"]
    conc = payload["concentration"]

    lines = [
        "# FQIS Fixture-Level Research Report",
        "",
        "## Executive Summary",
        "",
        f"- Generated at UTC: `{payload['generated_at_utc']}`",
        f"- Ledger rows: **{payload['ledger_rows']}**",
        f"- Snapshot settled: **{snapshot['settled']}**",
        f"- Snapshot PnL: **{snapshot['pnl_unit']}u**",
        f"- Snapshot ROI: **{snapshot['roi_unit']}**",
        f"- Fixture-level settled: **{fixture['settled']}**",
        f"- Fixture-level PnL: **{fixture['pnl_unit']}u**",
        f"- Fixture-level ROI: **{fixture['roi_unit']}**",
        f"- Fixture-level wins/losses/pushes: **{fixture['wins']} / {fixture['losses']} / {fixture['pushes']}**",
        "",
        "> Snapshot PnL is diagnostic. Fixture-level PnL is the conservative economic read.",
        "",
        "## Side Bias",
        "",
        f"- UNDER rows: **{side['under_rows']}**",
        f"- OVER rows: **{side['over_rows']}**",
        f"- UNDER share: **{side['under_share']:.2%}**",
        f"- OVER share: **{side['over_share']:.2%}**",
        "",
        "## Snapshot vs Fixture-Level",
        "",
        "| Level | Rows | Settled | Wins | Losses | Pushes | PnL | ROI | Win rate |",
        "|---|---:|---:|---:|---:|---:|---:|---:|---:|",
        f"| SNAPSHOT | {snapshot['rows']} | {snapshot['settled']} | {snapshot['wins']} | {snapshot['losses']} | {snapshot['pushes']} | {snapshot['pnl_unit']} | {snapshot['roi_unit']:.2%} | {snapshot['win_rate']:.2%} |",
        f"| FIXTURE | {fixture['rows']} | {fixture['settled']} | {fixture['wins']} | {fixture['losses']} | {fixture['pushes']} | {fixture['pnl_unit']} | {fixture['roi_unit']:.2%} | {fixture['win_rate']:.2%} |",
        "",
        "## Concentration By Match",
        "",
        "| Match | Snapshot rows | Snapshot PnL | Fixture rows | Fixture PnL | First selection | First odds | First result |",
        "|---|---:|---:|---:|---:|---|---:|---|",
    ]

    for r in conc[:50]:
        lines.append(
            "| {match} | {snapshot_rows} | {snapshot_pnl} | {fixture_rows} | {fixture_pnl} | {first_selection} | {first_odds} | {first_result} |".format(
                match=str(r["match"]).replace("|", "/"),
                snapshot_rows=r["snapshot_rows"],
                snapshot_pnl=r["snapshot_pnl"],
                fixture_rows=r["fixture_rows"],
                fixture_pnl=r["fixture_pnl"],
                first_selection=r["first_selection"],
                first_odds=r["first_odds"],
                first_result=r["first_result"],
            )
        )

    lines += [
        "",
        "## Verdict",
        "",
    ]

    if fixture["settled"] < 50:
        lines.append("KEEP_RESEARCH. Fixture-level sample is far too small for promotion.")
    elif side["under_share"] >= 0.70:
        lines.append("KEEP_RESEARCH. Under-side concentration is too high and must be audited before any promotion.")
    else:
        lines.append("KEEP_RESEARCH unless daily audit and promotion gates explicitly approve escalation.")

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    rows = read_rows()
    settled_rows = [r for r in rows if is_settled(r)]
    fixture_rows = fixture_level_rows(rows)

    payload = {
        "mode": "FQIS_FIXTURE_LEVEL_RESEARCH_REPORT",
        "generated_at_utc": utc_now(),
        "ledger_csv": str(LEDGER_CSV),
        "ledger_rows": len(rows),
        "snapshot": metric_pack(settled_rows),
        "fixture": metric_pack(fixture_rows),
        "side_distribution": side_distribution(rows),
        "concentration": concentration(rows),
    }

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    write_markdown(payload)

    print(json.dumps({
        "status": "READY",
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
        "snapshot_pnl": payload["snapshot"]["pnl_unit"],
        "fixture_pnl": payload["fixture"]["pnl_unit"],
        "fixture_settled": payload["fixture"]["settled"],
        "fixture_roi": payload["fixture"]["roi_unit"],
    }, indent=2, ensure_ascii=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
