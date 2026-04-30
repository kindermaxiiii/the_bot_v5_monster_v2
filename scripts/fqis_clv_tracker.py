from __future__ import annotations

import csv
import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

LEDGER_CSV = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
RUNS_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_report.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_report.json"

CLV_FIELDS = [
    "closing_odds",
    "clv_decimal",
    "clv_status",
    "near_close_observed_at_utc",
    "near_close_source_cycle_dir",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(x: Any, default: float = 0.0) -> float:
    try:
        if x is None or x == "":
            return default
        return float(str(x).replace(",", ".").strip())
    except Exception:
        return default


def parse_dt(value: Any) -> datetime | None:
    try:
        if not value:
            return None
        text = str(value).replace("Z", "+00:00")
        return datetime.fromisoformat(text)
    except Exception:
        return None


def read_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), list(reader.fieldnames or [])


def write_rows(path: Path, rows: list[dict[str, Any]], original_fields: list[str]) -> None:
    fields = list(original_fields)

    for field in CLV_FIELDS:
        if field not in fields:
            fields.append(field)

    for row in rows:
        for field in row:
            if field not in fields:
                fields.append(field)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fields)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fields})


def observation_key_from_decision(d: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(d.get("fixture_id") or ""),
        str(d.get("market_key") or ""),
        str(d.get("side") or "").upper(),
        str(d.get("line") or ""),
        str(d.get("selection") or ""),
    )


def observation_key_from_ledger(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("fixture_id") or ""),
        str(row.get("market_key") or ""),
        str(row.get("side") or "").upper(),
        str(row.get("line") or ""),
        str(row.get("selection") or ""),
    )


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def collect_observations() -> dict[tuple[str, str, str, str, str], list[dict[str, Any]]]:
    observations: dict[tuple[str, str, str, str, str], list[dict[str, Any]]] = {}

    run_dirs = sorted(
        [p for p in RUNS_DIR.glob("run_*") if p.is_dir()],
        key=lambda p: p.name,
    )

    for run_dir in run_dirs:
        payload = load_json(run_dir / "live_decisions.json")
        if not payload:
            continue

        observed_at = payload.get("generated_at_utc") or ""
        cycle_dir = payload.get("cycle_dir") or str(run_dir)

        for decision in payload.get("decisions") or []:
            key = observation_key_from_decision(decision)
            odds = fnum(decision.get("odds_decimal"), 0.0)

            if odds <= 1.0:
                continue

            observations.setdefault(key, []).append({
                "observed_at_utc": observed_at,
                "cycle_dir": cycle_dir,
                "odds_decimal": odds,
                "price_state": decision.get("price_state"),
                "real_status": decision.get("real_status"),
            })

    for key in observations:
        observations[key].sort(key=lambda x: str(x.get("observed_at_utc") or ""))

    return observations


def compute_clv(entry_odds: float, close_odds: float) -> float:
    """
    Positive CLV means we took a better price than the later/near-close price.
    Example:
    entry 2.25, close 2.00 => +12.5%
    entry 2.00, close 2.25 => -11.11%
    """
    if entry_odds <= 1.0 or close_odds <= 1.0:
        return 0.0

    return (entry_odds / close_odds) - 1.0


def update_row_clv(row: dict[str, Any], observations: dict[tuple[str, str, str, str, str], list[dict[str, Any]]]) -> dict[str, Any]:
    row = dict(row)

    key = observation_key_from_ledger(row)
    obs = observations.get(key) or []

    entry_time = parse_dt(row.get("observed_at_utc"))
    entry_odds = fnum(row.get("odds_decimal"), 0.0)

    if not obs:
        row["clv_status"] = "NO_OBSERVATION"
        row.setdefault("closing_odds", "")
        row.setdefault("clv_decimal", "")
        row.setdefault("near_close_observed_at_utc", "")
        row.setdefault("near_close_source_cycle_dir", "")
        return row

    future_obs = []

    for item in obs:
        obs_time = parse_dt(item.get("observed_at_utc"))
        if entry_time is None or obs_time is None:
            continue
        if obs_time >= entry_time:
            future_obs.append(item)

    if not future_obs:
        row["clv_status"] = "NO_FUTURE_OBSERVATION"
        row.setdefault("closing_odds", "")
        row.setdefault("clv_decimal", "")
        row.setdefault("near_close_observed_at_utc", "")
        row.setdefault("near_close_source_cycle_dir", "")
        return row

    near_close = future_obs[-1]
    close_odds = fnum(near_close.get("odds_decimal"), 0.0)

    if entry_odds <= 1.0 or close_odds <= 1.0:
        row["clv_status"] = "INVALID_ODDS"
        return row

    clv = compute_clv(entry_odds, close_odds)

    row["closing_odds"] = round(close_odds, 6)
    row["clv_decimal"] = round(clv, 6)
    row["clv_status"] = "TRACKED"
    row["near_close_observed_at_utc"] = near_close.get("observed_at_utc") or ""
    row["near_close_source_cycle_dir"] = near_close.get("cycle_dir") or ""

    return row


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    tracked = [r for r in rows if str(r.get("clv_status") or "") == "TRACKED"]
    positive = [r for r in tracked if fnum(r.get("clv_decimal")) > 0]
    negative = [r for r in tracked if fnum(r.get("clv_decimal")) < 0]
    flat = [r for r in tracked if fnum(r.get("clv_decimal")) == 0]

    avg_clv = sum(fnum(r.get("clv_decimal")) for r in tracked) / len(tracked) if tracked else 0.0

    return {
        "rows_total": len(rows),
        "tracked": len(tracked),
        "untracked": len(rows) - len(tracked),
        "positive_clv": len(positive),
        "negative_clv": len(negative),
        "flat_clv": len(flat),
        "avg_clv_decimal": round(avg_clv, 6),
        "positive_clv_rate": round(len(positive) / len(tracked), 6) if tracked else 0.0,
    }


def write_markdown(rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    lines = [
        "# FQIS CLV Tracker",
        "",
        "## Summary",
        "",
        f"- Rows total: **{summary['rows_total']}**",
        f"- Tracked: **{summary['tracked']}**",
        f"- Untracked: **{summary['untracked']}**",
        f"- Positive CLV: **{summary['positive_clv']}**",
        f"- Negative CLV: **{summary['negative_clv']}**",
        f"- Flat CLV: **{summary['flat_clv']}**",
        f"- Avg CLV decimal: **{summary['avg_clv_decimal']}**",
        f"- Positive CLV rate: **{summary['positive_clv_rate']}**",
        "",
        "> Positive CLV means entry odds were better than the later/near-close observed odds.",
        "",
        "## Rows",
        "",
    ]

    if not rows:
        lines.append("No rows.")
    else:
        lines.append("| Status | Match | Score | Min | Selection | Entry odds | Near-close odds | CLV | Result | PnL | Bucket |")
        lines.append("|---|---|---:|---:|---|---:|---:|---:|---|---:|---|")

        for r in rows:
            close = r.get("closing_odds", "")
            close_text = "" if close == "" else f"{fnum(close):.3f}"
            clv = r.get("clv_decimal", "")
            clv_text = "" if clv == "" else f"{fnum(clv):.2%}"

            lines.append(
                "| {status} | {match} | {score} | {minute} | {selection} | {entry:.3f} | {close} | {clv} | {result} | {pnl} | {bucket} |".format(
                    status=r.get("clv_status", ""),
                    match=str(r.get("match", "")).replace("|", "/"),
                    score=r.get("score", ""),
                    minute=r.get("minute", ""),
                    selection=r.get("selection", ""),
                    entry=fnum(r.get("odds_decimal")),
                    close=close_text,
                    clv=clv_text,
                    result=r.get("result_status", ""),
                    pnl=r.get("pnl_unit", ""),
                    bucket=r.get("research_bucket", ""),
                )
            )

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    rows, original_fields = read_rows(LEDGER_CSV)
    observations = collect_observations()

    updated = [update_row_clv(row, observations) for row in rows]
    summary = build_summary(updated)

    write_rows(LEDGER_CSV, updated, original_fields)
    write_markdown(updated, summary)

    OUT_JSON.write_text(
        json.dumps(
            {
                "mode": "FQIS_CLV_TRACKER",
                "generated_at_utc": utc_now(),
                "ledger_csv": str(LEDGER_CSV),
                "summary": summary,
                "rows": updated,
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    print(json.dumps({
        "status": "READY",
        "summary": summary,
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
    }, indent=2, ensure_ascii=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
