from __future__ import annotations

import csv
import json
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]

LEDGER_CSV = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
RUNS_DIR = ROOT / "data" / "pipeline" / "api_sports" / "decision_bridge_live"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_horizon_audit.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_clv_horizon_audit.json"

HORIZONS_MINUTES = [1, 5, 15]


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
        dt = datetime.fromisoformat(text)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def read_csv(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        return []

    with path.open("r", encoding="utf-8", newline="") as f:
        return list(csv.DictReader(f))


def load_json(path: Path) -> dict[str, Any] | None:
    try:
        if not path.exists():
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def key_from_decision(d: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(d.get("fixture_id") or ""),
        str(d.get("market_key") or ""),
        str(d.get("side") or "").upper(),
        str(d.get("line") or ""),
        str(d.get("selection") or ""),
    )


def key_from_ledger(row: dict[str, Any]) -> tuple[str, str, str, str, str]:
    return (
        str(row.get("fixture_id") or ""),
        str(row.get("market_key") or ""),
        str(row.get("side") or "").upper(),
        str(row.get("line") or ""),
        str(row.get("selection") or ""),
    )


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
            odds = fnum(decision.get("odds_decimal"), 0.0)
            if odds <= 1.0:
                continue

            key = key_from_decision(decision)
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


def compute_clv(entry_odds: float, future_odds: float) -> float:
    if entry_odds <= 1.0 or future_odds <= 1.0:
        return 0.0
    return (entry_odds / future_odds) - 1.0


def first_obs_after(obs: list[dict[str, Any]], target_time: datetime) -> dict[str, Any] | None:
    for item in obs:
        obs_time = parse_dt(item.get("observed_at_utc"))
        if obs_time is not None and obs_time >= target_time:
            return item
    return None


def last_obs_after(obs: list[dict[str, Any]], entry_time: datetime) -> dict[str, Any] | None:
    valid = []
    for item in obs:
        obs_time = parse_dt(item.get("observed_at_utc"))
        if obs_time is not None and obs_time >= entry_time:
            valid.append(item)
    return valid[-1] if valid else None


def audit_row(row: dict[str, Any], observations: dict[tuple[str, str, str, str, str], list[dict[str, Any]]]) -> dict[str, Any]:
    key = key_from_ledger(row)
    obs = observations.get(key) or []

    entry_time = parse_dt(row.get("observed_at_utc"))
    entry_odds = fnum(row.get("odds_decimal"), 0.0)

    out = {
        "snapshot_key": row.get("snapshot_key", ""),
        "signal_key": row.get("signal_key", ""),
        "fixture_id": row.get("fixture_id", ""),
        "match": row.get("match", ""),
        "score": row.get("score", ""),
        "minute": row.get("minute", ""),
        "selection": row.get("selection", ""),
        "entry_odds": entry_odds,
        "result_status": row.get("result_status", ""),
        "pnl_unit": row.get("pnl_unit", ""),
        "research_bucket": row.get("research_bucket", ""),
        "near_close_odds": "",
        "near_close_clv": "",
        "near_close_time_delta_sec": "",
    }

    if entry_time is None or entry_odds <= 1.0 or not obs:
        for h in HORIZONS_MINUTES:
            out[f"clv_{h}m_status"] = "UNTRACKED"
            out[f"clv_{h}m_odds"] = ""
            out[f"clv_{h}m_decimal"] = ""
        out["near_close_status"] = "UNTRACKED"
        return out

    for h in HORIZONS_MINUTES:
        item = first_obs_after(obs, entry_time + timedelta(minutes=h))
        if not item:
            out[f"clv_{h}m_status"] = "NO_HORIZON_OBSERVATION"
            out[f"clv_{h}m_odds"] = ""
            out[f"clv_{h}m_decimal"] = ""
            continue

        future_odds = fnum(item.get("odds_decimal"), 0.0)
        if future_odds <= 1.0:
            out[f"clv_{h}m_status"] = "INVALID_ODDS"
            out[f"clv_{h}m_odds"] = ""
            out[f"clv_{h}m_decimal"] = ""
            continue

        out[f"clv_{h}m_status"] = "TRACKED"
        out[f"clv_{h}m_odds"] = round(future_odds, 6)
        out[f"clv_{h}m_decimal"] = round(compute_clv(entry_odds, future_odds), 6)

    near = last_obs_after(obs, entry_time)
    if near:
        near_time = parse_dt(near.get("observed_at_utc"))
        near_odds = fnum(near.get("odds_decimal"), 0.0)

        if near_time and near_odds > 1.0:
            out["near_close_status"] = "TRACKED"
            out["near_close_odds"] = round(near_odds, 6)
            out["near_close_clv"] = round(compute_clv(entry_odds, near_odds), 6)
            out["near_close_time_delta_sec"] = round((near_time - entry_time).total_seconds(), 3)
        else:
            out["near_close_status"] = "INVALID_ODDS"
    else:
        out["near_close_status"] = "NO_FUTURE_OBSERVATION"

    return out


def horizon_summary(rows: list[dict[str, Any]], field: str) -> dict[str, Any]:
    tracked = [r for r in rows if r.get(field) not in {"", None}]
    values = [fnum(r.get(field)) for r in tracked]

    positive = [v for v in values if v > 0]
    negative = [v for v in values if v < 0]
    flat = [v for v in values if v == 0]

    return {
        "tracked": len(values),
        "positive": len(positive),
        "negative": len(negative),
        "flat": len(flat),
        "avg": round(sum(values) / len(values), 6) if values else 0.0,
        "positive_rate": round(len(positive) / len(values), 6) if values else 0.0,
    }


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    summary = {
        "rows_total": len(rows),
        "horizons": {},
    }

    for h in HORIZONS_MINUTES:
        summary["horizons"][f"{h}m"] = horizon_summary(rows, f"clv_{h}m_decimal")

    summary["horizons"]["near_close"] = horizon_summary(rows, "near_close_clv")

    return summary


def write_markdown(rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    lines = [
        "# FQIS CLV Horizon Audit",
        "",
        "## Summary",
        "",
        f"- Rows total: **{summary['rows_total']}**",
        "",
        "| Horizon | Tracked | Positive | Negative | Flat | Avg CLV | Positive rate |",
        "|---|---:|---:|---:|---:|---:|---:|",
    ]

    for name, s in summary["horizons"].items():
        lines.append(
            f"| {name} | {s['tracked']} | {s['positive']} | {s['negative']} | {s['flat']} | {s['avg']:.2%} | {s['positive_rate']:.2%} |"
        )

    lines.extend([
        "",
        "## Interpretation",
        "",
        "Near-close CLV is diagnostic only for live markets because odds can move mechanically with time decay.",
        "For promotion decisions, prefer fixed-horizon CLV, especially 5m and 15m, combined with settlement and calibration.",
        "",
        "## Rows",
        "",
        "| Match | Score | Min | Selection | Entry | CLV 1m | CLV 5m | CLV 15m | Near-close CLV | Result | PnL | Bucket |",
        "|---|---:|---:|---|---:|---:|---:|---:|---:|---|---:|---|",
    ])

    for r in rows:
        def pct_field(name: str) -> str:
            value = r.get(name, "")
            return "" if value == "" else f"{fnum(value):.2%}"

        lines.append(
            "| {match} | {score} | {minute} | {selection} | {entry:.3f} | {c1} | {c5} | {c15} | {near} | {result} | {pnl} | {bucket} |".format(
                match=str(r.get("match", "")).replace("|", "/"),
                score=r.get("score", ""),
                minute=r.get("minute", ""),
                selection=r.get("selection", ""),
                entry=fnum(r.get("entry_odds")),
                c1=pct_field("clv_1m_decimal"),
                c5=pct_field("clv_5m_decimal"),
                c15=pct_field("clv_15m_decimal"),
                near=pct_field("near_close_clv"),
                result=r.get("result_status", ""),
                pnl=r.get("pnl_unit", ""),
                bucket=r.get("research_bucket", ""),
            )
        )

    OUT_MD.parent.mkdir(parents=True, exist_ok=True)
    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    ledger_rows = read_csv(LEDGER_CSV)
    observations = collect_observations()

    audited_rows = [audit_row(row, observations) for row in ledger_rows]
    summary = build_summary(audited_rows)

    OUT_JSON.parent.mkdir(parents=True, exist_ok=True)
    OUT_JSON.write_text(
        json.dumps(
            {
                "mode": "FQIS_CLV_HORIZON_AUDIT",
                "generated_at_utc": utc_now(),
                "ledger_csv": str(LEDGER_CSV),
                "summary": summary,
                "rows": audited_rows,
            },
            indent=2,
            ensure_ascii=False,
            sort_keys=True,
        ),
        encoding="utf-8",
    )

    write_markdown(audited_rows, summary)

    print(json.dumps({
        "status": "READY",
        "summary": summary,
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
