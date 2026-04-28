from __future__ import annotations

import csv
import json
import os
import urllib.parse
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
LEDGER_CSV = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
OUT_MD = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.md"
OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "latest_research_settlement.json"

FINISHED_STATUS = {"FT", "AET", "PEN"}

SETTLEMENT_FIELDS = [
    "fixture_status_short",
    "fixture_status_long",
    "fixture_elapsed",
    "final_home_goals",
    "final_away_goals",
    "final_total_goals",
    "provisional_result_if_now",
    "provisional_pnl_if_now",
    "settlement_status",
    "result_status",
    "pnl_unit",
]


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def fnum(value: Any, default: float = 0.0) -> float:
    try:
        if value is None or value == "":
            return default
        return float(str(value).replace(",", ".").strip())
    except Exception:
        return default


def safe_int(value: Any) -> int | None:
    try:
        if value is None or value == "":
            return None
        return int(float(str(value).strip()))
    except Exception:
        return None


def api_key_from_env_or_dotenv() -> str | None:
    names = [
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    ]

    for name in names:
        value = os.getenv(name)
        if value and value.strip():
            return value.strip()

    env_path = ROOT / ".env"
    if env_path.exists():
        for line in env_path.read_text(encoding="utf-8-sig").splitlines():
            if "=" not in line or line.strip().startswith("#"):
                continue
            key, value = line.split("=", 1)
            if key.strip() in names and value.strip():
                return value.strip().strip('"').strip("'")

    return None


def api_get_fixture(fixture_id: str, api_key: str | None) -> dict[str, Any]:
    if not api_key:
        return {"errors": ["missing_api_key"], "response": []}

    query = urllib.parse.urlencode({"id": fixture_id})
    url = f"https://v3.football.api-sports.io/fixtures?{query}"

    req = urllib.request.Request(
        url,
        headers={
            "x-apisports-key": api_key,
            "Accept": "application/json",
            "User-Agent": "FQIS-ResearchSettlement/1.0",
        },
        method="GET",
    )

    with urllib.request.urlopen(req, timeout=30) as response:
        return json.loads(response.read().decode("utf-8"))


def parse_fixture_result(payload: dict[str, Any]) -> dict[str, Any]:
    response = payload.get("response") or []
    if not response:
        return {
            "fixture_status_short": "",
            "fixture_status_long": "",
            "fixture_elapsed": "",
            "final_home_goals": "",
            "final_away_goals": "",
            "final_total_goals": "",
        }

    item = response[0] or {}
    fixture = item.get("fixture") or {}
    status = fixture.get("status") or {}
    goals = item.get("goals") or {}

    home = safe_int(goals.get("home"))
    away = safe_int(goals.get("away"))

    total = ""
    if home is not None and away is not None:
        total = str(home + away)

    return {
        "fixture_status_short": str(status.get("short") or ""),
        "fixture_status_long": str(status.get("long") or ""),
        "fixture_elapsed": "" if status.get("elapsed") is None else str(status.get("elapsed")),
        "final_home_goals": "" if home is None else str(home),
        "final_away_goals": "" if away is None else str(away),
        "final_total_goals": total,
    }


def read_rows(path: Path) -> tuple[list[dict[str, Any]], list[str]]:
    if not path.exists():
        return [], []

    with path.open("r", encoding="utf-8", newline="") as f:
        reader = csv.DictReader(f)
        return list(reader), list(reader.fieldnames or [])


def write_rows(path: Path, rows: list[dict[str, Any]], original_fields: list[str]) -> None:
    fieldnames = list(original_fields)

    for field in SETTLEMENT_FIELDS:
        if field not in fieldnames:
            fieldnames.append(field)

    for row in rows:
        for field in row:
            if field not in fieldnames:
                fieldnames.append(field)

    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def provisional_status(side: str, total_goals: int | None, line: float, odds_decimal: float) -> tuple[str, str]:
    if total_goals is None:
        return "", ""

    side = side.upper()

    if side == "UNDER":
        if total_goals < line:
            return "LIVE_WIN_IF_NOW", str(round(odds_decimal - 1.0, 6))
        if total_goals > line:
            return "LIVE_LOSS_IF_NOW", "-1.0"
        return "LIVE_PUSH_IF_NOW", "0.0"

    if side == "OVER":
        if total_goals > line:
            return "LIVE_WIN_IF_NOW", str(round(odds_decimal - 1.0, 6))
        if total_goals < line:
            return "LIVE_LOSS_IF_NOW", "-1.0"
        return "LIVE_PUSH_IF_NOW", "0.0"

    return "", ""


def settled_result(side: str, total_goals: int, line: float, odds_decimal: float) -> tuple[str, str]:
    side = side.upper()

    if side == "UNDER":
        if total_goals < line:
            return "WIN", str(round(odds_decimal - 1.0, 6))
        if total_goals > line:
            return "LOSS", "-1.0"
        return "PUSH", "0.0"

    if side == "OVER":
        if total_goals > line:
            return "WIN", str(round(odds_decimal - 1.0, 6))
        if total_goals < line:
            return "LOSS", "-1.0"
        return "PUSH", "0.0"

    return "UNSUPPORTED_SIDE", ""


def settle_row(row: dict[str, Any], fixture_result: dict[str, Any]) -> dict[str, Any]:
    row = dict(row)

    for key, value in fixture_result.items():
        row[key] = value

    side = str(row.get("side") or "").upper()
    line = fnum(row.get("line"))
    odds = fnum(row.get("odds_decimal"))

    total_goals = safe_int(row.get("final_total_goals"))

    live_result, live_pnl = provisional_status(side, total_goals, line, odds)
    row["provisional_result_if_now"] = live_result
    row["provisional_pnl_if_now"] = live_pnl

    status_short = str(row.get("fixture_status_short") or "").upper()

    if not status_short:
        row["settlement_status"] = "UNSETTLED"
        row["result_status"] = ""
        row["pnl_unit"] = ""
        return row

    if status_short not in FINISHED_STATUS:
        row["settlement_status"] = "PENDING"
        row["result_status"] = ""
        row["pnl_unit"] = ""
        return row

    if total_goals is None:
        row["settlement_status"] = "UNSETTLED"
        row["result_status"] = ""
        row["pnl_unit"] = ""
        return row

    result, pnl = settled_result(side, total_goals, line, odds)

    row["settlement_status"] = "SETTLED"
    row["result_status"] = result
    row["pnl_unit"] = pnl

    return row


def build_summary(rows: list[dict[str, Any]]) -> dict[str, Any]:
    settled = [r for r in rows if r.get("settlement_status") == "SETTLED"]
    pending = [r for r in rows if r.get("settlement_status") == "PENDING"]
    unsettled = [r for r in rows if r.get("settlement_status") == "UNSETTLED"]

    wins = [r for r in settled if r.get("result_status") == "WIN"]
    losses = [r for r in settled if r.get("result_status") == "LOSS"]
    pushes = [r for r in settled if r.get("result_status") == "PUSH"]

    pnl_total = sum(fnum(r.get("pnl_unit")) for r in settled)
    roi = pnl_total / len(settled) if settled else 0.0

    return {
        "rows_total": len(rows),
        "settled": len(settled),
        "pending": len(pending),
        "unsettled": len(unsettled),
        "wins": len(wins),
        "losses": len(losses),
        "pushes": len(pushes),
        "pnl_unit_total": round(pnl_total, 6),
        "roi_unit": round(roi, 6),
    }


def write_markdown(path: Path, rows: list[dict[str, Any]], summary: dict[str, Any]) -> None:
    lines = [
        "# FQIS Research Settlement",
        "",
        "## Summary",
        "",
        f"- Rows total: **{summary['rows_total']}**",
        f"- Settled: **{summary['settled']}**",
        f"- Pending: **{summary['pending']}**",
        f"- Unsettled: **{summary['unsettled']}**",
        f"- Wins: **{summary['wins']}**",
        f"- Losses: **{summary['losses']}**",
        f"- Pushes: **{summary['pushes']}**",
        f"- PnL unit total: **{summary['pnl_unit_total']}**",
        f"- ROI per settled snapshot: **{summary['roi_unit']}**",
        "",
        "> Research only. Not production staking.",
        "",
        "## Rows",
        "",
    ]

    if not rows:
        lines.append("No research rows.")
    else:
        lines.append("| Settlement | Fixture status | Settled result | Live if now | Match | Entry score | Goals now/final | Selection | Odds | Edge | EV | Settled PnL | Live PnL if now | Bucket |")
        lines.append("|---|---|---|---|---|---:|---:|---|---:|---:|---:|---:|---:|---|")

        for r in rows:
            lines.append(
                "| {settlement} | {fixture_status} | {result} | {live_result} | {match} | {score} | {goals} | {selection} | {odds:.3f} | {edge:.2f}% | {ev:.2f}% | {pnl} | {live_pnl} | {bucket} |".format(
                    settlement=r.get("settlement_status", ""),
                    fixture_status=r.get("fixture_status_short", ""),
                    result=r.get("result_status", ""),
                    live_result=r.get("provisional_result_if_now", ""),
                    match=str(r.get("match", "")).replace("|", "/"),
                    score=r.get("score", ""),
                    goals=r.get("final_total_goals", ""),
                    selection=r.get("selection", ""),
                    odds=fnum(r.get("odds_decimal")),
                    edge=fnum(r.get("edge")) * 100,
                    ev=fnum(r.get("expected_value")) * 100,
                    pnl=r.get("pnl_unit", ""),
                    live_pnl=r.get("provisional_pnl_if_now", ""),
                    bucket=r.get("research_bucket", ""),
                )
            )

    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    rows, original_fields = read_rows(LEDGER_CSV)

    api_key = api_key_from_env_or_dotenv()

    fixture_ids = sorted({
        str(row.get("fixture_id") or "")
        for row in rows
        if str(row.get("fixture_id") or "").strip()
    })

    results_by_fixture: dict[str, dict[str, Any]] = {}

    for fixture_id in fixture_ids:
        try:
            payload = api_get_fixture(fixture_id, api_key)
        except Exception as exc:
            payload = {"errors": [str(exc)], "response": []}

        results_by_fixture[fixture_id] = parse_fixture_result(payload)

    settled_rows = []
    for row in rows:
        fixture_id = str(row.get("fixture_id") or "")
        fixture_result = results_by_fixture.get(fixture_id, {})
        settled_rows.append(settle_row(row, fixture_result))

    summary = build_summary(settled_rows)

    write_rows(LEDGER_CSV, settled_rows, original_fields)
    write_markdown(OUT_MD, settled_rows, summary)

    OUT_JSON.write_text(
        json.dumps(
            {
                "mode": "FQIS_RESEARCH_SETTLEMENT",
                "generated_at_utc": utc_now(),
                "ledger_csv": str(LEDGER_CSV),
                "summary": summary,
                "rows": settled_rows,
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
    }, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
