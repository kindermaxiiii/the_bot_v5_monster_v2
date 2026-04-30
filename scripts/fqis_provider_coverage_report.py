from __future__ import annotations

import json
import re
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


ROOT = Path(__file__).resolve().parents[1]
RAW_DIR = ROOT / "data" / "pipeline" / "api_sports" / "level3_live_state" / "raw"
OUT_DIR = ROOT / "data" / "pipeline" / "api_sports" / "provider_coverage"
OUT_JSON = OUT_DIR / "latest_provider_coverage_report.json"
OUT_MD = OUT_DIR / "latest_provider_coverage_report.md"


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def load_json(path: Path) -> dict[str, Any]:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception as exc:
        return {"errors": [str(exc)], "response": []}


def parse_file(path: Path) -> dict[str, Any]:
    name = path.name
    match = re.search(r"fixture_(\d+)_(events|statistics)\.json$", name)

    fixture_id = match.group(1) if match else ""
    kind = match.group(2) if match else "unknown"

    payload = load_json(path)
    response = payload.get("response") or []
    errors = payload.get("errors") or {}

    return {
        "fixture_id": fixture_id,
        "kind": kind,
        "file": name,
        "has_data": len(response) > 0,
        "response_count": len(response),
        "errors": errors,
        "last_write": path.stat().st_mtime,
        "last_write_text": datetime.fromtimestamp(path.stat().st_mtime).isoformat(),
        "length": path.stat().st_size,
    }


def coverage_label(events: bool, stats: bool) -> str:
    if events and stats:
        return "EVENTS_PLUS_STATS"
    if events and not stats:
        return "EVENTS_ONLY"
    if not events and stats:
        return "STATS_ONLY_ANOMALY"
    return "SCORE_ONLY"


def pct(n: int, d: int) -> str:
    if d <= 0:
        return "0.00%"
    return f"{n / d * 100:.2f}%"


def main() -> int:
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    files = sorted(RAW_DIR.glob("fixture_*_*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    parsed = [parse_file(p) for p in files]

    by_fixture: dict[str, dict[str, Any]] = defaultdict(lambda: {
        "fixture_id": "",
        "events_available": False,
        "statistics_available": False,
        "events_count": 0,
        "statistics_count": 0,
        "latest_seen_ts": 0.0,
        "latest_seen": "",
        "files": [],
    })

    for row in parsed:
        fid = row["fixture_id"]
        if not fid:
            continue

        item = by_fixture[fid]
        item["fixture_id"] = fid
        item["files"].append(row["file"])
        item["latest_seen_ts"] = max(float(item["latest_seen_ts"]), float(row["last_write"]))
        item["latest_seen"] = datetime.fromtimestamp(float(item["latest_seen_ts"])).isoformat()

        if row["kind"] == "events":
            item["events_available"] = bool(row["has_data"])
            item["events_count"] = int(row["response_count"])

        if row["kind"] == "statistics":
            item["statistics_available"] = bool(row["has_data"])
            item["statistics_count"] = int(row["response_count"])

    fixtures = []
    for item in by_fixture.values():
        item["coverage_label"] = coverage_label(
            bool(item["events_available"]),
            bool(item["statistics_available"]),
        )
        fixtures.append(item)

    fixtures.sort(key=lambda x: (x["latest_seen_ts"], x["coverage_label"]), reverse=True)

    total = len(fixtures)
    events_available = sum(1 for f in fixtures if f["events_available"])
    stats_available = sum(1 for f in fixtures if f["statistics_available"])
    events_plus_stats = sum(1 for f in fixtures if f["coverage_label"] == "EVENTS_PLUS_STATS")
    events_only = sum(1 for f in fixtures if f["coverage_label"] == "EVENTS_ONLY")
    score_only = sum(1 for f in fixtures if f["coverage_label"] == "SCORE_ONLY")

    by_label = defaultdict(int)
    for f in fixtures:
        by_label[f["coverage_label"]] += 1

    summary = {
        "fixtures_total": total,
        "events_available": events_available,
        "statistics_available": stats_available,
        "events_plus_stats": events_plus_stats,
        "events_only": events_only,
        "score_only": score_only,
        "events_coverage_rate": events_available / total if total else 0.0,
        "statistics_coverage_rate": stats_available / total if total else 0.0,
        "events_plus_stats_rate": events_plus_stats / total if total else 0.0,
        "coverage_labels": dict(sorted(by_label.items())),
    }

    payload = {
        "mode": "FQIS_PROVIDER_COVERAGE_REPORT",
        "generated_at_utc": utc_now(),
        "raw_dir": str(RAW_DIR),
        "summary": summary,
        "fixtures": fixtures,
    }

    OUT_JSON.write_text(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True), encoding="utf-8")

    lines = [
        "# FQIS Provider Coverage Report",
        "",
        "## Summary",
        "",
        f"- Fixtures total: **{total}**",
        f"- Events available: **{events_available}** / {total} = **{pct(events_available, total)}**",
        f"- Statistics available: **{stats_available}** / {total} = **{pct(stats_available, total)}**",
        f"- Events + stats: **{events_plus_stats}** / {total} = **{pct(events_plus_stats, total)}**",
        f"- Events only: **{events_only}** / {total} = **{pct(events_only, total)}**",
        f"- Score only: **{score_only}** / {total} = **{pct(score_only, total)}**",
        "",
        "## Coverage Labels",
        "",
        "| Label | Count |",
        "|---|---:|",
    ]

    for label, count in sorted(by_label.items()):
        lines.append(f"| {label} | {count} |")

    lines += [
        "",
        "## Fixtures",
        "",
        "| Fixture | Coverage | Events | Stats | Events count | Stats count | Latest seen |",
        "|---:|---|---|---|---:|---:|---|",
    ]

    for f in fixtures[:200]:
        lines.append(
            "| {fixture_id} | {coverage} | {events} | {stats} | {events_count} | {stats_count} | {latest} |".format(
                fixture_id=f["fixture_id"],
                coverage=f["coverage_label"],
                events="yes" if f["events_available"] else "no",
                stats="yes" if f["statistics_available"] else "no",
                events_count=f["events_count"],
                stats_count=f["statistics_count"],
                latest=f["latest_seen"],
            )
        )

    OUT_MD.write_text("\n".join(lines) + "\n", encoding="utf-8")

    print(json.dumps({
        "status": "READY",
        "summary": summary,
        "output_md": str(OUT_MD),
        "output_json": str(OUT_JSON),
    }, indent=2, ensure_ascii=True))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
