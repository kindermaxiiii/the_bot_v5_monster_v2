from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional


def _safe_int(value: Any) -> Optional[int]:
    if value in (None, "", "null"):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_datetime(value: Any) -> Optional[datetime]:
    if not value:
        return None
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc).replace(tzinfo=None) if value.tzinfo else value
    if isinstance(value, str):
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        return parsed.astimezone(timezone.utc).replace(tzinfo=None) if parsed.tzinfo else parsed
    return None


def normalize_live_fixtures(raw: Dict[str, Any]) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    for item in raw.get("response", []) or []:
        fixture = item.get("fixture", {}) or {}
        league = item.get("league", {}) or {}
        teams = item.get("teams", {}) or {}
        goals = item.get("goals", {}) or {}
        status = fixture.get("status", {}) or {}
        home = teams.get("home", {}) or {}
        away = teams.get("away", {}) or {}
        out.append({
            "fixture_id": fixture.get("id"),
            "league_id": league.get("id"),
            "league_name": league.get("name"),
            "season": league.get("season"),
            "country_name": league.get("country"),
            "status": (status.get("short") or ""),
            "period": (status.get("short") or ""),
            "minute": _safe_int(status.get("elapsed")),
            "start_time_utc": _safe_datetime(fixture.get("date")),
            "home_team_id": home.get("id"),
            "home_team_name": home.get("name") or "Home",
            "away_team_id": away.get("id"),
            "away_team_name": away.get("name") or "Away",
            "home_goals": _safe_int(goals.get("home")) or 0,
            "away_goals": _safe_int(goals.get("away")) or 0,
            "home_red": 0,
            "away_red": 0,
            "home_yellow": 0,
            "away_yellow": 0,
            "home_subs": 0,
            "away_subs": 0,
            "raw_payload_json": item,
        })
    return out
