from __future__ import annotations

from datetime import datetime, timezone
from typing import Any


def _safe_int(value: Any) -> int | None:
    if value in (None, "", "null"):
        return None
    try:
        return int(float(value))
    except (TypeError, ValueError):
        return None


def _safe_datetime(value: Any) -> datetime | None:
    if not value:
        return None

    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value

    if isinstance(value, str):
        text = value.strip()
        if not text:
            return None
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            parsed = datetime.fromisoformat(text)
        except ValueError:
            return None
        if parsed.tzinfo is not None:
            return parsed.astimezone(timezone.utc).replace(tzinfo=None)
        return parsed

    return None


def _safe_unix_datetime(value: Any) -> datetime | None:
    iv = _safe_int(value)
    if iv is None:
        return None
    try:
        return datetime.fromtimestamp(iv, tz=timezone.utc).replace(tzinfo=None)
    except (OverflowError, OSError, ValueError):
        return None


def _norm_text(value: Any) -> str:
    return str(value or "").strip().casefold()


def _event_team_name(event: dict[str, Any]) -> str:
    return _norm_text(((event.get("team") or {}).get("name")))


def _event_type(event: dict[str, Any]) -> str:
    return _norm_text((event.get("type") or "") + " " + (event.get("detail") or ""))


def _canonical_status_short(value: Any) -> str:
    short = str(value or "").upper().strip()
    if short in {"1H", "2H", "HT", "FT", "ET", "P", "AET", "PEN", "SUSP", "INT", "LIVE", "NS"}:
        return short
    return short or ""


def normalize_live_fixtures(raw: dict[str, Any]) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []

    for item in raw.get("response", []) or []:
        fixture = item.get("fixture", {}) or {}
        league = item.get("league", {}) or {}
        teams = item.get("teams", {}) or {}
        goals = item.get("goals", {}) or {}
        events = item.get("events", []) or []
        status_block = fixture.get("status", {}) or {}

        fixture_id = _safe_int(fixture.get("id"))
        if fixture_id is None:
            continue

        home_team = teams.get("home", {}) or {}
        away_team = teams.get("away", {}) or {}

        home_name = str(home_team.get("name") or "Home").strip()
        away_name = str(away_team.get("name") or "Away").strip()

        home_name_norm = _norm_text(home_name)
        away_name_norm = _norm_text(away_name)

        reds_home = 0
        reds_away = 0
        yellows_home = 0
        yellows_away = 0
        subs_home = 0
        subs_away = 0

        events_present = len(events) > 0

        for ev in events:
            etype = _event_type(ev)
            tname = _event_team_name(ev)

            is_home = tname == home_name_norm
            is_away = tname == away_name_norm

            if "card" in etype:
                if "red" in etype:
                    reds_home += int(is_home)
                    reds_away += int(is_away)
                elif "yellow" in etype:
                    yellows_home += int(is_home)
                    yellows_away += int(is_away)
            elif "subst" in etype or "substitution" in etype:
                subs_home += int(is_home)
                subs_away += int(is_away)

        status_short = _canonical_status_short(status_block.get("short"))
        status_long = str(status_block.get("long") or "").strip()
        status_elapsed = _safe_int(status_block.get("elapsed"))
        status_extra = _safe_int(status_block.get("extra"))

        home_goals = _safe_int(goals.get("home"))
        away_goals = _safe_int(goals.get("away"))

        row = {
            "fixture_id": fixture_id,
            "league_id": _safe_int(league.get("id")),
            "league_name": league.get("name"),
            "league_logo": league.get("logo"),
            "season": _safe_int(league.get("season")),
            "country_name": league.get("country"),

            # Canonical live state fields used downstream
            "status": status_short,
            "period": status_short,
            "minute": status_elapsed,
            "start_time_utc": _safe_datetime(fixture.get("date")),

            "home_team_id": _safe_int(home_team.get("id")),
            "home_team_name": home_name,
            "home_team_logo": home_team.get("logo"),

            "away_team_id": _safe_int(away_team.get("id")),
            "away_team_name": away_name,
            "away_team_logo": away_team.get("logo"),

            # Canonical score truth
            "home_goals": home_goals if home_goals is not None else 0,
            "away_goals": away_goals if away_goals is not None else 0,

            # Event-derived disciplinary counts
            "home_red": reds_home,
            "away_red": reds_away,
            "home_yellow": yellows_home,
            "away_yellow": yellows_away,
            "home_subs": subs_home,
            "away_subs": subs_away,

            # Extra audit fields
            "status_long": status_long,
            "status_extra": status_extra,
            "fixture_timestamp_utc": _safe_unix_datetime(fixture.get("timestamp")),
            "score_source": "goals.home_away",
            "minute_source": "fixture.status.elapsed",
            "event_coverage_present": events_present,

            "raw_payload_json": item,
        }

        out.append(row)

    return out