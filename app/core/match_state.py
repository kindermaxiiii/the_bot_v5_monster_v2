from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

from app.config import settings


@dataclass(slots=True)
class TeamLiveStats:
    team_id: int | None = None
    name: str = ""
    logo: str = ""
    shots_total: int = 0
    shots_on_target: int = 0
    shots_inside_box: int = 0
    blocked_shots: int = 0
    corners: int = 0
    saves: int = 0
    possession: float | None = None
    pass_accuracy: float | None = None
    dangerous_attacks: int | None = None
    attacks: int | None = None


@dataclass(slots=True)
class MarketQuote:
    market_key: str
    scope: str
    side: str
    line: float | None
    bookmaker: str
    odds_decimal: float
    is_main: bool | None = None
    raw: dict[str, Any] = field(default_factory=dict)


@dataclass(slots=True)
class MatchState:
    fixture_id: int
    competition_id: int | None = None
    competition_name: str = ""
    competition_logo: str = ""
    country_name: str = ""
    start_time_utc: datetime | None = None

    minute: int | None = None
    phase: str = ""
    status: str = ""

    home_goals: int = 0
    away_goals: int = 0
    home_reds: int = 0
    away_reds: int = 0

    feed_quality_score: float = settings.feed_quality_default
    competition_quality_score: float = settings.competition_quality_default
    market_quality_score: float = settings.market_quality_default

    home: TeamLiveStats = field(default_factory=TeamLiveStats)
    away: TeamLiveStats = field(default_factory=TeamLiveStats)

    stats_windows: dict[str, dict[str, Any]] = field(default_factory=dict)
    odds_windows: dict[str, dict[str, Any]] = field(default_factory=dict)
    active_theses: dict[str, Any] = field(default_factory=dict)

    quotes: list[MarketQuote] = field(default_factory=list)
    lineups: list[dict[str, Any]] = field(default_factory=list)
    players: list[dict[str, Any]] = field(default_factory=list)

    raw: dict[str, Any] = field(default_factory=dict)

    @property
    def total_goals(self) -> int:
        return self.home_goals + self.away_goals

    @property
    def goal_diff(self) -> int:
        return self.home_goals - self.away_goals

    @property
    def game_state(self) -> str:
        if self.goal_diff == 0:
            return "DRAW"
        return "HOME_LEAD" if self.goal_diff > 0 else "AWAY_LEAD"

    @property
    def trailing_side(self) -> str:
        if self.goal_diff == 0:
            return "NONE"
        return "AWAY" if self.goal_diff > 0 else "HOME"

    @property
    def leading_side(self) -> str:
        if self.goal_diff == 0:
            return "NONE"
        return "HOME" if self.goal_diff > 0 else "AWAY"

    @property
    def score_text(self) -> str:
        return f"{self.home_goals}-{self.away_goals}"

    @property
    def time_remaining_estimate(self) -> int:
        minute = int(self.minute or 0)
        status = str(self.status or "").upper().strip()

        if status == "HT":
            return 15

        # estimation grossière seulement ; la vraie intensité gère mieux le temps restant
        if minute <= 45:
            added = 2
            horizon = 45 + added
        else:
            added = 4
            horizon = 90 + added

        return max(0, horizon - minute)


# ----------------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------------

def _f(value: Any, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _i(value: Any, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _clamp(x: float, lo: float, hi: float) -> float:
    return max(lo, min(hi, x))


def _competition_quality(name: str, country: str) -> float:
    full = f"{country} {name}".lower().strip()

    elite = [
        "premier league",
        "la liga",
        "serie a",
        "bundesliga",
        "ligue 1",
        "champions league",
        "europa league",
        "eredi",
        "championship",
    ]
    good = [
        "primeira",
        "super liga",
        "liga 1",
        "division 2",
        "j1",
        "mls",
        "first division",
        "second division",
        "pro league",
    ]

    if any(token in full for token in elite):
        return 0.84
    if any(token in full for token in good):
        return 0.70
    return settings.competition_quality_default


def _normalize_dt(value: Any) -> datetime | None:
    if value is None:
        return None
    if isinstance(value, datetime):
        if value.tzinfo is not None:
            return value.astimezone(timezone.utc).replace(tzinfo=None)
        return value
    return None


def _normalize_live_status(period: Any, status: Any, minute: int) -> tuple[str, str]:
    raw_period = str(period or "").upper().strip()
    raw_status = str(status or "").upper().strip()

    canonical_status = raw_status or raw_period or ""

    if raw_period in {"1H", "2H", "HT", "ET", "PEN"}:
        phase = raw_period
    elif raw_status in {"1H", "2H", "HT", "ET", "PEN"}:
        phase = raw_status
    elif minute > 0 and raw_status not in {"NS", "FT"}:
        phase = "LIVE"
    else:
        phase = raw_status or raw_period or ""

    return phase, canonical_status


def _sanitize_minute(minute: Any, status: str) -> int:
    m = _i(minute, 0)
    s = str(status or "").upper().strip()

    if s in {"NS"}:
        return 0
    if s in {"HT"}:
        return 45
    if s in {"FT", "AET", "PEN"}:
        return max(m, 90 if m == 0 else m)

    return max(0, min(m, 130))


def _sanitize_score(value: Any) -> int:
    return max(0, _i(value, 0))


def _sanitize_optional_float(value: Any) -> float | None:
    if value in (None, "", "null"):
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _merged_red_count(fixture_red: Any, stats_red: Any) -> int:
    """
    Vérité rouge = max(event/fixture, statistics).
    On préfère surestimer légèrement un rouge plutôt que l'ignorer.
    """
    return max(_sanitize_score(fixture_red), _sanitize_score(stats_red))


def _compute_feed_quality(stats_row: dict[str, Any], quotes_count: int) -> float:
    if not stats_row:
        return max(0.30, min(0.70, settings.feed_quality_default))

    stat_presence = sum(
        1
        for k in [
            "home_shots_total",
            "away_shots_total",
            "home_shots_on",
            "away_shots_on",
            "home_corners",
            "away_corners",
            "home_dangerous_attacks",
            "away_dangerous_attacks",
            "home_attacks",
            "away_attacks",
            "home_possession",
            "away_possession",
        ]
        if stats_row.get(k) not in (None, "")
    )

    score = 0.18 + 0.055 * stat_presence
    if quotes_count > 0:
        score += 0.05

    return _clamp(score, 0.28, 0.92)


# ----------------------------------------------------------------------
# Builder
# ----------------------------------------------------------------------

def build_match_state(
    fixture_row: dict[str, Any],
    stats_row: dict[str, Any] | None = None,
    odds_rows: list[dict[str, Any]] | None = None,
    lineups_rows: list[dict[str, Any]] | None = None,
    players_rows: list[dict[str, Any]] | None = None,
) -> MatchState:
    stats_row = stats_row or {}

    home_goals = _sanitize_score(fixture_row.get("home_goals"))
    away_goals = _sanitize_score(fixture_row.get("away_goals"))

    phase, status = _normalize_live_status(
        fixture_row.get("period"),
        fixture_row.get("status"),
        _i(fixture_row.get("minute"), 0),
    )
    minute = _sanitize_minute(fixture_row.get("minute"), status)

    home_reds = _merged_red_count(
        fixture_row.get("home_red"),
        stats_row.get("home_red_cards"),
    )
    away_reds = _merged_red_count(
        fixture_row.get("away_red"),
        stats_row.get("away_red_cards"),
    )

    home = TeamLiveStats(
        team_id=fixture_row.get("home_team_id"),
        name=str(fixture_row.get("home_team_name") or "Home"),
        logo=str(fixture_row.get("home_team_logo") or ""),
        shots_total=_sanitize_score(stats_row.get("home_shots_total")),
        shots_on_target=_sanitize_score(stats_row.get("home_shots_on")),
        shots_inside_box=_sanitize_score(stats_row.get("home_shots_inside_box")),
        blocked_shots=_sanitize_score(stats_row.get("home_blocked_shots")),
        corners=_sanitize_score(stats_row.get("home_corners")),
        saves=_sanitize_score(stats_row.get("home_saves")),
        possession=_sanitize_optional_float(stats_row.get("home_possession")),
        pass_accuracy=_sanitize_optional_float(stats_row.get("home_pass_accuracy")),
        dangerous_attacks=_i(stats_row.get("home_dangerous_attacks"), 0),
        attacks=_i(stats_row.get("home_attacks"), 0),
    )

    away = TeamLiveStats(
        team_id=fixture_row.get("away_team_id"),
        name=str(fixture_row.get("away_team_name") or "Away"),
        logo=str(fixture_row.get("away_team_logo") or ""),
        shots_total=_sanitize_score(stats_row.get("away_shots_total")),
        shots_on_target=_sanitize_score(stats_row.get("away_shots_on")),
        shots_inside_box=_sanitize_score(stats_row.get("away_shots_inside_box")),
        blocked_shots=_sanitize_score(stats_row.get("away_blocked_shots")),
        corners=_sanitize_score(stats_row.get("away_corners")),
        saves=_sanitize_score(stats_row.get("away_saves")),
        possession=_sanitize_optional_float(stats_row.get("away_possession")),
        pass_accuracy=_sanitize_optional_float(stats_row.get("away_pass_accuracy")),
        dangerous_attacks=_i(stats_row.get("away_dangerous_attacks"), 0),
        attacks=_i(stats_row.get("away_attacks"), 0),
    )

    quotes: list[MarketQuote] = []
    for row in odds_rows or []:
        try:
            odds = float(row.get("odds_decimal") or 0.0)
        except (TypeError, ValueError):
            odds = 0.0

        quotes.append(
            MarketQuote(
                market_key=str(row.get("market_key") or "unknown"),
                scope=str(row.get("market_scope") or "FT"),
                side=str(row.get("selection_name") or ""),
                line=row.get("line_value"),
                bookmaker=str(row.get("bookmaker") or ""),
                odds_decimal=odds,
                is_main=row.get("is_main"),
                raw=row,
            )
        )

    competition_quality = _competition_quality(
        str(fixture_row.get("league_name") or ""),
        str(fixture_row.get("country_name") or ""),
    )

    feed_quality = _compute_feed_quality(stats_row, len(quotes))
    market_quality = 0.72 if quotes else settings.market_quality_default

    raw = {
        "fixture": fixture_row,
        "stats": stats_row,
        "quotes_count": len(quotes),
        "used_truth": {
            "minute": minute,
            "phase": phase,
            "status": status,
            "home_goals": home_goals,
            "away_goals": away_goals,
            "home_reds": home_reds,
            "away_reds": away_reds,
        },
        "source_fields": {
            "period": fixture_row.get("period"),
            "status": fixture_row.get("status"),
            "minute": fixture_row.get("minute"),
            "home_goals": fixture_row.get("home_goals"),
            "away_goals": fixture_row.get("away_goals"),
            "home_red_fixture": fixture_row.get("home_red"),
            "away_red_fixture": fixture_row.get("away_red"),
            "home_red_stats": stats_row.get("home_red_cards"),
            "away_red_stats": stats_row.get("away_red_cards"),
        },
        "league_name": fixture_row.get("league_name"),
        "league_logo": fixture_row.get("league_logo"),
    }

    return MatchState(
        fixture_id=int(fixture_row["fixture_id"]),
        competition_id=fixture_row.get("league_id"),
        competition_name=str(fixture_row.get("league_name") or ""),
        competition_logo=str(fixture_row.get("league_logo") or ""),
        country_name=str(fixture_row.get("country_name") or ""),
        start_time_utc=_normalize_dt(fixture_row.get("start_time_utc")),
        minute=minute,
        phase=phase,
        status=status,
        home_goals=home_goals,
        away_goals=away_goals,
        home_reds=home_reds,
        away_reds=away_reds,
        competition_quality_score=competition_quality,
        feed_quality_score=feed_quality,
        market_quality_score=market_quality,
        home=home,
        away=away,
        quotes=quotes,
        lineups=lineups_rows or [],
        players=players_rows or [],
        raw=raw,
    )