from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

INPLAY_MODE = "INPLAY_FIXTURES"
DEFAULT_LIVE_STATUSES = ("1H", "HT", "2H", "ET", "BT", "P", "SUSP", "INT", "LIVE")


@dataclass(frozen=True)
class ApiSportsInplayFixturesConfig:
    live: str = "all"
    timezone: str = "Europe/Paris"
    max_fixtures: int = 100
    live_statuses: tuple[str, ...] = DEFAULT_LIVE_STATUSES

    @classmethod
    def from_env(cls) -> "ApiSportsInplayFixturesConfig":
        return cls(
            live=os.getenv("APISPORTS_INPLAY_LIVE", "all").strip() or "all",
            timezone=os.getenv("APISPORTS_INPLAY_TIMEZONE", "Europe/Paris").strip() or "Europe/Paris",
            max_fixtures=_env_int("APISPORTS_INPLAY_MAX_FIXTURES", 100),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "live": self.live,
            "timezone": self.timezone,
            "max_fixtures": self.max_fixtures,
            "live_statuses": list(self.live_statuses),
        }


@dataclass(frozen=True)
class ApiSportsInplayFixture:
    fixture_id: int
    match: str
    home_team: str
    away_team: str
    league_name: str | None
    league_id: int | None
    country: str | None
    kickoff_utc: str | None
    elapsed: int | None
    status_short: str | None
    status_long: str | None
    score_home: int | None
    score_away: int | None
    goals_home: int | None
    goals_away: int | None
    live: bool

    def to_dict(self) -> dict[str, object]:
        return {
            "fixture_id": self.fixture_id,
            "match": self.match,
            "home_team": self.home_team,
            "away_team": self.away_team,
            "league_name": self.league_name,
            "league_id": self.league_id,
            "country": self.country,
            "kickoff_utc": self.kickoff_utc,
            "elapsed": self.elapsed,
            "status_short": self.status_short,
            "status_long": self.status_long,
            "score_home": self.score_home,
            "score_away": self.score_away,
            "goals_home": self.goals_home,
            "goals_away": self.goals_away,
            "live": self.live,
        }


@dataclass(frozen=True)
class ApiSportsRejectedInplayFixture:
    fixture_id: int | None
    match: str | None
    status_short: str | None
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "fixture_id": self.fixture_id,
            "match": self.match,
            "status_short": self.status_short,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ApiSportsInplayFixtures:
    status: str
    mode: str
    real_staking_enabled: bool
    generated_at_utc: str
    config: ApiSportsInplayFixturesConfig
    fixtures: tuple[ApiSportsInplayFixture, ...]
    rejected: tuple[ApiSportsRejectedInplayFixture, ...]
    warnings: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def ready(self) -> bool:
        return self.status == "READY"

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "ready": self.ready,
            "mode": self.mode,
            "real_staking_enabled": self.real_staking_enabled,
            "generated_at_utc": self.generated_at_utc,
            "config": self.config.to_dict(),
            "fixtures": [item.to_dict() for item in self.fixtures],
            "rejected": [item.to_dict() for item in self.rejected],
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "summary": {
                "fixtures_total": len(self.fixtures),
                "rejected_total": len(self.rejected),
                "warnings_total": len(self.warnings),
                "errors_total": len(self.errors),
            },
        }


class ApiSportsInplayFixturesError(RuntimeError):
    pass


def build_api_sports_inplay_fixtures_from_payload(
    payload: Mapping[str, Any],
    *,
    config: ApiSportsInplayFixturesConfig | None = None,
) -> ApiSportsInplayFixtures:
    inplay_config = config or ApiSportsInplayFixturesConfig.from_env()
    response = payload.get("response", [])

    fixtures: list[ApiSportsInplayFixture] = []
    rejected: list[ApiSportsRejectedInplayFixture] = []

    if not isinstance(response, Sequence) or isinstance(response, (str, bytes, bytearray)):
        raise ApiSportsInplayFixturesError("API-Sports in-play payload response must be a list.")

    for item in response:
        if not isinstance(item, Mapping):
            continue

        fixture_id = _fixture_id(item)
        match = _match_name(item)
        status_short = _status_short(item)

        if fixture_id is None:
            rejected.append(
                ApiSportsRejectedInplayFixture(
                    fixture_id=None,
                    match=match,
                    status_short=status_short,
                    reason="missing fixture id",
                )
            )
            continue

        if not _is_live_status(status_short, inplay_config.live_statuses):
            rejected.append(
                ApiSportsRejectedInplayFixture(
                    fixture_id=fixture_id,
                    match=match,
                    status_short=status_short,
                    reason="fixture is not currently live",
                )
            )
            continue

        parsed = _parse_fixture(item, fixture_id=fixture_id)
        fixtures.append(parsed)

        if len(fixtures) >= inplay_config.max_fixtures:
            break

    warnings = ["INPLAY_ONLY", "NO_REAL_STAKING", "NO_MODEL_EDGE_VALIDATION"]
    if not fixtures:
        warnings.append("NO_INPLAY_FIXTURES_FOUND")

    return ApiSportsInplayFixtures(
        status="READY",
        mode=INPLAY_MODE,
        real_staking_enabled=False,
        generated_at_utc=_utc_now(),
        config=inplay_config,
        fixtures=tuple(fixtures),
        rejected=tuple(rejected),
        warnings=tuple(warnings),
        errors=(),
    )


def fetch_api_sports_inplay_fixtures(
    *,
    api_key: str,
    base_url: str = "https://v3.football.api-sports.io",
    league: int | None = None,
    config: ApiSportsInplayFixturesConfig | None = None,
) -> ApiSportsInplayFixtures:
    inplay_config = config or ApiSportsInplayFixturesConfig.from_env()

    if not api_key:
        raise ApiSportsInplayFixturesError("Missing API-Sports key.")

    params: dict[str, object] = {
        "live": inplay_config.live,
        "timezone": inplay_config.timezone,
    }

    if league is not None:
        params["league"] = league

    payload = _api_get(base_url, "/fixtures", params, api_key)

    return build_api_sports_inplay_fixtures_from_payload(
        payload,
        config=inplay_config,
    )


def write_api_sports_inplay_fixtures(
    *,
    output_path: str | Path,
    api_key: str | None = None,
    base_url: str = "https://v3.football.api-sports.io",
    league: int | None = None,
    sample: bool = False,
    config: ApiSportsInplayFixturesConfig | None = None,
) -> ApiSportsInplayFixtures:
    inplay_config = config or ApiSportsInplayFixturesConfig.from_env()

    if sample:
        result = build_api_sports_inplay_fixtures_from_payload(
            sample_inplay_fixtures_payload(),
            config=inplay_config,
        )
    else:
        key = api_key or _api_key_from_env_or_dotenv()
        result = fetch_api_sports_inplay_fixtures(
            api_key=key,
            base_url=base_url,
            league=league,
            config=inplay_config,
        )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)

    return result


def sample_inplay_fixtures_payload() -> dict[str, object]:
    return {
        "response": [
            {
                "fixture": {
                    "id": 9001,
                    "date": "2026-04-28T19:00:00+00:00",
                    "status": {
                        "long": "First Half",
                        "short": "1H",
                        "elapsed": 37,
                    },
                },
                "league": {
                    "id": 39,
                    "name": "Premier League",
                    "country": "England",
                },
                "teams": {
                    "home": {"name": "Sample Home"},
                    "away": {"name": "Sample Away"},
                },
                "goals": {
                    "home": 1,
                    "away": 0,
                },
                "score": {
                    "halftime": {
                        "home": 1,
                        "away": 0,
                    }
                },
            },
            {
                "fixture": {
                    "id": 9002,
                    "date": "2026-04-28T20:00:00+00:00",
                    "status": {
                        "long": "Not Started",
                        "short": "NS",
                        "elapsed": None,
                    },
                },
                "league": {
                    "id": 140,
                    "name": "La Liga",
                    "country": "Spain",
                },
                "teams": {
                    "home": {"name": "Not Live Home"},
                    "away": {"name": "Not Live Away"},
                },
                "goals": {
                    "home": None,
                    "away": None,
                },
            },
        ]
    }


def _parse_fixture(item: Mapping[str, Any], *, fixture_id: int) -> ApiSportsInplayFixture:
    fixture = item.get("fixture") if isinstance(item.get("fixture"), Mapping) else {}
    teams = item.get("teams") if isinstance(item.get("teams"), Mapping) else {}
    league = item.get("league") if isinstance(item.get("league"), Mapping) else {}
    goals = item.get("goals") if isinstance(item.get("goals"), Mapping) else {}
    score = item.get("score") if isinstance(item.get("score"), Mapping) else {}
    status = fixture.get("status") if isinstance(fixture.get("status"), Mapping) else {}

    home_team = _team_name(teams.get("home")) or "Unknown home"
    away_team = _team_name(teams.get("away")) or "Unknown away"

    goals_home = _optional_int(goals.get("home"))
    goals_away = _optional_int(goals.get("away"))

    score_home, score_away = _score_pair(score, goals_home, goals_away)

    status_short = _optional_text(status.get("short"))

    return ApiSportsInplayFixture(
        fixture_id=fixture_id,
        match=f"{home_team} vs {away_team}",
        home_team=home_team,
        away_team=away_team,
        league_name=_optional_text(league.get("name")),
        league_id=_optional_int(league.get("id")),
        country=_optional_text(league.get("country")),
        kickoff_utc=_optional_text(fixture.get("date")),
        elapsed=_optional_int(status.get("elapsed")),
        status_short=status_short,
        status_long=_optional_text(status.get("long")),
        score_home=score_home,
        score_away=score_away,
        goals_home=goals_home,
        goals_away=goals_away,
        live=_is_live_status(status_short, DEFAULT_LIVE_STATUSES),
    )


def _score_pair(
    score: Mapping[str, Any],
    goals_home: int | None,
    goals_away: int | None,
) -> tuple[int | None, int | None]:
    current_home = goals_home
    current_away = goals_away

    for key in ("fulltime", "extratime", "penalty", "halftime"):
        value = score.get(key)
        if isinstance(value, Mapping):
            home = _optional_int(value.get("home"))
            away = _optional_int(value.get("away"))
            if home is not None:
                current_home = home
            if away is not None:
                current_away = away

    return current_home, current_away


def _api_get(base_url: str, path: str, params: Mapping[str, object], api_key: str) -> Mapping[str, Any]:
    query = urllib.parse.urlencode({key: value for key, value in params.items() if value is not None})
    url = f"{base_url.rstrip('/')}{path}"
    if query:
        url = f"{url}?{query}"

    request = urllib.request.Request(
        url,
        headers={
            "x-apisports-key": api_key,
            "Accept": "application/json",
        },
        method="GET",
    )

    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except Exception as exc:
        raise ApiSportsInplayFixturesError(f"API-Sports request failed: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise ApiSportsInplayFixturesError("API-Sports returned a non-object payload.")

    errors = payload.get("errors")
    if errors:
        raise ApiSportsInplayFixturesError(f"API-Sports returned errors: {errors}")

    return payload


def _api_key_from_env_or_dotenv() -> str:
    names = (
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    )

    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()

    env_path = Path(".env")
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            if key.strip() in names:
                return value.strip().strip('"').strip("'")

    return ""


def _fixture_id(item: Mapping[str, Any]) -> int | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        return _optional_int(fixture.get("id"))
    return _optional_int(item.get("fixture_id") or item.get("id"))


def _status_short(item: Mapping[str, Any]) -> str | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        status = fixture.get("status")
        if isinstance(status, Mapping):
            return _optional_text(status.get("short"))
    return _optional_text(item.get("status_short"))


def _match_name(item: Mapping[str, Any]) -> str | None:
    direct = _optional_text(item.get("match"))
    if direct:
        return direct

    teams = item.get("teams")
    if isinstance(teams, Mapping):
        home = _team_name(teams.get("home"))
        away = _team_name(teams.get("away"))
        if home and away:
            return f"{home} vs {away}"

    fixture_id = _fixture_id(item)
    return f"Fixture {fixture_id}" if fixture_id is not None else None


def _team_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return _optional_text(value.get("name") or value.get("team") or value.get("label"))
    return _optional_text(value)


def _is_live_status(status_short: str | None, live_statuses: Sequence[str]) -> bool:
    if not status_short:
        return False
    return status_short.upper().strip() in {item.upper().strip() for item in live_statuses}


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsInplayFixturesError(f"{name} must be an integer.") from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
