
from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any


LIVE_MODE = "LIVE_PAPER_CANDIDATES"
PAPER_ONLY_WARNING = "NO_MODEL_EDGE_VALIDATION"


class ApiSportsLivePaperCandidatesError(RuntimeError):
    pass


@dataclass(frozen=True)
class ApiSportsLivePaperConfig:
    date: str
    timezone: str = "Europe/Paris"
    max_fixtures: int = 10
    max_candidates: int = 20
    max_bookmakers: int = 3
    min_odds: float = 1.25
    max_odds: float = 5.0
    baseline_probability_discount: float = 0.98

    @classmethod
    def from_env(cls) -> "ApiSportsLivePaperConfig":
        return cls(
            date=os.getenv("APISPORTS_LIVE_PAPER_DATE", date.today().isoformat()),
            timezone=os.getenv("APISPORTS_LIVE_PAPER_TIMEZONE", "Europe/Paris"),
            max_fixtures=_env_int("APISPORTS_LIVE_PAPER_MAX_FIXTURES", 10),
            max_candidates=_env_int("APISPORTS_LIVE_PAPER_MAX_CANDIDATES", 20),
            max_bookmakers=_env_int("APISPORTS_LIVE_PAPER_MAX_BOOKMAKERS", 3),
            min_odds=_env_float("APISPORTS_LIVE_PAPER_MIN_ODDS", 1.25),
            max_odds=_env_float("APISPORTS_LIVE_PAPER_MAX_ODDS", 5.0),
            baseline_probability_discount=_env_float("APISPORTS_LIVE_PAPER_PROB_DISCOUNT", 0.98),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "date": self.date,
            "timezone": self.timezone,
            "max_fixtures": self.max_fixtures,
            "max_candidates": self.max_candidates,
            "max_bookmakers": self.max_bookmakers,
            "min_odds": self.min_odds,
            "max_odds": self.max_odds,
            "baseline_probability_discount": self.baseline_probability_discount,
        }


@dataclass(frozen=True)
class ApiSportsLivePaperCandidate:
    match: str
    market: str
    selection: str
    odds: float
    model_probability: float
    bookmaker: str
    kickoff_utc: str | None
    reason: str
    source: str
    fixture_id: int | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "match": self.match,
            "market": self.market,
            "selection": self.selection,
            "odds": self.odds,
            "model_probability": self.model_probability,
            "bookmaker": self.bookmaker,
            "kickoff_utc": self.kickoff_utc,
            "reason": self.reason,
            "source": self.source,
            "fixture_id": self.fixture_id,
        }


@dataclass(frozen=True)
class ApiSportsLivePaperCandidates:
    status: str
    mode: str
    real_staking_enabled: bool
    config: ApiSportsLivePaperConfig
    candidates: tuple[ApiSportsLivePaperCandidate, ...]
    rejected: tuple[dict[str, object], ...]
    warnings: tuple[str, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "mode": self.mode,
            "real_staking_enabled": self.real_staking_enabled,
            "config": self.config.to_dict(),
            "candidates": [item.to_dict() for item in self.candidates],
            "rejected": list(self.rejected),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
        }


def build_api_sports_live_paper_candidates_from_payloads(
    *,
    odds_payloads: Sequence[Mapping[str, Any]],
    fixtures: Mapping[int, Mapping[str, Any]] | None = None,
    config: ApiSportsLivePaperConfig | None = None,
) -> ApiSportsLivePaperCandidates:
    live_config = config or ApiSportsLivePaperConfig.from_env()
    fixture_map = fixtures or {}

    candidates: list[ApiSportsLivePaperCandidate] = []
    rejected: list[dict[str, object]] = []

    for payload in odds_payloads:
        response = payload.get("response", [])
        if not isinstance(response, Sequence) or isinstance(response, (str, bytes, bytearray)):
            continue

        for odds_item in response:
            if not isinstance(odds_item, Mapping):
                continue

            fixture_id = _fixture_id(odds_item)
            fixture_meta = fixture_map.get(fixture_id or -1, {})
            match = _match_name(odds_item, fixture_meta)
            kickoff_utc = _kickoff_utc(odds_item, fixture_meta)

            bookmakers = odds_item.get("bookmakers", [])
            if not isinstance(bookmakers, Sequence) or isinstance(bookmakers, (str, bytes, bytearray)):
                continue

            for bookmaker in list(bookmakers)[: live_config.max_bookmakers]:
                if not isinstance(bookmaker, Mapping):
                    continue

                bookmaker_name = _text(bookmaker.get("name")) or "ApiSportsBook"
                bets = bookmaker.get("bets", [])
                if not isinstance(bets, Sequence) or isinstance(bets, (str, bytes, bytearray)):
                    continue

                for bet in bets:
                    if not isinstance(bet, Mapping):
                        continue

                    market = _market_name(bet)
                    if market is None:
                        continue

                    values = bet.get("values", [])
                    if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                        continue

                    for value in values:
                        if not isinstance(value, Mapping):
                            continue

                        selection = _text(value.get("value"))
                        odds = _float_or_none(value.get("odd") or value.get("odds"))

                        if not selection or odds is None:
                            rejected.append({"reason": "missing selection or odds", "raw": dict(value)})
                            continue

                        canonical_selection = _canonical_live_selection(market, selection)
                        if canonical_selection is None:
                            rejected.append(
                                {
                                    "reason": "unsupported live paper market selection",
                                    "odds": odds,
                                    "market": market,
                                    "selection": selection,
                                }
                            )
                            continue

                        selection = canonical_selection

                        if odds < live_config.min_odds or odds > live_config.max_odds:
                            rejected.append(
                                {
                                    "reason": "odds outside live paper bounds",
                                    "odds": odds,
                                    "market": market,
                                    "selection": selection,
                                }
                            )
                            continue

                        implied_probability = 1.0 / odds
                        baseline_probability = implied_probability * live_config.baseline_probability_discount
                        baseline_probability = min(max(baseline_probability, 0.001), 0.999)

                        candidates.append(
                            ApiSportsLivePaperCandidate(
                                match=match,
                                market=market,
                                selection=selection,
                                odds=round(odds, 4),
                                model_probability=round(baseline_probability, 6),
                                bookmaker=bookmaker_name,
                                kickoff_utc=kickoff_utc,
                                reason=(
                                    "Live paper input only: bookmaker-implied baseline probability "
                                    "with safety discount. No model edge validation."
                                ),
                                source="api-sports-live-paper",
                                fixture_id=fixture_id,
                            )
                        )

                        if len(candidates) >= live_config.max_candidates:
                            return _result(live_config, candidates, rejected)

    return _result(live_config, candidates, rejected)


def fetch_api_sports_live_paper_candidates(
    *,
    api_key: str,
    base_url: str = "https://v3.football.api-sports.io",
    league: int | None = None,
    season: int | None = None,
    fixture: int | None = None,
    config: ApiSportsLivePaperConfig | None = None,
) -> ApiSportsLivePaperCandidates:
    live_config = config or ApiSportsLivePaperConfig.from_env()

    if not api_key:
        raise ApiSportsLivePaperCandidatesError("Missing API-Sports key.")

    fixture_map: dict[int, Mapping[str, Any]] = {}
    odds_payloads: list[Mapping[str, Any]] = []

    if fixture is not None:
        odds_params: dict[str, object] = {"fixture": fixture}
        if league is not None:
            odds_params["league"] = league
        if season is not None:
            odds_params["season"] = season

        odds_payloads.append(_api_get(base_url, "/odds", odds_params, api_key))
    else:
        fixture_params: dict[str, object] = {
            "date": live_config.date,
            "timezone": live_config.timezone,
        }
        if league is not None:
            fixture_params["league"] = league
        if season is not None:
            fixture_params["season"] = season

        fixtures_payload = _api_get(base_url, "/fixtures", fixture_params, api_key)
        fixtures_response = fixtures_payload.get("response", [])

        if isinstance(fixtures_response, Sequence) and not isinstance(fixtures_response, (str, bytes, bytearray)):
            for item in list(fixtures_response)[: live_config.max_fixtures]:
                if not isinstance(item, Mapping):
                    continue

                fixture_id = _fixture_id(item)
                if fixture_id is None:
                    continue

                fixture_map[fixture_id] = item

                odds_params = {"fixture": fixture_id}
                if league is not None:
                    odds_params["league"] = league
                if season is not None:
                    odds_params["season"] = season

                odds_payloads.append(_api_get(base_url, "/odds", odds_params, api_key))

    return build_api_sports_live_paper_candidates_from_payloads(
        odds_payloads=odds_payloads,
        fixtures=fixture_map,
        config=live_config,
    )


def write_api_sports_live_paper_candidates(
    *,
    output_path: str | Path,
    api_key: str | None = None,
    base_url: str = "https://v3.football.api-sports.io",
    league: int | None = None,
    season: int | None = None,
    fixture: int | None = None,
    sample: bool = False,
    config: ApiSportsLivePaperConfig | None = None,
) -> ApiSportsLivePaperCandidates:
    live_config = config or ApiSportsLivePaperConfig.from_env()

    if sample:
        result = build_api_sports_live_paper_candidates_from_payloads(
            odds_payloads=[sample_odds_payload()],
            fixtures=sample_fixture_map(),
            config=live_config,
        )
    else:
        key = api_key or _api_key_from_env_or_dotenv()
        result = fetch_api_sports_live_paper_candidates(
            api_key=key,
            base_url=base_url,
            league=league,
            season=season,
            fixture=fixture,
            config=live_config,
        )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)
    return result


def sample_odds_payload() -> dict[str, object]:
    return {
        "response": [
            {
                "fixture": {"id": 1001, "date": "2026-04-28T19:00:00+00:00"},
                "bookmakers": [
                    {
                        "name": "LivePaperBook",
                        "bets": [
                            {
                                "name": "Match Winner",
                                "values": [
                                    {"value": "Home", "odd": "1.90"},
                                    {"value": "Draw", "odd": "3.40"},
                                    {"value": "Away", "odd": "4.20"},
                                ],
                            },
                            {
                                "name": "Goals Over/Under",
                                "values": [
                                    {"value": "Over 2.5", "odd": "1.95"},
                                    {"value": "Under 2.5", "odd": "1.85"},
                                ],
                            },
                            {
                                "name": "Both Teams Score",
                                "values": [
                                    {"value": "Yes", "odd": "1.82"},
                                    {"value": "No", "odd": "1.98"},
                                ],
                            },
                        ],
                    }
                ],
            }
        ]
    }


def sample_fixture_map() -> dict[int, Mapping[str, Any]]:
    return {
        1001: {
            "fixture": {"id": 1001, "date": "2026-04-28T19:00:00+00:00"},
            "teams": {
                "home": {"name": "Sample Home"},
                "away": {"name": "Sample Away"},
            },
        }
    }


def _result(
    config: ApiSportsLivePaperConfig,
    candidates: Sequence[ApiSportsLivePaperCandidate],
    rejected: Sequence[dict[str, object]],
) -> ApiSportsLivePaperCandidates:
    return ApiSportsLivePaperCandidates(
        status="READY",
        mode=LIVE_MODE,
        real_staking_enabled=False,
        config=config,
        candidates=tuple(candidates[: config.max_candidates]),
        rejected=tuple(rejected),
        warnings=(PAPER_ONLY_WARNING, "PAPER_ONLY", "NO_REAL_MONEY_VALIDATION"),
        errors=(),
    )


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
        raise ApiSportsLivePaperCandidatesError(f"API-Sports request failed: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise ApiSportsLivePaperCandidatesError("API-Sports returned a non-object payload.")

    errors = payload.get("errors")
    if errors:
        raise ApiSportsLivePaperCandidatesError(f"API-Sports returned errors: {errors}")

    return payload


def _fixture_id(payload: Mapping[str, Any]) -> int | None:
    fixture = payload.get("fixture")
    if isinstance(fixture, Mapping):
        return _int_or_none(fixture.get("id"))
    return _int_or_none(payload.get("fixture_id") or payload.get("id"))


def _match_name(odds_item: Mapping[str, Any], fixture_meta: Mapping[str, Any]) -> str:
    direct = _text(odds_item.get("match") or fixture_meta.get("match"))
    if direct:
        return direct

    teams = fixture_meta.get("teams")
    if isinstance(teams, Mapping):
        home = teams.get("home")
        away = teams.get("away")
        home_name = _team_name(home)
        away_name = _team_name(away)
        if home_name and away_name:
            return f"{home_name} vs {away_name}"

    fixture_id = _fixture_id(odds_item)
    return f"Fixture {fixture_id}" if fixture_id is not None else "Unknown fixture"


def _kickoff_utc(odds_item: Mapping[str, Any], fixture_meta: Mapping[str, Any]) -> str | None:
    for container in (fixture_meta.get("fixture"), odds_item.get("fixture")):
        if isinstance(container, Mapping):
            value = _text(container.get("date"))
            if value:
                return value
    return None


def _team_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return _text(value.get("name") or value.get("team") or value.get("label"))
    return _text(value)


def _market_name(bet: Mapping[str, Any]) -> str | None:
    name = _text(bet.get("name"))
    if not name:
        return None

    normalized = " ".join(name.lower().replace("-", " ").split())

    if normalized in {"match winner", "1x2"}:
        return "1X2"

    if normalized in {"goals over/under", "goals over under", "total goals"}:
        return "Total Goals"

    if normalized in {"both teams score", "both teams to score"}:
        return "Both Teams To Score"

    return None


def _canonical_live_selection(market: str, selection: str) -> str | None:
    normalized = " ".join(selection.strip().split()).lower()

    if market == "1X2":
        mapping = {
            "home": "Home",
            "draw": "Draw",
            "away": "Away",
        }
        return mapping.get(normalized)

    if market == "Total Goals":
        mapping = {
            "over 2.5": "Over 2.5",
            "under 2.5": "Under 2.5",
        }
        return mapping.get(normalized)

    if market == "Both Teams To Score":
        mapping = {
            "yes": "Yes",
            "no": "No",
        }
        return mapping.get(normalized)

    return None

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


def _text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ApiSportsLivePaperCandidatesError(f"{name} must be a float.") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsLivePaperCandidatesError(f"{name} must be an integer.") from exc
