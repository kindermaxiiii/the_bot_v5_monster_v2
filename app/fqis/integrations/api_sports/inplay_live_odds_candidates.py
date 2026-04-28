from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

MODE = "INPLAY_LIVE_ODDS_CANDIDATES"
SOURCE = "api-sports-inplay-live-odds"
DEFAULT_BOOKMAKER = "API-Sports Live"


@dataclass(frozen=True)
class ApiSportsInplayLiveOddsConfig:
    max_candidates: int = 100
    min_odds: float = 1.25
    max_odds: float = 8.0
    baseline_probability_discount: float = 0.98
    require_main_values: bool = True
    reject_suspended: bool = True

    @classmethod
    def from_env(cls) -> "ApiSportsInplayLiveOddsConfig":
        return cls(
            max_candidates=_env_int("APISPORTS_INPLAY_LIVE_ODDS_MAX_CANDIDATES", 100),
            min_odds=_env_float("APISPORTS_INPLAY_LIVE_ODDS_MIN_ODDS", 1.25),
            max_odds=_env_float("APISPORTS_INPLAY_LIVE_ODDS_MAX_ODDS", 8.0),
            baseline_probability_discount=_env_float("APISPORTS_INPLAY_LIVE_ODDS_PROB_DISCOUNT", 0.98),
            require_main_values=_env_bool("APISPORTS_INPLAY_LIVE_ODDS_REQUIRE_MAIN", True),
            reject_suspended=_env_bool("APISPORTS_INPLAY_LIVE_ODDS_REJECT_SUSPENDED", True),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "max_candidates": self.max_candidates,
            "min_odds": self.min_odds,
            "max_odds": self.max_odds,
            "baseline_probability_discount": self.baseline_probability_discount,
            "require_main_values": self.require_main_values,
            "reject_suspended": self.reject_suspended,
        }


@dataclass(frozen=True)
class ApiSportsInplayLiveOddsCandidate:
    match: str
    market: str
    selection: str
    odds: float
    model_probability: float
    bookmaker: str
    kickoff_utc: str | None
    reason: str
    source: str
    fixture_id: int | None
    elapsed: int | None
    status_short: str | None

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
            "elapsed": self.elapsed,
            "status_short": self.status_short,
        }


@dataclass(frozen=True)
class ApiSportsInplayLiveOddsCandidates:
    status: str
    mode: str
    real_staking_enabled: bool
    generated_at_utc: str
    config: ApiSportsInplayLiveOddsConfig
    candidates: tuple[ApiSportsInplayLiveOddsCandidate, ...]
    rejected: tuple[dict[str, object], ...]
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
            "candidates": [item.to_dict() for item in self.candidates],
            "rejected": list(self.rejected),
            "warnings": list(self.warnings),
            "errors": list(self.errors),
            "summary": {
                "candidates_total": len(self.candidates),
                "rejected_total": len(self.rejected),
                "warnings_total": len(self.warnings),
                "errors_total": len(self.errors),
            },
        }


class ApiSportsInplayLiveOddsCandidatesError(RuntimeError):
    pass


def build_api_sports_inplay_live_odds_candidates_from_payload(
    payload: Mapping[str, Any],
    *,
    config: ApiSportsInplayLiveOddsConfig | None = None,
) -> ApiSportsInplayLiveOddsCandidates:
    live_config = config or ApiSportsInplayLiveOddsConfig.from_env()
    response = payload.get("response", [])

    if not isinstance(response, Sequence) or isinstance(response, (str, bytes, bytearray)):
        raise ApiSportsInplayLiveOddsCandidatesError("API-Sports live odds payload response must be a list.")

    candidates: list[ApiSportsInplayLiveOddsCandidate] = []
    rejected: list[dict[str, object]] = []

    for item in response:
        if not isinstance(item, Mapping):
            continue

        fixture_id = _fixture_id(item)
        match = _match_name(item)
        kickoff_utc = _kickoff_utc(item)
        elapsed = _elapsed(item)
        status_short = _status_short(item)

        if _fixture_live_odds_blocked_or_finished(item):
            rejected.append(
                {
                    "fixture_id": fixture_id,
                    "match": match,
                    "reason": "live odds fixture blocked or finished",
                    "status_short": status_short,
                }
            )
            continue

        for bet, bookmaker_name in _iter_live_bets(item):
            market = _market_name(bet)
            if market is None:
                continue

            values = bet.get("values")
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                continue

            for value_item in values:
                if not isinstance(value_item, Mapping):
                    continue

                selection_raw = _selection_text(value_item)
                odds = _odds(value_item)

                if selection_raw is None or odds is None:
                    rejected.append(
                        {
                            "fixture_id": fixture_id,
                            "match": match,
                            "market": market,
                            "selection": selection_raw,
                            "reason": "missing selection or odds",
                        }
                    )
                    continue

                if live_config.require_main_values and "main" in value_item and not _bool(value_item.get("main")):
                    rejected.append(
                        {
                            "fixture_id": fixture_id,
                            "match": match,
                            "market": market,
                            "selection": selection_raw,
                            "odds": odds,
                            "reason": "non-main live odds value",
                        }
                    )
                    continue

                if live_config.reject_suspended and _is_suspended(value_item, bet):
                    rejected.append(
                        {
                            "fixture_id": fixture_id,
                            "match": match,
                            "market": market,
                            "selection": selection_raw,
                            "odds": odds,
                            "reason": "suspended live odds value",
                        }
                    )
                    continue

                selection = _canonical_live_selection(market, selection_raw, value_item)
                if selection is None:
                    rejected.append(
                        {
                            "fixture_id": fixture_id,
                            "match": match,
                            "market": market,
                            "selection": selection_raw,
                            "odds": odds,
                            "reason": "unsupported in-play live odds market selection",
                        }
                    )
                    continue

                if odds < live_config.min_odds or odds > live_config.max_odds:
                    rejected.append(
                        {
                            "fixture_id": fixture_id,
                            "match": match,
                            "market": market,
                            "selection": selection,
                            "odds": odds,
                            "reason": "odds outside in-play live odds bounds",
                        }
                    )
                    continue

                baseline_probability = min(
                    max((1.0 / odds) * live_config.baseline_probability_discount, 0.001),
                    0.999,
                )

                candidates.append(
                    ApiSportsInplayLiveOddsCandidate(
                        match=match,
                        market=market,
                        selection=selection,
                        odds=round(odds, 4),
                        model_probability=round(baseline_probability, 6),
                        bookmaker=bookmaker_name or DEFAULT_BOOKMAKER,
                        kickoff_utc=kickoff_utc,
                        reason=(
                            "In-play live odds input only: bookmaker/live-feed implied baseline probability "
                            "with safety discount. No independent model edge validation."
                        ),
                        source=SOURCE,
                        fixture_id=fixture_id,
                        elapsed=elapsed,
                        status_short=status_short,
                    )
                )

                if len(candidates) >= live_config.max_candidates:
                    return _result(live_config, candidates, rejected)

    return _result(live_config, candidates, rejected)


def fetch_api_sports_inplay_live_odds_candidates(
    *,
    api_key: str,
    base_url: str = "https://v3.football.api-sports.io",
    fixture: int | None = None,
    league: int | None = None,
    bet: int | None = None,
    config: ApiSportsInplayLiveOddsConfig | None = None,
) -> ApiSportsInplayLiveOddsCandidates:
    live_config = config or ApiSportsInplayLiveOddsConfig.from_env()

    if not api_key:
        raise ApiSportsInplayLiveOddsCandidatesError("Missing API-Sports key.")

    params: dict[str, object] = {}
    if fixture is not None:
        params["fixture"] = fixture
    if league is not None:
        params["league"] = league
    if bet is not None:
        params["bet"] = bet

    payload = _api_get(base_url, "/odds/live", params, api_key)

    return build_api_sports_inplay_live_odds_candidates_from_payload(
        payload,
        config=live_config,
    )


def write_api_sports_inplay_live_odds_candidates(
    *,
    output_path: str | Path,
    api_key: str | None = None,
    base_url: str = "https://v3.football.api-sports.io",
    fixture: int | None = None,
    league: int | None = None,
    bet: int | None = None,
    sample: bool = False,
    config: ApiSportsInplayLiveOddsConfig | None = None,
) -> ApiSportsInplayLiveOddsCandidates:
    live_config = config or ApiSportsInplayLiveOddsConfig.from_env()

    if sample:
        result = build_api_sports_inplay_live_odds_candidates_from_payload(
            sample_inplay_live_odds_payload(),
            config=live_config,
        )
    else:
        key = api_key or _api_key_from_env_or_dotenv()
        result = fetch_api_sports_inplay_live_odds_candidates(
            api_key=key,
            base_url=base_url,
            fixture=fixture,
            league=league,
            bet=bet,
            config=live_config,
        )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)

    return result


def sample_inplay_live_odds_payload() -> dict[str, object]:
    return {
        "response": [
            {
                "fixture": {
                    "id": 9001,
                    "date": "2026-04-28T19:00:00+00:00",
                    "status": {
                        "short": "2H",
                        "elapsed": 63,
                    },
                },
                "teams": {
                    "home": {"name": "Sample Home"},
                    "away": {"name": "Sample Away"},
                },
                "status": {
                    "stopped": False,
                    "blocked": False,
                    "finished": False,
                },
                "odds": [
                    {
                        "id": 1,
                        "name": "Match Winner",
                        "values": [
                            {"value": "Home", "odd": "1.90", "main": True, "suspended": False},
                            {"value": "Draw", "odd": "3.40", "main": True, "suspended": False},
                            {"value": "Away", "odd": "4.20", "main": True, "suspended": False},
                        ],
                    },
                    {
                        "id": 5,
                        "name": "Goals Over/Under",
                        "values": [
                            {"value": "Over", "handicap": "2.5", "odd": "1.95", "main": True, "suspended": False},
                            {"value": "Under", "handicap": "2.5", "odd": "1.85", "main": True, "suspended": False},
                            {"value": "Over", "handicap": "1.5", "odd": "1.25", "main": True, "suspended": False},
                        ],
                    },
                    {
                        "id": 8,
                        "name": "Both Teams Score",
                        "values": [
                            {"value": "Yes", "odd": "1.82", "main": True, "suspended": False},
                            {"value": "No", "odd": "1.98", "main": True, "suspended": False},
                            {"value": "Yes", "odd": "2.10", "main": False, "suspended": False},
                        ],
                    },
                ],
            }
        ]
    }


def _result(
    config: ApiSportsInplayLiveOddsConfig,
    candidates: Sequence[ApiSportsInplayLiveOddsCandidate],
    rejected: Sequence[dict[str, object]],
) -> ApiSportsInplayLiveOddsCandidates:
    warnings = ["INPLAY_LIVE_ODDS_ONLY", "NO_REAL_STAKING", "NO_MODEL_EDGE_VALIDATION"]
    if not candidates:
        warnings.append("NO_INPLAY_LIVE_ODDS_FOUND")

    return ApiSportsInplayLiveOddsCandidates(
        status="READY",
        mode=MODE,
        real_staking_enabled=False,
        generated_at_utc=_utc_now(),
        config=config,
        candidates=tuple(candidates[: config.max_candidates]),
        rejected=tuple(rejected),
        warnings=tuple(warnings),
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
        raise ApiSportsInplayLiveOddsCandidatesError(f"API-Sports request failed: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise ApiSportsInplayLiveOddsCandidatesError("API-Sports returned a non-object payload.")

    errors = payload.get("errors")
    if errors:
        raise ApiSportsInplayLiveOddsCandidatesError(f"API-Sports returned errors: {errors}")

    return payload


def _iter_live_bets(item: Mapping[str, Any]) -> list[tuple[Mapping[str, Any], str]]:
    result: list[tuple[Mapping[str, Any], str]] = []

    bookmakers = item.get("bookmakers")
    if isinstance(bookmakers, Sequence) and not isinstance(bookmakers, (str, bytes, bytearray)):
        for bookmaker in bookmakers:
            if not isinstance(bookmaker, Mapping):
                continue

            bookmaker_name = _bookmaker_name(bookmaker)
            for key in ("bets", "odds"):
                bets = bookmaker.get(key)
                if isinstance(bets, Sequence) and not isinstance(bets, (str, bytes, bytearray)):
                    for bet in bets:
                        if isinstance(bet, Mapping):
                            result.append((bet, bookmaker_name))
        if result:
            return result

    for key in ("odds", "bets"):
        bets = item.get(key)
        if isinstance(bets, Sequence) and not isinstance(bets, (str, bytes, bytearray)):
            for bet in bets:
                if isinstance(bet, Mapping):
                    result.append((bet, _bookmaker_name(item)))

    if {"name", "values"} <= set(item):
        result.append((item, _bookmaker_name(item)))

    return result


def _fixture_live_odds_blocked_or_finished(item: Mapping[str, Any]) -> bool:
    status = item.get("status")
    if isinstance(status, Mapping):
        if _bool(status.get("blocked")) or _bool(status.get("finished")):
            return True
    return False


def _is_suspended(value_item: Mapping[str, Any], bet: Mapping[str, Any]) -> bool:
    for key in ("suspended", "blocked", "stopped"):
        if key in value_item and _bool(value_item.get(key)):
            return True
        if key in bet and _bool(bet.get(key)):
            return True
    return False


def _market_name(bet: Mapping[str, Any]) -> str | None:
    name = _optional_text(bet.get("name") or bet.get("label"))
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


def _canonical_live_selection(market: str, selection: str, value_item: Mapping[str, Any]) -> str | None:
    normalized = " ".join(selection.strip().split()).lower()
    handicap = _optional_text(value_item.get("handicap") or value_item.get("line"))

    if market == "1X2":
        return {
            "home": "Home",
            "draw": "Draw",
            "away": "Away",
        }.get(normalized)

    if market == "Total Goals":
        if normalized in {"over 2.5", "under 2.5"}:
            return normalized.title().replace("2.5", "2.5")

        if handicap == "2.5":
            if normalized == "over":
                return "Over 2.5"
            if normalized == "under":
                return "Under 2.5"

        return None

    if market == "Both Teams To Score":
        return {
            "yes": "Yes",
            "no": "No",
        }.get(normalized)

    return None


def _selection_text(value_item: Mapping[str, Any]) -> str | None:
    return _optional_text(value_item.get("value") or value_item.get("selection") or value_item.get("name"))


def _odds(value_item: Mapping[str, Any]) -> float | None:
    for key in ("odd", "odds", "price"):
        value = value_item.get(key)
        result = _float_or_none(value)
        if result is not None:
            return result
    return None


def _bookmaker_name(item: Mapping[str, Any]) -> str:
    direct = _optional_text(item.get("bookmaker") or item.get("bookmaker_name"))
    if direct:
        return direct

    bookmaker = item.get("bookmaker")
    if isinstance(bookmaker, Mapping):
        name = _optional_text(bookmaker.get("name"))
        if name:
            return name

    name = _optional_text(item.get("name"))
    if name and "values" not in item:
        return name

    return DEFAULT_BOOKMAKER


def _fixture_id(item: Mapping[str, Any]) -> int | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        return _int_or_none(fixture.get("id"))
    return _int_or_none(item.get("fixture_id") or item.get("id"))


def _match_name(item: Mapping[str, Any]) -> str:
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
    return f"Fixture {fixture_id}" if fixture_id is not None else "Unknown fixture"


def _kickoff_utc(item: Mapping[str, Any]) -> str | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        return _optional_text(fixture.get("date"))
    return _optional_text(item.get("kickoff_utc"))


def _elapsed(item: Mapping[str, Any]) -> int | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        status = fixture.get("status")
        if isinstance(status, Mapping):
            return _int_or_none(status.get("elapsed"))
    return _int_or_none(item.get("elapsed"))


def _status_short(item: Mapping[str, Any]) -> str | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        status = fixture.get("status")
        if isinstance(status, Mapping):
            return _optional_text(status.get("short"))
    return _optional_text(item.get("status_short"))


def _team_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return _optional_text(value.get("name") or value.get("team") or value.get("label"))
    return _optional_text(value)


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


def _optional_text(value: Any) -> str | None:
    if value is None:
        return None
    result = str(value).strip()
    return result or None


def _int_or_none(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if value is None:
        return False
    return str(value).strip().lower() in {"1", "true", "yes", "y"}


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y"}


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ApiSportsInplayLiveOddsCandidatesError(f"{name} must be a float.") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsInplayLiveOddsCandidatesError(f"{name} must be an integer.") from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
