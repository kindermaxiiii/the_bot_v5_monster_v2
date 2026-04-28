from __future__ import annotations

import json
import os
import urllib.parse
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping, Sequence

LIVE_ODDS_COVERAGE_DIAGNOSTICS_MODE = "LIVE_ODDS_COVERAGE_DIAGNOSTICS"


@dataclass(frozen=True)
class ApiSportsLiveOddsCoverageDiagnosticsConfig:
    min_odds: float = 1.25
    max_odds: float = 8.0

    @classmethod
    def from_env(cls) -> "ApiSportsLiveOddsCoverageDiagnosticsConfig":
        return cls(
            min_odds=_env_float("APISPORTS_INPLAY_LIVE_ODDS_MIN_ODDS", 1.25),
            max_odds=_env_float("APISPORTS_INPLAY_LIVE_ODDS_MAX_ODDS", 8.0),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "min_odds": self.min_odds,
            "max_odds": self.max_odds,
        }


@dataclass(frozen=True)
class ApiSportsLiveOddsCoverageDiagnostics:
    status: str
    mode: str
    ready: bool
    real_staking_enabled: bool
    generated_at_utc: str
    fixtures_source_path: str | None
    odds_source_path: str | None
    candidates_source_path: str | None
    config: ApiSportsLiveOddsCoverageDiagnosticsConfig
    metrics: Mapping[str, int]
    warnings: tuple[str, ...]
    errors: tuple[str, ...]

    @property
    def summary(self) -> dict[str, int]:
        return {
            "warnings_total": len(self.warnings),
            "errors_total": len(self.errors),
            "live_fixtures_total": self.metrics.get("live_fixtures_total", 0),
            "live_odds_fixtures_total": self.metrics.get("live_odds_fixtures_total", 0),
            "matched_fixture_odds_total": self.metrics.get("matched_fixture_odds_total", 0),
            "candidates_total": self.metrics.get("candidates_total", 0),
        }

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "mode": self.mode,
            "ready": self.ready,
            "real_staking_enabled": self.real_staking_enabled,
            "generated_at_utc": self.generated_at_utc,
            "fixtures_source_path": self.fixtures_source_path,
            "odds_source_path": self.odds_source_path,
            "candidates_source_path": self.candidates_source_path,
            "config": self.config.to_dict(),
            "metrics": dict(self.metrics),
            "summary": self.summary,
            "warnings": list(self.warnings),
            "errors": list(self.errors),
        }


class ApiSportsLiveOddsCoverageDiagnosticsError(RuntimeError):
    pass


def build_api_sports_live_odds_coverage_diagnostics(
    *,
    fixtures_payload: Any | None = None,
    odds_payload: Any | None = None,
    candidates_payload: Any | None = None,
    fixtures_path: str | Path | None = None,
    odds_path: str | Path | None = None,
    candidates_path: str | Path | None = None,
    config: ApiSportsLiveOddsCoverageDiagnosticsConfig | None = None,
) -> ApiSportsLiveOddsCoverageDiagnostics:
    diagnostics_config = config or ApiSportsLiveOddsCoverageDiagnosticsConfig.from_env()

    if fixtures_path is not None:
        fixtures_payload = _load_json(Path(fixtures_path), "fixtures")

    if odds_path is not None:
        odds_payload = _load_json(Path(odds_path), "live odds")

    if candidates_path is not None:
        candidates_payload = _load_json(Path(candidates_path), "candidates")

    metrics = _coverage_metrics(
        fixtures_payload=fixtures_payload,
        odds_payload=odds_payload,
        candidates_payload=candidates_payload,
        config=diagnostics_config,
    )

    warnings = _coverage_warnings(metrics)

    return ApiSportsLiveOddsCoverageDiagnostics(
        status="READY",
        mode=LIVE_ODDS_COVERAGE_DIAGNOSTICS_MODE,
        ready=True,
        real_staking_enabled=False,
        generated_at_utc=_utc_now(),
        fixtures_source_path=str(fixtures_path) if fixtures_path is not None else None,
        odds_source_path=str(odds_path) if odds_path is not None else None,
        candidates_source_path=str(candidates_path) if candidates_path is not None else None,
        config=diagnostics_config,
        metrics=metrics,
        warnings=warnings,
        errors=(),
    )


def fetch_api_sports_live_odds_payload(
    *,
    api_key: str,
    base_url: str = "https://v3.football.api-sports.io",
    fixture: int | None = None,
) -> Mapping[str, Any]:
    if not api_key:
        raise ApiSportsLiveOddsCoverageDiagnosticsError("Missing API-Sports key.")

    params: dict[str, object] = {}
    if fixture is not None:
        params["fixture"] = fixture

    return _api_get(base_url, "/odds/live", params, api_key)


def write_api_sports_live_odds_coverage_diagnostics(
    *,
    output_path: str | Path,
    fixtures_path: str | Path | None = None,
    odds_path: str | Path | None = None,
    candidates_path: str | Path | None = None,
    odds_payload: Any | None = None,
    markdown_path: str | Path | None = None,
    config: ApiSportsLiveOddsCoverageDiagnosticsConfig | None = None,
) -> ApiSportsLiveOddsCoverageDiagnostics:
    result = build_api_sports_live_odds_coverage_diagnostics(
        fixtures_path=fixtures_path,
        odds_path=odds_path,
        candidates_path=candidates_path,
        odds_payload=odds_payload,
        config=config,
    )

    target = Path(output_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_text(json.dumps(result.to_dict(), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(target)

    if markdown_path is not None:
        md_target = Path(markdown_path)
        md_target.parent.mkdir(parents=True, exist_ok=True)
        md_tmp = md_target.with_suffix(md_target.suffix + ".tmp")
        md_tmp.write_text(render_api_sports_live_odds_coverage_diagnostics_markdown(result), encoding="utf-8")
        md_tmp.replace(md_target)

    return result


def render_api_sports_live_odds_coverage_diagnostics_markdown(
    result: ApiSportsLiveOddsCoverageDiagnostics,
) -> str:
    metrics = result.metrics

    lines = [
        "# FQIS API-Sports Live Odds Coverage Diagnostics",
        "",
        "## Summary",
        "",
        f"- Status: **{result.status}**",
        f"- Mode: **{result.mode}**",
        f"- Real staking enabled: **{str(result.real_staking_enabled).lower()}**",
        f"- Live fixtures: **{metrics.get('live_fixtures_total', 0)}**",
        f"- Live odds fixtures: **{metrics.get('live_odds_fixtures_total', 0)}**",
        f"- Matched fixture odds: **{metrics.get('matched_fixture_odds_total', 0)}**",
        f"- Candidates: **{metrics.get('candidates_total', 0)}**",
        f"- Generated at UTC: `{result.generated_at_utc}`",
        "",
        "> OBSERVATION ONLY. This report explains live odds coverage gaps. It is not a betting signal.",
        "",
        "## Coverage Metrics",
        "",
        "| Metric | Value |",
        "|---|---:|",
    ]

    for key in sorted(metrics):
        lines.append(f"| `{key}` | {metrics[key]} |")

    lines.extend(
        [
            "",
            "## Warnings",
            "",
        ]
    )

    if result.warnings:
        for warning in result.warnings:
            lines.append(f"- `{warning}`")
    else:
        lines.append("No warnings.")

    lines.extend(
        [
            "",
            "## Operator Interpretation",
            "",
            "- If live fixtures exist but matched fixture odds are zero, API-Sports has fixtures live but no tradable live odds for those fixtures.",
            "- If live odds fixtures are blocked or values are suspended, the market exists but is not currently tradable.",
            "- If supported markets are zero, the live feed only exposes markets outside the FQIS core market whitelist.",
            "- If candidates are zero while supported tradable values exist, inspect the candidate runner filters.",
            "- Do not place real stakes from this diagnostic.",
            "",
        ]
    )

    return "\n".join(lines)


def sample_live_odds_coverage_payloads() -> tuple[Mapping[str, Any], Mapping[str, Any], Mapping[str, Any]]:
    fixtures_payload = {
        "fixtures": [
            {
                "fixture_id": 9001,
                "match": "Sample Home vs Sample Away",
                "status_short": "2H",
                "elapsed": 63,
                "live": True,
            },
            {
                "fixture_id": 9002,
                "match": "Blocked Home vs Blocked Away",
                "status_short": "2H",
                "elapsed": 87,
                "live": True,
            },
        ]
    }

    odds_payload = {
        "response": [
            {
                "fixture": {
                    "id": 9001,
                    "status": {
                        "long": "Second Half",
                        "elapsed": 63,
                    },
                },
                "status": {
                    "blocked": False,
                    "finished": False,
                },
                "odds": [
                    {
                        "name": "Match Winner",
                        "values": [
                            {"value": "1", "odd": "1.90", "suspended": False},
                            {"value": "X", "odd": "3.40", "suspended": False},
                            {"value": "2", "odd": "4.20", "suspended": False},
                        ],
                    },
                    {
                        "name": "Which team will score the 2nd goal?",
                        "values": [
                            {"value": "1", "odd": "0", "suspended": True},
                            {"value": "No goal", "odd": "1.004", "suspended": True},
                        ],
                    },
                ],
            },
            {
                "fixture": {
                    "id": 9002,
                    "status": {
                        "long": "Second Half",
                        "elapsed": 87,
                    },
                },
                "status": {
                    "blocked": True,
                    "finished": False,
                },
                "odds": [
                    {
                        "name": "Both Teams To Score",
                        "values": [
                            {"value": "Yes", "odd": "1.80", "suspended": True},
                            {"value": "No", "odd": "2.00", "suspended": True},
                        ],
                    }
                ],
            },
        ]
    }

    candidates_payload = {
        "summary": {
            "candidates_total": 3,
        },
        "candidates": [
            {"fixture_id": 9001, "market": "1X2", "selection": "Home", "odds": 1.9},
            {"fixture_id": 9001, "market": "1X2", "selection": "Draw", "odds": 3.4},
            {"fixture_id": 9001, "market": "1X2", "selection": "Away", "odds": 4.2},
        ],
    }

    return fixtures_payload, odds_payload, candidates_payload


def _coverage_metrics(
    *,
    fixtures_payload: Any | None,
    odds_payload: Any | None,
    candidates_payload: Any | None,
    config: ApiSportsLiveOddsCoverageDiagnosticsConfig,
) -> dict[str, int]:
    live_fixture_ids = _fixture_ids_from_fixtures_payload(fixtures_payload)
    live_odds_items = _live_odds_items(odds_payload)
    live_odds_fixture_ids = {_fixture_id_from_live_odds_item(item) for item in live_odds_items}
    live_odds_fixture_ids.discard(None)

    metrics = {
        "live_fixtures_total": len(live_fixture_ids),
        "live_odds_fixtures_total": len(live_odds_items),
        "matched_fixture_odds_total": len(live_fixture_ids & live_odds_fixture_ids),
        "live_fixtures_without_live_odds_total": len(live_fixture_ids - live_odds_fixture_ids),
        "live_odds_not_in_live_fixtures_total": len(live_odds_fixture_ids - live_fixture_ids),
        "blocked_fixtures_total": 0,
        "finished_fixtures_total": 0,
        "markets_total": 0,
        "supported_markets_total": 0,
        "unsupported_markets_total": 0,
        "values_total": 0,
        "suspended_values_total": 0,
        "invalid_odds_total": 0,
        "below_min_odds_total": 0,
        "above_max_odds_total": 0,
        "supported_tradable_values_total": 0,
        "candidates_total": _candidate_count(candidates_payload),
    }

    for item in live_odds_items:
        status = item.get("status") if isinstance(item.get("status"), Mapping) else {}

        if bool(status.get("blocked")):
            metrics["blocked_fixtures_total"] += 1

        if bool(status.get("finished")):
            metrics["finished_fixtures_total"] += 1

        odds = item.get("odds")
        if not isinstance(odds, Sequence) or isinstance(odds, (str, bytes, bytearray)):
            continue

        for market in odds:
            if not isinstance(market, Mapping):
                continue

            metrics["markets_total"] += 1

            canonical_market = _canonical_market_name(_optional_text(market.get("name")))
            if canonical_market is None:
                metrics["unsupported_markets_total"] += 1
            else:
                metrics["supported_markets_total"] += 1

            values = market.get("values")
            if not isinstance(values, Sequence) or isinstance(values, (str, bytes, bytearray)):
                continue

            for value in values:
                if not isinstance(value, Mapping):
                    continue

                metrics["values_total"] += 1

                suspended = bool(value.get("suspended"))
                if suspended:
                    metrics["suspended_values_total"] += 1

                odd = _optional_float(value.get("odd"))
                if odd is None or odd <= 0:
                    metrics["invalid_odds_total"] += 1
                    continue

                if odd < config.min_odds:
                    metrics["below_min_odds_total"] += 1
                    continue

                if odd > config.max_odds:
                    metrics["above_max_odds_total"] += 1
                    continue

                if suspended:
                    continue

                if canonical_market is None:
                    continue

                if _canonical_selection(canonical_market, value) is None:
                    continue

                metrics["supported_tradable_values_total"] += 1

    return metrics


def _coverage_warnings(metrics: Mapping[str, int]) -> tuple[str, ...]:
    warnings: list[str] = [
        "LIVE_ODDS_COVERAGE_DIAGNOSTIC",
        "OBSERVATION_ONLY",
        "NO_REAL_STAKING",
        "NO_MODEL_EDGE_VALIDATION",
    ]

    if metrics.get("live_fixtures_total", 0) == 0:
        warnings.append("NO_INPLAY_FIXTURES_FOUND")

    if metrics.get("live_odds_fixtures_total", 0) == 0:
        warnings.append("NO_LIVE_ODDS_FEED_FIXTURES_FOUND")

    if (
        metrics.get("live_fixtures_total", 0) > 0
        and metrics.get("live_odds_fixtures_total", 0) > 0
        and metrics.get("matched_fixture_odds_total", 0) == 0
    ):
        warnings.append("NO_MATCHED_LIVE_ODDS_FOR_INPLAY_FIXTURES")

    if (
        metrics.get("live_odds_fixtures_total", 0) > 0
        and metrics.get("blocked_fixtures_total", 0) == metrics.get("live_odds_fixtures_total", 0)
    ):
        warnings.append("ALL_LIVE_ODDS_FIXTURES_BLOCKED")

    if (
        metrics.get("markets_total", 0) > 0
        and metrics.get("supported_markets_total", 0) == 0
    ):
        warnings.append("NO_SUPPORTED_LIVE_ODDS_MARKETS_FOUND")

    if (
        metrics.get("values_total", 0) > 0
        and metrics.get("suspended_values_total", 0) == metrics.get("values_total", 0)
    ):
        warnings.append("ALL_LIVE_ODDS_VALUES_SUSPENDED")

    if metrics.get("live_fixtures_without_live_odds_total", 0) > 0:
        warnings.append("SOME_INPLAY_FIXTURES_WITHOUT_LIVE_ODDS")

    if metrics.get("candidates_total", 0) == 0:
        warnings.append("NO_CANDIDATES_FOUND")

    if (
        metrics.get("supported_tradable_values_total", 0) > 0
        and metrics.get("candidates_total", 0) == 0
    ):
        warnings.append("SUPPORTED_TRADABLE_VALUES_EXIST_BUT_NO_CANDIDATES")

    return tuple(warnings)


def _fixture_ids_from_fixtures_payload(payload: Any | None) -> set[int]:
    fixtures: Any = []

    if isinstance(payload, Mapping):
        if isinstance(payload.get("fixtures"), Sequence) and not isinstance(payload.get("fixtures"), (str, bytes, bytearray)):
            fixtures = payload.get("fixtures")
        elif isinstance(payload.get("response"), Sequence) and not isinstance(payload.get("response"), (str, bytes, bytearray)):
            fixtures = payload.get("response")
    elif isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        fixtures = payload

    ids: set[int] = set()

    if not isinstance(fixtures, Sequence) or isinstance(fixtures, (str, bytes, bytearray)):
        return ids

    for item in fixtures:
        if not isinstance(item, Mapping):
            continue

        fixture_id = _fixture_id_from_fixture_item(item)
        if fixture_id is not None:
            ids.add(fixture_id)

    return ids


def _fixture_id_from_fixture_item(item: Mapping[str, Any]) -> int | None:
    direct = _optional_int(item.get("fixture_id") or item.get("id"))
    if direct is not None:
        return direct

    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        return _optional_int(fixture.get("id"))

    return None


def _live_odds_items(payload: Any | None) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        response = payload.get("response")
        if isinstance(response, Sequence) and not isinstance(response, (str, bytes, bytearray)):
            return [item for item in response if isinstance(item, Mapping)]

        if "odds" in payload:
            return [payload]

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return [item for item in payload if isinstance(item, Mapping)]

    return []


def _fixture_id_from_live_odds_item(item: Mapping[str, Any]) -> int | None:
    fixture = item.get("fixture")
    if isinstance(fixture, Mapping):
        return _optional_int(fixture.get("id"))

    return _optional_int(item.get("fixture_id") or item.get("id"))


def _candidate_count(payload: Any | None) -> int:
    if isinstance(payload, Mapping):
        summary = payload.get("summary")
        if isinstance(summary, Mapping):
            count = _optional_int(summary.get("candidates_total"))
            if count is not None:
                return count

        candidates = payload.get("candidates")
        if isinstance(candidates, Sequence) and not isinstance(candidates, (str, bytes, bytearray)):
            return len([item for item in candidates if isinstance(item, Mapping)])

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return len([item for item in payload if isinstance(item, Mapping)])

    return 0


def _canonical_market_name(name: str | None) -> str | None:
    if not name:
        return None

    normalized = " ".join(name.lower().replace("-", " ").split())

    if normalized in {"match winner", "1x2", "winner"}:
        return "1X2"

    if normalized in {"goals over/under", "goals over under", "total goals", "over/under", "over under"}:
        return "Total Goals"

    if normalized in {"both teams score", "both teams to score", "btts"}:
        return "Both Teams To Score"

    return None


def _canonical_selection(market: str, value: Mapping[str, Any]) -> str | None:
    raw = _optional_text(value.get("value"))
    handicap = _optional_text(value.get("handicap"))

    if raw is None:
        return None

    normalized = " ".join(raw.strip().lower().split())

    if market == "1X2":
        mapping = {
            "1": "Home",
            "home": "Home",
            "x": "Draw",
            "draw": "Draw",
            "2": "Away",
            "away": "Away",
        }
        return mapping.get(normalized)

    if market == "Total Goals":
        if normalized in {"over 2.5", "under 2.5"}:
            return " ".join(part.capitalize() if idx == 0 else part for idx, part in enumerate(normalized.split()))

        if handicap == "2.5" and normalized in {"over", "under"}:
            return f"{normalized.capitalize()} 2.5"

        return None

    if market == "Both Teams To Score":
        mapping = {
            "yes": "Yes",
            "no": "No",
        }
        return mapping.get(normalized)

    return None


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
        raise ApiSportsLiveOddsCoverageDiagnosticsError(f"API-Sports request failed: {exc}") from exc

    if not isinstance(payload, Mapping):
        raise ApiSportsLiveOddsCoverageDiagnosticsError("API-Sports returned a non-object payload.")

    errors = payload.get("errors")
    if errors:
        raise ApiSportsLiveOddsCoverageDiagnosticsError(f"API-Sports returned errors: {errors}")

    return payload


def _load_json(path: Path, label: str) -> Any:
    if not path.exists():
        raise ApiSportsLiveOddsCoverageDiagnosticsError(f"Missing {label} JSON file: {path}")

    try:
        return json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ApiSportsLiveOddsCoverageDiagnosticsError(f"Invalid {label} JSON file: {path}") from exc


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


def _optional_float(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default

    try:
        return float(raw)
    except ValueError as exc:
        raise ApiSportsLiveOddsCoverageDiagnosticsError(f"{name} must be numeric.") from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
