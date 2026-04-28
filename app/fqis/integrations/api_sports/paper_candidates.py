
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PAPER_CANDIDATES_MODE = "PAPER_CANDIDATES"


@dataclass(frozen=True)
class ApiSportsPaperCandidateConfig:
    default_bookmaker: str = "PaperBook"
    max_candidates: int = 20
    min_odds: float = 1.01
    max_odds: float = 50.0

    @classmethod
    def from_env(cls) -> "ApiSportsPaperCandidateConfig":
        return cls(
            default_bookmaker=os.getenv("APISPORTS_PAPER_DEFAULT_BOOKMAKER", "PaperBook"),
            max_candidates=_env_int("APISPORTS_PAPER_CANDIDATES_MAX", 20),
            min_odds=_env_float("APISPORTS_PAPER_CANDIDATES_MIN_ODDS", 1.01),
            max_odds=_env_float("APISPORTS_PAPER_CANDIDATES_MAX_ODDS", 50.0),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "default_bookmaker": self.default_bookmaker,
            "max_candidates": self.max_candidates,
            "min_odds": self.min_odds,
            "max_odds": self.max_odds,
        }


@dataclass(frozen=True)
class ApiSportsPaperCandidateRecord:
    match: str
    market: str
    selection: str
    odds: float
    model_probability: float
    bookmaker: str
    kickoff_utc: str | None = None
    reason: str | None = None
    source: str | None = None

    @classmethod
    def from_mapping(
        cls,
        payload: Mapping[str, Any],
        *,
        config: ApiSportsPaperCandidateConfig,
        source: str | None = None,
    ) -> "ApiSportsPaperCandidateRecord":
        match = _match_name(payload)
        market = _required_str(payload.get("market"), "market")
        selection = _required_str(payload.get("selection") or payload.get("pick"), "selection")
        odds = _required_float(payload.get("odds") or payload.get("book_odds") or payload.get("decimal_odds"), "odds")
        probability = _probability(payload.get("model_probability") or payload.get("probability") or payload.get("prob"))

        if probability is None:
            raise ApiSportsPaperCandidatesError("Candidate field is required: model_probability")

        if odds < config.min_odds or odds > config.max_odds:
            raise ApiSportsPaperCandidatesError(
                f"Candidate odds outside paper bounds: odds={odds}, min={config.min_odds}, max={config.max_odds}"
            )

        if probability <= 0.0 or probability >= 1.0:
            raise ApiSportsPaperCandidatesError("Candidate model_probability must be between 0 and 1.")

        return cls(
            match=match,
            market=market,
            selection=selection,
            odds=odds,
            model_probability=probability,
            bookmaker=_optional_str(payload.get("bookmaker")) or config.default_bookmaker,
            kickoff_utc=_optional_str(payload.get("kickoff_utc")),
            reason=_optional_str(payload.get("reason")) or "Paper candidate generated for preview only.",
            source=source,
        )

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
        }


@dataclass(frozen=True)
class ApiSportsRejectedPaperCandidate:
    raw: Mapping[str, Any]
    reason: str

    def to_dict(self) -> dict[str, object]:
        return {
            "raw": dict(self.raw),
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ApiSportsPaperCandidates:
    status: str
    mode: str
    real_staking_enabled: bool
    generated_at_utc: str
    source_path: str | None
    config: ApiSportsPaperCandidateConfig
    candidates: tuple[ApiSportsPaperCandidateRecord, ...]
    rejected: tuple[ApiSportsRejectedPaperCandidate, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "mode": self.mode,
            "real_staking_enabled": self.real_staking_enabled,
            "generated_at_utc": self.generated_at_utc,
            "source_path": self.source_path,
            "config": self.config.to_dict(),
            "candidates": [item.to_dict() for item in self.candidates],
            "rejected": [item.to_dict() for item in self.rejected],
            "errors": list(self.errors),
        }


class ApiSportsPaperCandidatesError(RuntimeError):
    pass


def build_api_sports_paper_candidates(
    *,
    source_path: str | Path | None = None,
    candidates: Sequence[Mapping[str, Any]] | None = None,
    sample: bool = False,
    config: ApiSportsPaperCandidateConfig | None = None,
) -> ApiSportsPaperCandidates:
    candidate_config = config or ApiSportsPaperCandidateConfig.from_env()
    errors: list[str] = []
    raw_candidates: list[tuple[Mapping[str, Any], str | None]] = []

    if source_path is not None:
        source = Path(source_path)
        raw_candidates.extend((item, str(source)) for item in _load_candidate_source(source))

    if candidates is not None:
        raw_candidates.extend((item, "inline") for item in candidates if isinstance(item, Mapping))

    if sample:
        raw_candidates.extend((item, "sample") for item in sample_paper_candidate_inputs())

    accepted: list[ApiSportsPaperCandidateRecord] = []
    rejected: list[ApiSportsRejectedPaperCandidate] = []

    for raw, source in raw_candidates:
        try:
            accepted.append(
                ApiSportsPaperCandidateRecord.from_mapping(
                    raw,
                    config=candidate_config,
                    source=source,
                )
            )
        except ApiSportsPaperCandidatesError as exc:
            rejected.append(ApiSportsRejectedPaperCandidate(raw=raw, reason=str(exc)))

    accepted = accepted[: candidate_config.max_candidates]

    return ApiSportsPaperCandidates(
        status="READY" if not errors else "BLOCKED",
        mode=PAPER_CANDIDATES_MODE,
        real_staking_enabled=False,
        generated_at_utc=_utc_now(),
        source_path=str(source_path) if source_path is not None else None,
        config=candidate_config,
        candidates=tuple(accepted),
        rejected=tuple(rejected),
        errors=tuple(errors),
    )


def write_api_sports_paper_candidates(
    *,
    source_path: str | Path | None = None,
    candidates: Sequence[Mapping[str, Any]] | None = None,
    output_path: str | Path | None = None,
    sample: bool = False,
    config: ApiSportsPaperCandidateConfig | None = None,
) -> ApiSportsPaperCandidates:
    result = build_api_sports_paper_candidates(
        source_path=source_path,
        candidates=candidates,
        sample=sample,
        config=config,
    )
    target = Path(output_path) if output_path is not None else default_paper_candidates_path()
    _write_json_atomic(target, result.to_dict())
    return result


def sample_paper_candidate_inputs() -> list[dict[str, object]]:
    return [
        {
            "match": "Arsenal vs Everton",
            "market": "Total Goals",
            "selection": "Over 2.5",
            "odds": 1.92,
            "model_probability": 0.568,
            "bookmaker": "PaperBook",
            "reason": "Test paper: profil offensif, edge positif.",
        },
        {
            "match": "Lyon vs Nantes",
            "market": "Draw No Bet",
            "selection": "Lyon DNB",
            "odds": 1.89,
            "model_probability": 0.535,
            "bookmaker": "PaperBook",
            "reason": "Test paper: signal interessant mais edge faible.",
        },
        {
            "match": "Inter vs Torino",
            "market": "1X2",
            "selection": "Inter Win",
            "odds": 1.42,
            "model_probability": 0.68,
            "bookmaker": "PaperBook",
            "reason": "Test paper: cote trop basse, rejet attendu.",
        },
        {
            "match": "Real Sociedad vs Valencia",
            "market": "Both Teams To Score",
            "selection": "BTTS Yes",
            "odds": 2.05,
            "model_probability": 0.545,
            "bookmaker": "PaperBook",
            "reason": "Test paper: value theorique elevee.",
        },
    ]


def default_paper_candidates_path() -> Path:
    return Path(os.getenv("APISPORTS_PAPER_CANDIDATES_PATH", "data/pipeline/api_sports/paper_candidates.json"))


def _load_candidate_source(path: Path) -> list[Mapping[str, Any]]:
    if not path.exists():
        raise ApiSportsPaperCandidatesError(f"Paper candidate source does not exist: {path}")
    if not path.is_file():
        raise ApiSportsPaperCandidatesError(f"Paper candidate source is not a file: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8-sig"))
    except json.JSONDecodeError as exc:
        raise ApiSportsPaperCandidatesError(f"Paper candidate source is invalid JSON: {path}") from exc

    return _extract_candidate_mappings(payload)


def _extract_candidate_mappings(payload: Any) -> list[Mapping[str, Any]]:
    if isinstance(payload, Mapping):
        for key in ("candidates", "paper_candidates", "bets", "items"):
            value = payload.get(key)
            if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
                return [item for item in value if isinstance(item, Mapping)]

        if {"match", "market", "selection"} <= set(payload):
            return [payload]

        return []

    if isinstance(payload, Sequence) and not isinstance(payload, (str, bytes, bytearray)):
        return [item for item in payload if isinstance(item, Mapping)]

    return []


def _match_name(payload: Mapping[str, Any]) -> str:
    direct = _optional_str(payload.get("match") or payload.get("event"))
    if direct is not None:
        return direct

    fixture = payload.get("fixture")
    if isinstance(fixture, str):
        text = fixture.strip()
        if text:
            return text

    home = _team_name(payload.get("home_team") or payload.get("home"))
    away = _team_name(payload.get("away_team") or payload.get("away"))

    if home and away:
        return f"{home} vs {away}"

    raise ApiSportsPaperCandidatesError("Candidate field is required: match")


def _team_name(value: Any) -> str | None:
    if isinstance(value, Mapping):
        return _optional_str(value.get("name") or value.get("team") or value.get("label"))
    return _optional_str(value)


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsPaperCandidatesError(f"Candidate field is required: {field}")
    return result


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_float(value: Any, field: str) -> float:
    result = _float_or_none(value)
    if result is None:
        raise ApiSportsPaperCandidatesError(f"Candidate field is required: {field}")
    return result


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _probability(value: Any) -> float | None:
    result = _float_or_none(value)
    if result is None:
        return None
    if result > 1.0 and result <= 100.0:
        return result / 100.0
    return result


def _env_float(name: str, default: float) -> float:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return float(raw)
    except ValueError as exc:
        raise ApiSportsPaperCandidatesError(f"{name} must be a float.") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsPaperCandidatesError(f"{name} must be an integer.") from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
