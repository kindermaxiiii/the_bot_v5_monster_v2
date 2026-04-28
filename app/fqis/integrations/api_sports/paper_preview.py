
from __future__ import annotations

import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


PAPER_MODE = "PAPER_ONLY"


@dataclass(frozen=True)
class ApiSportsPaperPreviewConfig:
    max_stake_units: float = 0.05
    min_bet_edge: float = 0.05
    min_watch_edge: float = 0.01
    max_bets: int = 5

    @classmethod
    def from_env(cls) -> "ApiSportsPaperPreviewConfig":
        return cls(
            max_stake_units=_env_float("APISPORTS_PAPER_MAX_STAKE_UNITS", 0.05),
            min_bet_edge=_env_float("APISPORTS_PAPER_MIN_BET_EDGE", 0.05),
            min_watch_edge=_env_float("APISPORTS_PAPER_MIN_WATCH_EDGE", 0.01),
            max_bets=_env_int("APISPORTS_PAPER_MAX_BETS", 5),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "max_stake_units": self.max_stake_units,
            "min_bet_edge": self.min_bet_edge,
            "min_watch_edge": self.min_watch_edge,
            "max_bets": self.max_bets,
        }


@dataclass(frozen=True)
class ApiSportsPaperCandidate:
    match: str
    market: str
    selection: str
    odds: float
    model_probability: float
    bookmaker: str | None = None
    kickoff_utc: str | None = None
    reason: str | None = None

    @classmethod
    def from_mapping(cls, payload: Mapping[str, Any]) -> "ApiSportsPaperCandidate":
        match = _required_str(payload.get("match") or payload.get("fixture") or payload.get("event"), "match")
        market = _required_str(payload.get("market"), "market")
        selection = _required_str(payload.get("selection") or payload.get("pick"), "selection")
        odds = _required_float(payload.get("odds") or payload.get("book_odds") or payload.get("decimal_odds"), "odds")
        probability = _probability(payload.get("model_probability") or payload.get("probability") or payload.get("prob"))

        if probability is None:
            raise ApiSportsPaperPreviewError("Candidate field is required: model_probability")

        return cls(
            match=match,
            market=market,
            selection=selection,
            odds=odds,
            model_probability=probability,
            bookmaker=_optional_str(payload.get("bookmaker")),
            kickoff_utc=_optional_str(payload.get("kickoff_utc")),
            reason=_optional_str(payload.get("reason")),
        )


@dataclass(frozen=True)
class ApiSportsPaperPick:
    match: str
    market: str
    selection: str
    bookmaker: str | None
    kickoff_utc: str | None
    odds: float
    model_probability: float
    fair_odds: float | None
    edge: float | None
    edge_pct: float | None
    stake_units: float
    decision: str
    status: str
    reason: str
    warnings: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "match": self.match,
            "market": self.market,
            "selection": self.selection,
            "bookmaker": self.bookmaker,
            "kickoff_utc": self.kickoff_utc,
            "odds": self.odds,
            "model_probability": self.model_probability,
            "fair_odds": self.fair_odds,
            "edge": self.edge,
            "edge_pct": self.edge_pct,
            "stake_units": self.stake_units,
            "decision": self.decision,
            "status": self.status,
            "reason": self.reason,
            "warnings": list(self.warnings),
        }


@dataclass(frozen=True)
class ApiSportsPaperPreview:
    status: str
    mode: str
    real_staking_enabled: bool
    max_stake_units: float
    generated_at_utc: str
    bets: tuple[ApiSportsPaperPick, ...]
    watchlist: tuple[ApiSportsPaperPick, ...]
    rejected: tuple[ApiSportsPaperPick, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "mode": self.mode,
            "real_staking_enabled": self.real_staking_enabled,
            "max_stake_units": self.max_stake_units,
            "generated_at_utc": self.generated_at_utc,
            "bets": [item.to_dict() for item in self.bets],
            "watchlist": [item.to_dict() for item in self.watchlist],
            "rejected": [item.to_dict() for item in self.rejected],
            "errors": list(self.errors),
        }


class ApiSportsPaperPreviewError(RuntimeError):
    pass


def build_api_sports_paper_preview(
    *,
    candidates: Sequence[Mapping[str, Any]] | None = None,
    candidates_path: str | Path | None = None,
    sample: bool = False,
    config: ApiSportsPaperPreviewConfig | None = None,
) -> ApiSportsPaperPreview:
    preview_config = config or ApiSportsPaperPreviewConfig.from_env()
    errors: list[str] = []

    raw_candidates: list[Mapping[str, Any]] = []

    if candidates_path is not None:
        raw_candidates.extend(_load_candidates(candidates_path))

    if candidates is not None:
        raw_candidates.extend(item for item in candidates if isinstance(item, Mapping))

    if sample:
        raw_candidates.extend(sample_paper_candidates())

    bets: list[ApiSportsPaperPick] = []
    watchlist: list[ApiSportsPaperPick] = []
    rejected: list[ApiSportsPaperPick] = []

    for index, raw in enumerate(raw_candidates):
        try:
            candidate = ApiSportsPaperCandidate.from_mapping(raw)
            pick = _score_candidate(candidate, preview_config)
        except ApiSportsPaperPreviewError as exc:
            rejected.append(
                ApiSportsPaperPick(
                    match=str(raw.get("match") or raw.get("fixture") or f"candidate-{index + 1}"),
                    market=str(raw.get("market") or "UNKNOWN"),
                    selection=str(raw.get("selection") or raw.get("pick") or "UNKNOWN"),
                    bookmaker=_optional_str(raw.get("bookmaker")),
                    kickoff_utc=_optional_str(raw.get("kickoff_utc")),
                    odds=_float_or_zero(raw.get("odds") or raw.get("book_odds") or raw.get("decimal_odds")),
                    model_probability=_float_or_zero(raw.get("model_probability") or raw.get("probability") or raw.get("prob")),
                    fair_odds=None,
                    edge=None,
                    edge_pct=None,
                    stake_units=0.0,
                    decision="REJECTED",
                    status="PAPER_ONLY",
                    reason=f"Invalid candidate: {exc}",
                    warnings=("NO_REAL_MONEY_VALIDATION",),
                )
            )
            continue

        if pick.decision == "PAPER_BET":
            bets.append(pick)
        elif pick.decision == "WATCHLIST":
            watchlist.append(pick)
        else:
            rejected.append(pick)

    bets = sorted(bets, key=lambda item: item.edge or 0.0, reverse=True)[: preview_config.max_bets]
    watchlist = sorted(watchlist, key=lambda item: item.edge or 0.0, reverse=True)

    return ApiSportsPaperPreview(
        status="READY" if not errors else "BLOCKED",
        mode=PAPER_MODE,
        real_staking_enabled=False,
        max_stake_units=preview_config.max_stake_units,
        generated_at_utc=_utc_now(),
        bets=tuple(bets),
        watchlist=tuple(watchlist),
        rejected=tuple(rejected),
        errors=tuple(errors),
    )


def write_api_sports_paper_preview(
    *,
    candidates: Sequence[Mapping[str, Any]] | None = None,
    candidates_path: str | Path | None = None,
    output_path: str | Path | None = None,
    sample: bool = False,
    config: ApiSportsPaperPreviewConfig | None = None,
) -> ApiSportsPaperPreview:
    preview = build_api_sports_paper_preview(
        candidates=candidates,
        candidates_path=candidates_path,
        sample=sample,
        config=config,
    )
    target = Path(output_path) if output_path is not None else default_paper_preview_path()
    _write_json_atomic(target, preview.to_dict())
    return preview


def sample_paper_candidates() -> list[dict[str, object]]:
    return [
        {
            "match": "Team A vs Team B",
            "market": "Total Goals",
            "selection": "Over 2.5",
            "odds": 1.92,
            "model_probability": 0.568,
            "bookmaker": "SampleBook",
            "reason": "Paper sample: profile offensif coherent, edge theorique positif.",
        },
        {
            "match": "Team C vs Team D",
            "market": "Draw No Bet",
            "selection": "Team C DNB",
            "odds": 1.89,
            "model_probability": 0.535,
            "bookmaker": "SampleBook",
            "reason": "Paper sample: signal interessant mais edge trop faible pour bet principal.",
        },
        {
            "match": "Team E vs Team F",
            "market": "1X2",
            "selection": "Team E Win",
            "odds": 1.42,
            "model_probability": 0.68,
            "bookmaker": "SampleBook",
            "reason": "Paper sample: cote trop basse, pas de value.",
        },
    ]


def default_paper_preview_path() -> Path:
    return Path(os.getenv("APISPORTS_PAPER_PREVIEW_PATH", "data/pipeline/api_sports/paper_preview.json"))


def _score_candidate(
    candidate: ApiSportsPaperCandidate,
    config: ApiSportsPaperPreviewConfig,
) -> ApiSportsPaperPick:
    warnings = ["PAPER_ONLY", "NO_REAL_MONEY_VALIDATION"]

    if candidate.odds <= 1.0:
        return _rejected(candidate, "Odds invalides ou non exploitables.", warnings)

    if candidate.model_probability <= 0.0 or candidate.model_probability >= 1.0:
        return _rejected(candidate, "Probabilit- mod-le invalide.", warnings)

    fair_odds = round(1.0 / candidate.model_probability, 4)
    edge = candidate.odds * candidate.model_probability - 1.0
    edge_pct = round(edge * 100.0, 2)

    if edge >= config.min_bet_edge:
        return ApiSportsPaperPick(
            match=candidate.match,
            market=candidate.market,
            selection=candidate.selection,
            bookmaker=candidate.bookmaker,
            kickoff_utc=candidate.kickoff_utc,
            odds=candidate.odds,
            model_probability=round(candidate.model_probability, 6),
            fair_odds=fair_odds,
            edge=round(edge, 6),
            edge_pct=edge_pct,
            stake_units=config.max_stake_units,
            decision="PAPER_BET",
            status="PAPER_ONLY",
            reason=candidate.reason or "Edge theorique positif. Observation uniquement.",
            warnings=tuple(warnings + ["MICRO_STAKE_MAX"]),
        )

    if edge >= config.min_watch_edge:
        return ApiSportsPaperPick(
            match=candidate.match,
            market=candidate.market,
            selection=candidate.selection,
            bookmaker=candidate.bookmaker,
            kickoff_utc=candidate.kickoff_utc,
            odds=candidate.odds,
            model_probability=round(candidate.model_probability, 6),
            fair_odds=fair_odds,
            edge=round(edge, 6),
            edge_pct=edge_pct,
            stake_units=0.0,
            decision="WATCHLIST",
            status="PAPER_ONLY",
            reason=candidate.reason or "Signal interessant mais edge insuffisant pour paper bet principal.",
            warnings=tuple(warnings),
        )

    return ApiSportsPaperPick(
        match=candidate.match,
        market=candidate.market,
        selection=candidate.selection,
        bookmaker=candidate.bookmaker,
        kickoff_utc=candidate.kickoff_utc,
        odds=candidate.odds,
        model_probability=round(candidate.model_probability, 6),
        fair_odds=fair_odds,
        edge=round(edge, 6),
        edge_pct=edge_pct,
        stake_units=0.0,
        decision="REJECTED",
        status="PAPER_ONLY",
        reason=candidate.reason or "Pas assez de value selon les seuils paper.",
        warnings=tuple(warnings),
    )


def _rejected(
    candidate: ApiSportsPaperCandidate,
    reason: str,
    warnings: Sequence[str],
) -> ApiSportsPaperPick:
    return ApiSportsPaperPick(
        match=candidate.match,
        market=candidate.market,
        selection=candidate.selection,
        bookmaker=candidate.bookmaker,
        kickoff_utc=candidate.kickoff_utc,
        odds=candidate.odds,
        model_probability=candidate.model_probability,
        fair_odds=None,
        edge=None,
        edge_pct=None,
        stake_units=0.0,
        decision="REJECTED",
        status="PAPER_ONLY",
        reason=reason,
        warnings=tuple(warnings),
    )


def _load_candidates(path: str | Path) -> list[Mapping[str, Any]]:
    target = Path(path)
    if not target.exists():
        raise ApiSportsPaperPreviewError(f"Candidates path does not exist: {target}")
    if not target.is_file():
        raise ApiSportsPaperPreviewError(f"Candidates path is not a file: {target}")

    payload = json.loads(target.read_text(encoding="utf-8-sig"))

    if isinstance(payload, Mapping):
        value = payload.get("candidates") or payload.get("bets") or payload.get("items")
    else:
        value = payload

    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        raise ApiSportsPaperPreviewError("Candidates payload must be a list or an object containing candidates.")

    return [item for item in value if isinstance(item, Mapping)]


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsPaperPreviewError(f"Candidate field is required: {field}")
    return result


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _required_float(value: Any, field: str) -> float:
    result = _float_or_none(value)
    if result is None:
        raise ApiSportsPaperPreviewError(f"Candidate field is required: {field}")
    return result


def _float_or_none(value: Any) -> float | None:
    if value is None:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _float_or_zero(value: Any) -> float:
    result = _float_or_none(value)
    return result if result is not None else 0.0


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
        raise ApiSportsPaperPreviewError(f"{name} must be a float.") from exc


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsPaperPreviewError(f"{name} must be an integer.") from exc


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
