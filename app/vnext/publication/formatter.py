from __future__ import annotations

from app.vnext.pipeline.models import PublishableMatchResult
from app.vnext.publication.models import ConfidenceBand, PublicMatchPayload, PublicStatus


def _confidence_band(score: float) -> ConfidenceBand:
    if score >= 0.75:
        return "HIGH"
    if score >= 0.60:
        return "MEDIUM"
    return "LOW"


def build_public_payload(result: PublishableMatchResult) -> PublicMatchPayload | None:
    if result.publish_status != "PUBLISH":
        return None
    if result.governed_public_status not in {"ELITE", "WATCHLIST"}:
        return None
    if result.execution_candidate is None or result.selected_offer is None:
        return None

    candidate = result.best_candidate.candidate if result.best_candidate else None
    template = candidate.line_template if candidate else None

    market_label = candidate.family if candidate else "UNKNOWN"
    line_label = template.label if template else "LINE_UNKNOWN"
    match_label = result.match_label
    competition_label = result.competition_label
    bookmaker_label = result.selected_offer.bookmaker_name
    odds_label = f"{result.selected_offer.odds_decimal:.2f}"
    confidence_band = _confidence_band(result.execution_candidate.quality.publishability_score)
    public_summary = f"{market_label} {line_label} @ {bookmaker_label} {odds_label}"

    return PublicMatchPayload(
        fixture_id=result.fixture_id,
        public_status=result.governed_public_status,  # type: ignore[arg-type]
        publish_channel=result.governed_public_status,  # type: ignore[arg-type]
        match_label=match_label,
        competition_label=competition_label,
        market_label=market_label,
        line_label=line_label,
        bookmaker_label=bookmaker_label,
        odds_label=odds_label,
        confidence_band=confidence_band,
        public_summary=public_summary,
    )
