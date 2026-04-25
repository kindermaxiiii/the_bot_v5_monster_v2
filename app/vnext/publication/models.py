from __future__ import annotations

from dataclasses import dataclass
from typing import Literal


PublicStatus = Literal["ELITE", "WATCHLIST"]
PublishChannel = Literal["ELITE", "WATCHLIST"]
ConfidenceBand = Literal["LOW", "MEDIUM", "HIGH"]


@dataclass(slots=True, frozen=True)
class PublicMatchPayload:
    fixture_id: int
    public_status: PublicStatus
    publish_channel: PublishChannel
    match_label: str
    competition_label: str
    market_label: str
    line_label: str
    bookmaker_label: str
    odds_label: str
    confidence_band: ConfidenceBand
    public_summary: str

    # Champs enrichis optionnels pour un rendu Discord plus propre.
    # Tous ont des defaults pour rester rétrocompatibles avec le builder existant.
    home_team_label: str = ""
    away_team_label: str = ""
    kickoff_label: str = ""
    live_score_label: str = ""
    live_minute_label: str = ""
    market_compact_label: str = ""
    badge_label: str = "LIVE"

    # Réservés pour une future montée en gamme en embed / logos.
    competition_logo_url: str | None = None
    home_logo_url: str | None = None
    away_logo_url: str | None = None

    source: str = "public_payload.v2"


@dataclass(slots=True, frozen=True)
class PublicMessageBundle:
    publish_channel: PublishChannel
    payloads: tuple[PublicMatchPayload, ...]
    source: str = "public_bundle.v2"