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
    source: str = "public_payload.v1"


@dataclass(slots=True, frozen=True)
class PublicMessageBundle:
    publish_channel: PublishChannel
    payloads: tuple[PublicMatchPayload, ...]
    source: str = "public_bundle.v1"
