from __future__ import annotations

from dataclasses import dataclass, field

from app.vnext.governance.models import InternalMatchStatus, PublicMatchStatus
from app.vnext.selection.models import MatchMarketSelectionResult


@dataclass(slots=True, frozen=True)
class BoardEntry:
    fixture_id: int
    internal_status: InternalMatchStatus
    public_status: PublicMatchStatus
    board_score: float
    rank: int
    match_refusals: tuple[str, ...] = ()
    board_refusals: tuple[str, ...] = ()
    selection_result: MatchMarketSelectionResult | None = None
    source: str = "board_entry.v1"


@dataclass(slots=True, frozen=True)
class BoardSnapshot:
    entries: tuple[BoardEntry, ...]
    elite_count: int
    watchlist_count: int
    source_version: str = "board_snapshot.v1"
    notes: tuple[str, ...] = field(default_factory=tuple)
