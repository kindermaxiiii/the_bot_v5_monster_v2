from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta

from app.vnext.ops.models import DedupOrigin, DedupRecord
from app.vnext.pipeline.models import PublishableMatchResult


@dataclass(slots=True)
class Deduper:
    cooldown_seconds: int
    _last_seen: dict[str, datetime] = None  # type: ignore[assignment]
    _persisted_keys: set[str] = None  # type: ignore[assignment]
    _persistent_state_loaded: bool = False

    def __post_init__(self) -> None:
        if self._last_seen is None:
            self._last_seen = {}
        if self._persisted_keys is None:
            self._persisted_keys = set()

    def _key(self, result: PublishableMatchResult) -> str:
        candidate = result.execution_candidate
        offer = result.selected_offer
        if candidate is None or offer is None:
            return ""
        return "|".join(
            [
                str(result.fixture_id),
                result.governed_public_status,
                candidate.template_key,
                str(offer.bookmaker_id),
                str(offer.line),
                f"{offer.odds_decimal:.2f}",
            ]
        )

    def is_duplicate(self, result: PublishableMatchResult, now: datetime) -> bool:
        key = self._key(result)
        if not key or key not in self._last_seen:
            return False
        return now - self._last_seen[key] < timedelta(seconds=self.cooldown_seconds)

    def duplicate_origin(self, result: PublishableMatchResult, now: datetime) -> DedupOrigin | None:
        if not self.is_duplicate(result, now):
            return None
        key = self._key(result)
        if key in self._persisted_keys:
            return "deduped_persistent"
        return "deduped_in_memory"

    def mark_seen(self, result: PublishableMatchResult, now: datetime) -> None:
        key = self._key(result)
        if key:
            self._last_seen[key] = now

    @property
    def persistent_state_loaded(self) -> bool:
        return self._persistent_state_loaded

    def cleanup_expired(self, now: datetime) -> None:
        cutoff = timedelta(seconds=self.cooldown_seconds)
        expired = [
            key
            for key, last_seen in self._last_seen.items()
            if now - last_seen >= cutoff
        ]
        for key in expired:
            self._last_seen.pop(key, None)
            self._persisted_keys.discard(key)

    def load_records(self, records: tuple[DedupRecord, ...], now: datetime) -> None:
        self._last_seen = {record.key: record.last_seen_utc for record in records}
        self._persisted_keys = {record.key for record in records}
        self._persistent_state_loaded = True
        self.cleanup_expired(now)

    def snapshot_records(self, now: datetime) -> tuple[DedupRecord, ...]:
        self.cleanup_expired(now)
        return tuple(
            DedupRecord(key=key, last_seen_utc=last_seen)
            for key, last_seen in sorted(self._last_seen.items())
        )
