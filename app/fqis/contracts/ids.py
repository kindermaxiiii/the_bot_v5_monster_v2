from __future__ import annotations

from dataclasses import dataclass


@dataclass(slots=True, frozen=True)
class CanonicalEventId:
    value: int


@dataclass(slots=True, frozen=True)
class CanonicalTeamId:
    value: int


@dataclass(slots=True, frozen=True)
class CanonicalBookmakerId:
    value: int
    