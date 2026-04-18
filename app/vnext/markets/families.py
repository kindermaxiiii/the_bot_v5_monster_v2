from __future__ import annotations

from app.vnext.markets.models import FamilyMaturity, MarketFamily


FAMILY_MATURITY: dict[MarketFamily, FamilyMaturity] = {
    "OU_FT": "APPROVED",
    "BTTS": "APPROVED",
    "TEAM_TOTAL": "APPROVED",
    "RESULT": "PROBATION",
}


def family_maturity(family: MarketFamily) -> FamilyMaturity:
    return FAMILY_MATURITY[family]
