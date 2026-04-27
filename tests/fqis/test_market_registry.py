import pytest

from app.fqis.contracts.enums import MarketFamily, MarketSide, TeamRole
from app.fqis.contracts.registry import MARKET_REGISTRY, get_market_definition


def test_registry_contains_core_markets() -> None:
    assert "TEAM_TOTAL_AWAY_UNDER_1_5" in MARKET_REGISTRY
    assert "BTTS_NO" in MARKET_REGISTRY
    assert "RESULT_HOME" in MARKET_REGISTRY


def test_get_market_definition_team_total_away_under() -> None:
    definition = get_market_definition("TEAM_TOTAL_AWAY_UNDER_1_5")
    assert definition.family == MarketFamily.TEAM_TOTAL_AWAY
    assert definition.side == MarketSide.UNDER
    assert definition.team_role == TeamRole.AWAY
    assert definition.line_required is True


def test_unknown_market_key_raises() -> None:
    with pytest.raises(KeyError):
        get_market_definition("UNKNOWN_MARKET")