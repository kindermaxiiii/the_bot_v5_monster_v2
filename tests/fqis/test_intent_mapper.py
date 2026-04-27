from app.fqis.contracts.core import StatisticalThesis
from app.fqis.contracts.enums import MarketFamily, MarketSide, ThesisKey
from app.fqis.thesis.intent_mapper import map_thesis_to_market_intents


def test_low_away_scoring_hazard_maps_to_multiple_vehicles() -> None:
    thesis = StatisticalThesis(
        event_id=201,
        thesis_key=ThesisKey.LOW_AWAY_SCORING_HAZARD,
        strength=0.78,
        confidence=0.81,
    )

    intents = map_thesis_to_market_intents(thesis)

    assert len(intents) >= 2
    assert any(
        intent.family == MarketFamily.TEAM_TOTAL_AWAY and intent.side == MarketSide.UNDER
        for intent in intents
    )
    assert any(
        intent.family == MarketFamily.BTTS and intent.side == MarketSide.NO
        for intent in intents
    )


def test_open_game_maps_to_over_and_btts_yes() -> None:
    thesis = StatisticalThesis(
        event_id=202,
        thesis_key=ThesisKey.OPEN_GAME,
        strength=0.84,
        confidence=0.79,
    )

    intents = map_thesis_to_market_intents(thesis)

    assert any(
        intent.family == MarketFamily.MATCH_TOTAL and intent.side == MarketSide.OVER
        for intent in intents
    )
    assert any(
        intent.family == MarketFamily.BTTS and intent.side == MarketSide.YES
        for intent in intents
    )