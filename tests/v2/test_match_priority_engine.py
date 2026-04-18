from __future__ import annotations

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.contracts import MarketProjectionV2
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.prioritization.match_priority_engine import MatchPriorityEngine


def _projection(market_key: str, side: str, line: float | None) -> MarketProjectionV2:
    return MarketProjectionV2(
        market_key=market_key,
        side=side,
        line=line,
        bookmaker="bet365",
        odds_decimal=2.0,
        raw_probability=0.60,
        calibrated_probability=0.58,
        market_no_vig_probability=0.48,
        edge=0.10,
        expected_value=0.16,
        executable=True,
        price_state="VIVANT",
        payload={},
        reasons=[],
        vetoes=[],
        favorable_resolution_distance=0.0,
        adverse_resolution_distance=1.0,
        resolution_pressure=0.50,
        state_fragility_score=0.22,
        late_fragility_score=0.14,
        early_fragility_score=0.08,
        score_state_budget=1,
    )


def _state(*, fixture_id: int, competition_name: str, country_name: str, include_quotes: bool = True) -> MatchState:
    quotes = []
    if include_quotes:
        quotes = [
            MarketQuote(market_key="RESULT", scope="FT", side="HOME", line=None, bookmaker="bet365", odds_decimal=3.2, raw={}),
            MarketQuote(market_key="RESULT", scope="FT", side="DRAW", line=None, bookmaker="bet365", odds_decimal=2.3, raw={}),
            MarketQuote(market_key="RESULT", scope="FT", side="AWAY", line=None, bookmaker="bet365", odds_decimal=2.2, raw={}),
            MarketQuote(market_key="OU_FT", scope="FT", side="OVER", line=2.5, bookmaker="bet365", odds_decimal=1.9, raw={}),
            MarketQuote(market_key="OU_FT", scope="FT", side="UNDER", line=2.5, bookmaker="bet365", odds_decimal=1.9, raw={}),
        ]

    return MatchState(
        fixture_id=fixture_id,
        competition_id=100 + fixture_id,
        competition_name=competition_name,
        country_name=country_name,
        minute=63,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=0,
        feed_quality_score=0.84,
        market_quality_score=0.80,
        competition_quality_score=0.76,
        home=TeamLiveStats(
            name="Home",
            shots_total=12,
            shots_on_target=5,
            shots_inside_box=8,
            corners=6,
            possession=56.0,
            dangerous_attacks=33,
            attacks=77,
        ),
        away=TeamLiveStats(
            name="Away",
            shots_total=7,
            shots_on_target=2,
            shots_inside_box=4,
            corners=3,
            possession=44.0,
            dangerous_attacks=18,
            attacks=49,
        ),
        quotes=quotes,
    )


def test_match_priority_engine_penalizes_weaker_competition_structure() -> None:
    intelligence_layer = MatchIntelligenceLayer()
    priority_engine = MatchPriorityEngine()
    projections = [
        _projection("RESULT", "HOME", None),
        _projection("OU_FT", "UNDER", 2.5),
        _projection("BTTS", "NO", None),
        _projection("TEAM_TOTAL", "HOME_UNDER", 1.5),
        _projection("OU_1H", "UNDER", 0.5),
    ]

    elite_state = _state(fixture_id=5101, competition_name="Ligue 1", country_name="France")
    weak_state = _state(fixture_id=5102, competition_name="Liga 3", country_name="Portugal")

    elite_priority = priority_engine.build(elite_state, intelligence_layer.build(elite_state), projections)
    weak_priority = priority_engine.build(weak_state, intelligence_layer.build(weak_state), projections)

    assert elite_priority.q_competition > weak_priority.q_competition
    assert elite_priority.q_match > weak_priority.q_match
    assert elite_priority.priority_tier in {"ELITE_CANDIDATE", "WATCHLIST_CANDIDATE"}
    assert weak_priority.priority_tier != "ELITE_CANDIDATE"
    assert elite_priority.diagnostics["competition"]["competition_whitelisted"] is True
    assert weak_priority.diagnostics["competition"]["competition_whitelisted"] is False


def test_match_priority_engine_marks_sparse_snapshot_as_noisy_doc_only() -> None:
    intelligence_layer = MatchIntelligenceLayer()
    priority_engine = MatchPriorityEngine()
    sparse_state = MatchState(
        fixture_id=5201,
        competition_id=5201,
        competition_name="Liga 3",
        country_name="Portugal",
        minute=9,
        phase="1H",
        status="1H",
        home_goals=0,
        away_goals=0,
        feed_quality_score=0.40,
        market_quality_score=0.35,
        competition_quality_score=0.55,
        home=TeamLiveStats(name="Home"),
        away=TeamLiveStats(name="Away"),
        quotes=[],
    )

    priority = priority_engine.build(sparse_state, intelligence_layer.build(sparse_state), [])

    assert priority.priority_tier == "NOISY_DOC_ONLY"
    assert priority.q_noise >= 5.0
    assert priority.q_odds == 0.0
    assert priority.match_gate_state == "DOC_ONLY"


def test_match_priority_engine_enriches_projection_with_findability_and_publishability() -> None:
    intelligence_layer = MatchIntelligenceLayer()
    priority_engine = MatchPriorityEngine()
    state = _state(fixture_id=5301, competition_name="Ligue 1", country_name="France")
    intelligence = intelligence_layer.build(state)
    projection = _projection("OU_FT", "UNDER", 2.5)

    priority_engine.enrich_projections(state, intelligence, [projection])

    assert projection.market_findability_score > 0.0
    assert projection.publishability_score > 0.0
    assert projection.market_gate_state in {"MARKET_ELIGIBLE", "MARKET_REVIEW"}
    assert projection.thesis_gate_state in {"PUBLISHABLE", "WATCHLIST_ONLY"}
