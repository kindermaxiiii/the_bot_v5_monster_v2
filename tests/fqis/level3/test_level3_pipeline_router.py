from app.fqis.level3_pipeline_router import route_level3_pipeline


def test_real_trade_ready_routes_to_production_without_live_staking():
    route = route_level3_pipeline(
        state="REAL_TRADE_READY",
        promotion_allowed=False,
    )

    assert route.pipeline == "production"
    assert route.production_allowed is True
    assert route.research_allowed is True
    assert route.reject is False
    assert route.live_staking_allowed is False


def test_events_only_routes_to_research_only():
    route = route_level3_pipeline(
        state="EVENTS_ONLY_RESEARCH_READY",
        promotion_allowed=False,
    )

    assert route.pipeline == "research"
    assert route.production_allowed is False
    assert route.research_allowed is True
    assert route.reject is False
    assert route.live_staking_allowed is False


def test_score_only_routes_to_reject():
    route = route_level3_pipeline(
        state="SCORE_ONLY",
        promotion_allowed=False,
    )

    assert route.pipeline == "reject"
    assert route.production_allowed is False
    assert route.research_allowed is False
    assert route.reject is True
    assert route.live_staking_allowed is False


def test_unknown_state_routes_to_reject():
    route = route_level3_pipeline(
        state="BROKEN_STATE",
        promotion_allowed=True,
    )

    assert route.pipeline == "reject"
    assert route.live_staking_allowed is False
