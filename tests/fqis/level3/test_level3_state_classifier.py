from app.fqis.level3_state_classifier import Level3State, classify_level3_state


def test_real_trade_ready_requires_events_and_stats_but_no_live_staking():
    result = classify_level3_state(
        events_available=True,
        stats_available=True,
        promotion_allowed=False,
    )

    assert result.state == Level3State.REAL_TRADE_READY
    assert result.production_eligible is True
    assert result.research_eligible is True
    assert result.live_staking_allowed is False


def test_events_only_is_research_only():
    result = classify_level3_state(
        events_available=True,
        stats_available=False,
        promotion_allowed=False,
    )

    assert result.state == Level3State.EVENTS_ONLY_RESEARCH_READY
    assert result.production_eligible is False
    assert result.research_eligible is True
    assert result.live_staking_allowed is False


def test_score_only_is_rejected():
    result = classify_level3_state(
        events_available=False,
        stats_available=False,
        promotion_allowed=False,
    )

    assert result.state == Level3State.SCORE_ONLY
    assert result.production_eligible is False
    assert result.research_eligible is False
    assert result.live_staking_allowed is False


def test_promotion_allowed_does_not_enable_live_staking_yet():
    result = classify_level3_state(
        events_available=True,
        stats_available=True,
        promotion_allowed=True,
    )

    assert result.state == Level3State.REAL_TRADE_READY
    assert result.live_staking_allowed is False
