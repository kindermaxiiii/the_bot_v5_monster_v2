from __future__ import annotations

from app.vnext.live.blocks import build_live_context_pack
from tests.vnext.live_factories import make_live_snapshot


def test_live_blocks_expose_raw_measures_and_edges() -> None:
    snapshot = make_live_snapshot()
    context = build_live_context_pack(snapshot)

    assert context.threat.home_threat_raw > context.threat.away_threat_raw
    assert context.threat.threat_edge > 0.0
    assert context.pressure.home_pressure_raw > context.pressure.away_pressure_raw
    assert context.pressure.pressure_edge > 0.0
    assert context.balance.home_balance_raw > context.balance.away_balance_raw
    assert context.balance.balance_edge > 0.0
    assert context.state.time_band == "MID"
    assert context.state.state_edge < context.threat.threat_edge


def test_break_events_require_previous_snapshot() -> None:
    previous = make_live_snapshot(minute=61, status="2H", home_goals=1, away_goals=0)
    current = make_live_snapshot(minute=67, status="2H", home_goals=1, away_goals=1)
    context = build_live_context_pack(current, previous)

    assert context.break_events.goal_scored is True
    assert context.break_events.away_goal_scored is True
    assert context.break_events.equalizer_event is True
    assert context.break_events.event_clarity_score > 0.8
