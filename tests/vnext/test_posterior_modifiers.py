from __future__ import annotations

from app.vnext.live.blocks import build_live_context_pack
from app.vnext.posterior.modifiers import build_modifier
from tests.vnext.live_factories import make_live_snapshot


def test_noise_does_not_create_strong_modifier_on_its_own() -> None:
    previous = make_live_snapshot(minute=24, status="1H", home_goals=0, away_goals=0)
    current = make_live_snapshot(
        minute=27,
        status="1H",
        home_goals=0,
        away_goals=0,
        home_shots_total=7,
        away_shots_total=6,
        home_shots_on=2,
        away_shots_on=2,
        home_dangerous_attacks=19,
        away_dangerous_attacks=18,
        home_xg=0.35,
        away_xg=0.31,
    )
    modifier = build_modifier(key="HOME_CONTROL", context=build_live_context_pack(current, previous))

    assert modifier.intensity == "FAIBLE"
    assert modifier.status in {"ATTENDU", "CONTRARIE"}


def test_break_event_can_trigger_rupture() -> None:
    previous = make_live_snapshot(minute=63, status="2H", home_goals=1, away_goals=0)
    current = make_live_snapshot(
        minute=78,
        status="2H",
        home_goals=1,
        away_goals=2,
        home_shots_total=9,
        away_shots_total=14,
        home_shots_on=3,
        away_shots_on=7,
        home_dangerous_attacks=21,
        away_dangerous_attacks=39,
        home_xg=0.8,
        away_xg=1.8,
    )
    modifier = build_modifier(key="HOME_CONTROL", context=build_live_context_pack(current, previous))

    assert modifier.status == "RUPTURE"
    assert "lead_change_event" in modifier.active_event_flags
