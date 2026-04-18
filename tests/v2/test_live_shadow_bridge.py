from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.runtime.live_shadow_bridge import LiveShadowBridge
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


def _state_with_quotes(
    *,
    fixture_id: int,
    include_result: bool = False,
    include_ou: bool = False,
) -> MatchState:
    quotes: list[MarketQuote] = []

    if include_result:
        quotes.extend(
            [
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="HOME",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=3.40,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="DRAW",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.30,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="AWAY",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=1.35,
                    raw={},
                ),
            ]
        )

    if include_ou:
        quotes.extend(
            [
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="OVER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=2.15,
                    raw={},
                ),
                MarketQuote(
                    market_key="OU_FT",
                    scope="FT",
                    side="UNDER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=1.62,
                    raw={},
                ),
            ]
        )

    return MatchState(
        fixture_id=fixture_id,
        competition_id=1400 + fixture_id,
        competition_name="Live Shadow League",
        country_name="France",
        minute=73,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=0,
        feed_quality_score=0.82,
        market_quality_score=0.76,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=14,
            shots_on_target=6,
            shots_inside_box=9,
            corners=7,
            possession=58.0,
            dangerous_attacks=36,
            attacks=82,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=5,
            shots_on_target=1,
            shots_inside_box=2,
            corners=2,
            possession=42.0,
            dangerous_attacks=13,
            attacks=44,
        ),
        quotes=quotes,
    )


def test_live_shadow_bridge_runs_without_touching_real() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/live_shadow_bridge_guardrail.jsonl")
    runtime._write_export = lambda payload: None
    bridge = LiveShadowBridge(runtime=runtime)

    payload = bridge.run_live_states([_state_with_quotes(fixture_id=4101, include_result=True)])

    assert payload["shadow_mode"] is True
    assert payload["source_mode"] == "live_shadow"
    assert "dispatch" not in payload
    assert "discord" not in payload
    assert "db_live" not in payload
    assert "execution_result" not in payload


def test_live_shadow_payload_is_complete() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/live_shadow_bridge_complete.jsonl")
    runtime._write_export = lambda payload: None
    bridge = LiveShadowBridge(runtime=runtime)

    payload = bridge.run_live_states(
        [
            _state_with_quotes(fixture_id=4201, include_result=True),
            _state_with_quotes(fixture_id=4202, include_ou=True),
        ]
    )

    assert payload["source_mode"] == "live_shadow"
    assert "export_version" in payload
    assert "generated_at_utc" in payload
    assert "match_results" in payload
    assert "board_best" in payload
    assert "board_rankings" in payload
    assert "top_bet_eligible" in payload
    assert "shadow_comparison" in payload
    assert "comparison_summary" in payload
    assert "product" in payload
    assert "debug" in payload


def test_live_shadow_v1_v2_comparison_remains_functional() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/live_shadow_bridge_compare.jsonl")
    runtime._write_export = lambda payload: None
    bridge = LiveShadowBridge(runtime=runtime)
    state = _state_with_quotes(fixture_id=4301, include_result=True)

    baseline = bridge.run_live_states([state])
    best_projection = baseline["match_results"][0]["match_best"]["best_projection"]
    board_best = baseline["board_best"]["best_projection"]

    payload = bridge.run_live_states(
        [state],
        v1_match_documents={4301: {"fixture_id": 4301, **best_projection}},
        v1_board_best={"fixture_id": 4301, **board_best},
    )

    assert payload["shadow_comparison"]["match_level"][0]["same_market_family"] is True
    assert payload["shadow_comparison"]["match_level"][0]["same_direction"] is True


def test_live_shadow_export_schema_is_homogeneous() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/live_shadow_bridge_schema.jsonl")
    captured_exports: list[dict[str, object]] = []
    runtime._write_export = captured_exports.append
    bridge = LiveShadowBridge(runtime=runtime)

    payload = bridge.run_live_states([_state_with_quotes(fixture_id=4401, include_result=True)])

    assert isinstance(payload["shadow_comparison"]["match_level"], list)
    assert len(captured_exports) == 1
    exported = captured_exports[0]
    assert exported["source_mode"] == "live_shadow"
    assert isinstance(exported["shadow_comparison"]["match_level"], list)
    assert len(exported["shadow_comparison"]["match_level"]) == 1


def test_top_bet_eligible_bubbles_up_in_live_shadow_payload() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/live_shadow_bridge_topbet.jsonl")
    runtime._write_export = lambda payload: None
    bridge = LiveShadowBridge(runtime=runtime)

    payload = bridge.run_live_states(
        [
            _state_with_quotes(fixture_id=4501, include_result=True),
            _state_with_quotes(fixture_id=4502, include_ou=True),
        ]
    )

    assert isinstance(payload["top_bet_eligible"], bool)
    assert payload["top_bet_eligible"] == payload["board_best"]["top_bet_eligible"]
    assert payload["shadow_alert_tier"] in {"ELITE", "WATCHLIST", "NO_BET"}


def test_live_shadow_jsonl_exports_are_correct() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/live_shadow_bridge_jsonl.jsonl")
    captured_exports: list[dict[str, object]] = []
    runtime._write_export = captured_exports.append
    bridge = LiveShadowBridge(runtime=runtime)

    payload = bridge.run_live_states(
        [
            _state_with_quotes(fixture_id=4601, include_result=True),
            _state_with_quotes(fixture_id=4602, include_ou=True),
        ],
        v1_match_documents={4601: {"fixture_id": 4601, "market_key": "RESULT", "side": "HOME", "line": None}},
        v1_board_best={"fixture_id": 4601, "market_key": "RESULT", "side": "HOME", "line": None},
    )

    assert len(captured_exports) == 2
    exported = json.loads(json.dumps(captured_exports[0]))
    assert exported["export_version"] == payload["export_version"]
    assert exported["source_mode"] == "live_shadow"
    assert "shadow_comparison" in exported
    assert "comparison_summary" in exported
    assert isinstance(exported["shadow_comparison"]["match_level"], list)
    assert "product" in exported
    assert "debug" in exported
