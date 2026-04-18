from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.contracts import BoardBestVehicle, MarketProjectionV2
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2
from app.v2.runtime.shadow_recorder import ShadowRecorder


def _projection(
    *,
    market_key: str,
    side: str,
    line: float | None,
    bookmaker: str = "bet365",
    odds_decimal: float | None = 2.0,
) -> MarketProjectionV2:
    return MarketProjectionV2(
        market_key=market_key,
        side=side,
        line=line,
        bookmaker=bookmaker,
        odds_decimal=odds_decimal,
        raw_probability=0.62,
        calibrated_probability=0.60,
        market_no_vig_probability=0.50,
        edge=0.10,
        expected_value=0.14,
        executable=True,
        price_state="VIVANT",
        payload={"feed_quality": 0.80, "market_quality": 0.76},
        reasons=["test_projection"],
        vetoes=[],
        favorable_resolution_distance=0.0,
        adverse_resolution_distance=1.0,
        resolution_pressure=0.50,
        state_fragility_score=0.22,
        late_fragility_score=0.14,
        early_fragility_score=0.08,
        score_state_budget=1,
    )


def _state_with_quotes(
    *,
    fixture_id: int,
    home_goals: int = 1,
    away_goals: int = 0,
    include_result: bool = False,
    include_ou: bool = False,
    use_v1_ou_alias: bool = False,
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
                    odds_decimal=3.45,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="DRAW",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=2.35,
                    raw={},
                ),
                MarketQuote(
                    market_key="RESULT",
                    scope="FT",
                    side="AWAY",
                    line=None,
                    bookmaker="bet365",
                    odds_decimal=1.32,
                    raw={},
                ),
            ]
        )

    if include_ou:
        market_key = "ou" if use_v1_ou_alias else "OU_FT"
        quotes.extend(
            [
                MarketQuote(
                    market_key=market_key,
                    scope="FT",
                    side="OVER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=2.10,
                    raw={},
                ),
                MarketQuote(
                    market_key=market_key,
                    scope="FT",
                    side="UNDER",
                    line=2.5,
                    bookmaker="bet365",
                    odds_decimal=1.65,
                    raw={},
                ),
            ]
        )

    return MatchState(
        fixture_id=fixture_id,
        competition_id=900 + fixture_id,
        competition_name="Shadow League",
        country_name="France",
        minute=74,
        phase="2H",
        status="2H",
        home_goals=home_goals,
        away_goals=away_goals,
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


def test_runtime_v2_exports_complete_shadow_payload() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase7_test.jsonl")
    captured_exports: list[dict[str, object]] = []
    runtime._write_export = captured_exports.append

    payload = runtime.run_states(
        [
            _state_with_quotes(fixture_id=3101, include_result=True),
            _state_with_quotes(fixture_id=3102, include_ou=True),
        ]
    )

    assert payload["shadow_mode"] is True
    assert payload["source_mode"] == "runtime_shadow"
    assert "match_results" in payload
    assert "board_best" in payload
    assert "board_rankings" in payload
    assert "top_bet_eligible" in payload
    assert "shadow_governance" in payload
    assert "shadow_alert_tier" in payload
    assert "shadow_comparison" in payload
    assert "comparison_summary" in payload
    assert "product" in payload
    assert "debug" in payload
    assert len(payload["match_results"]) == 2
    assert "priority" in payload["match_results"][0]
    assert payload["product"]["shadow_alert_tier"] == payload["shadow_alert_tier"]
    assert "board_gate_state" not in payload["product"]["board_best"]
    assert "board_best" in payload["debug"]
    assert "shadow_governance" in payload["debug"]

    assert len(captured_exports) == 2
    exported_line = json.loads(json.dumps(captured_exports[0]))
    assert exported_line["source_mode"] == "runtime_shadow"
    assert "priority" in exported_line
    assert "shadow_governance" in exported_line
    assert "shadow_comparison" in exported_line
    assert "comparison_summary" in exported_line
    assert "product" in exported_line
    assert "debug" in exported_line


def test_shadow_recorder_records_comparisons_cleanly() -> None:
    recorder = ShadowRecorder()
    match_results = [
        {
            "fixture_id": 3201,
            "minute": 74,
            "score": "1-0",
            "intelligence": {"fixture_priority_score": 7.2, "regime_label": "OPEN_EXCHANGE"},
            "projection_counts": {"RESULT": 3},
            "match_best": {
                "best_projection": _projection(market_key="RESULT", side="HOME", line=None).to_dict(),
                "second_best_projection": _projection(market_key="RESULT", side="DRAW", line=None).to_dict(),
                "candidate_count": 3,
                "dominance_score": 0.48,
                "diagnostics": {"market_count_by_key": {"RESULT": 3}},
                "rejected_same_match_candidates": [],
            },
        }
    ]
    board_best = BoardBestVehicle(
        best_projection=_projection(market_key="RESULT", side="HOME", line=None),
        board_dominance_score=0.52,
        top_bet_eligible=True,
        diagnostics={"best_fixture_id": 3201},
    )

    bundle = recorder.build_export_records(
        export_version="test_shadow",
        generated_at_utc="2026-04-12T00:00:00+00:00",
        source_mode="runtime_shadow",
        match_results=match_results,
        board_best=board_best,
        board_rankings=[{"fixture_id": 3201, "board_score": 2.4}],
        top_bet_eligible=True,
        v1_match_documents={3201: {"fixture_id": 3201, "market_key": "RESULT", "side": "HOME", "line": None}},
        v1_board_best={"fixture_id": 3201, "market_key": "RESULT", "side": "HOME", "line": None},
    )

    assert "shadow_comparison" in bundle
    assert "comparison_summary" in bundle
    assert len(bundle["export_records"]) == 1
    assert bundle["export_records"][0]["source_mode"] == "runtime_shadow"
    assert len(bundle["export_records"][0]["shadow_comparison"]["match_level"]) == 1
    assert bundle["export_records"][0]["shadow_comparison"]["match_level"][0]["same_market_family"] is True


def test_identical_v1_v2_comparison_is_recognized_correctly() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase7_identical.jsonl")
    runtime._write_export = lambda payload: None
    state = _state_with_quotes(fixture_id=3301, include_result=True)

    base_payload = runtime.run_states([state])
    best_projection = base_payload["match_results"][0]["match_best"]["best_projection"]
    board_best = base_payload["board_best"]["best_projection"]

    payload = runtime.run_states(
        [state],
        v1_match_documents={3301: {"fixture_id": 3301, **best_projection}},
        v1_board_best={"fixture_id": 3301, **board_best},
    )

    comparison = payload["shadow_comparison"]["match_level"][0]
    assert comparison["same_market_family"] is True
    assert comparison["same_direction"] is True
    assert payload["shadow_comparison"]["board_level"]["same_market_family"] is True


def test_v1_v2_divergence_is_recognized_correctly() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase7_divergence.jsonl")
    runtime._write_export = lambda payload: None
    state = _state_with_quotes(fixture_id=3401, include_result=True)

    payload = runtime.run_states(
        [state],
        v1_match_documents={3401: {"fixture_id": 3401, "market_key": "RESULT", "side": "AWAY", "line": None}},
        v1_board_best={"fixture_id": 3401, "market_key": "RESULT", "side": "AWAY", "line": None},
    )

    comparison = payload["shadow_comparison"]["match_level"][0]
    assert comparison["same_market_family"] is True
    assert comparison["same_direction"] is False
    assert "side_differs" in comparison["diagnostics"]["divergence_reasons"]
    assert payload["comparison_summary"]["v2_divergence_count"] == 1


def test_comparison_summary_aggregates_stats_cleanly() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase7_summary.jsonl")
    runtime._write_export = lambda payload: None
    state_one = _state_with_quotes(fixture_id=3501, include_result=True)
    state_two = _state_with_quotes(fixture_id=3502, include_ou=True)

    base_payload = runtime.run_states([state_one, state_two])
    best_one = base_payload["match_results"][0]["match_best"]["best_projection"]

    payload = runtime.run_states(
        [state_one, state_two],
        v1_match_documents={
            3501: {"fixture_id": 3501, **best_one},
            3502: {"fixture_id": 3502, "market_key": "RESULT", "side": "HOME", "line": None},
        },
        v1_board_best={"fixture_id": 9999, "market_key": "BTTS", "side": "YES", "line": None},
    )

    summary = payload["comparison_summary"]
    assert summary["compared_match_count"] == 2
    assert summary["same_market_family_count"] == 1
    assert summary["v2_divergence_count"] == 1
    assert summary["board_best_difference_count"] == 1


def test_top_bet_eligible_is_exposed_in_final_payload() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase7_topbet.jsonl")
    runtime._write_export = lambda payload: None
    payload = runtime.run_states(
        [
            _state_with_quotes(fixture_id=3601, include_result=True),
            _state_with_quotes(fixture_id=3602, include_ou=True),
        ]
    )

    assert isinstance(payload["top_bet_eligible"], bool)
    assert payload["top_bet_eligible"] == payload["board_best"]["top_bet_eligible"]
    assert payload["comparison_summary"]["v2_top_bet_eligible_true_count"] == int(payload["top_bet_eligible"])
    assert payload["shadow_governance"]["shadow_alert_tier"] == payload["shadow_alert_tier"]
    assert payload["shadow_alert_tier"] in {"ELITE", "WATCHLIST", "NO_BET"}


def test_no_real_integration_is_introduced() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase7_guardrail.jsonl")
    runtime._write_export = lambda payload: None
    payload = runtime.run_states([_state_with_quotes(fixture_id=3701, include_result=True)])

    assert payload["shadow_mode"] is True
    assert payload["source_mode"] == "runtime_shadow"
    assert "dispatch" not in payload
    assert "discord" not in payload
    assert "db_live" not in payload
    assert "execution_result" not in payload


def test_shadow_comparison_schema_is_homogeneous_between_payload_and_exports() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase8_homogeneous.jsonl")
    captured_exports: list[dict[str, object]] = []
    runtime._write_export = captured_exports.append

    payload = runtime.run_states([_state_with_quotes(fixture_id=3801, include_result=True)])

    assert isinstance(payload["shadow_comparison"]["match_level"], list)
    assert len(captured_exports) == 1
    assert isinstance(captured_exports[0]["shadow_comparison"]["match_level"], list)


def test_runtime_v2_canonicalizes_v1_ou_aliases_before_translation() -> None:
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_alias_adapter.jsonl")
    runtime._write_export = lambda payload: None

    payload = runtime.run_states([_state_with_quotes(fixture_id=3901, include_ou=True, use_v1_ou_alias=True)])

    assert payload["match_results"][0]["projection_counts"]["OU_FT"] > 0
