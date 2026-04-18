from __future__ import annotations

import json

from app.core.match_state import MarketQuote, MatchState, TeamLiveStats
from app.v2.arbiter.board_meta_arbiter import BoardMetaArbiter
from app.v2.arbiter.market_meta_arbiter import MarketMetaArbiter
from app.v2.contracts import MarketProjectionV2
from app.v2.runtime.runtime_cycle_v2 import RuntimeCycleV2


def _priority(
    fixture_id: int,
    *,
    q_match: float,
    q_competition: float,
    q_noise: float,
    priority_tier: str,
    q_stats: float = 7.0,
    q_odds: float = 7.0,
    q_live: float = 7.0,
    match_gate_state: str = "MATCH_ELIGIBLE",
) -> dict[str, object]:
    return {
        "fixture_id": fixture_id,
        "q_match": q_match,
        "q_stats": q_stats,
        "q_odds": q_odds,
        "q_live": q_live,
        "q_competition": q_competition,
        "q_noise": q_noise,
        "priority_tier": priority_tier,
        "match_gate_state": match_gate_state,
        "diagnostics": {
            "competition": {"competition_bucket": "elite", "competition_whitelisted": True},
            "stats": {"coherence_score": 0.84, "stat_richness_floor_passed": True},
            "odds": {
                "executable_projection_count": 5,
                "market_family_count": 3,
                "odds_depth_floor_passed": True,
            },
            "match_gate_reasons": [],
        },
    }


def _projection(
    *,
    market_key: str = "OU_FT",
    side: str = "UNDER",
    line: float | None = 2.5,
    bookmaker: str = "bet365",
    odds_decimal: float | None = 1.90,
    raw_probability: float = 0.62,
    calibrated_probability: float = 0.60,
    market_no_vig_probability: float = 0.50,
    edge: float = 0.10,
    expected_value: float = 0.14,
    executable: bool = True,
    price_state: str = "VIVANT",
    vetoes: list[str] | None = None,
    feed_quality: float = 0.80,
    market_quality: float = 0.76,
    resolution_pressure: float = 0.55,
    state_fragility_score: float = 0.24,
    late_fragility_score: float = 0.16,
    early_fragility_score: float = 0.10,
    market_findability_score: float = 0.78,
    publishability_score: float = 0.81,
    market_gate_state: str = "MARKET_ELIGIBLE",
    thesis_gate_state: str = "PUBLISHABLE",
) -> MarketProjectionV2:
    return MarketProjectionV2(
        market_key=market_key,
        side=side,
        line=line,
        bookmaker=bookmaker,
        odds_decimal=odds_decimal,
        raw_probability=raw_probability,
        calibrated_probability=calibrated_probability,
        market_no_vig_probability=market_no_vig_probability,
        edge=edge,
        expected_value=expected_value,
        executable=executable,
        price_state=price_state,
        payload={"feed_quality": feed_quality, "market_quality": market_quality},
        reasons=["test_fixture"],
        vetoes=list(vetoes or []),
        favorable_resolution_distance=0.0,
        adverse_resolution_distance=1.0,
        resolution_pressure=resolution_pressure,
        state_fragility_score=state_fragility_score,
        late_fragility_score=late_fragility_score,
        early_fragility_score=early_fragility_score,
        score_state_budget=1,
        market_findability_score=market_findability_score,
        publishability_score=publishability_score,
        market_gate_state=market_gate_state,
        thesis_gate_state=thesis_gate_state,
    )


def _state_with_runtime_quotes(*, fixture_id: int, quotes: list[MarketQuote]) -> MatchState:
    return MatchState(
        fixture_id=fixture_id,
        competition_id=501,
        competition_name="Test League",
        country_name="France",
        minute=74,
        phase="2H",
        status="2H",
        home_goals=1,
        away_goals=0,
        feed_quality_score=0.82,
        market_quality_score=0.77,
        home=TeamLiveStats(
            team_id=1,
            name="Home",
            shots_total=14,
            shots_on_target=6,
            shots_inside_box=9,
            corners=7,
            possession=58.0,
            dangerous_attacks=36,
            attacks=83,
        ),
        away=TeamLiveStats(
            team_id=2,
            name="Away",
            shots_total=4,
            shots_on_target=1,
            shots_inside_box=2,
            corners=2,
            possession=42.0,
            dangerous_attacks=12,
            attacks=43,
        ),
        quotes=quotes,
    )


def test_match_level_executable_beats_non_executable_on_same_match() -> None:
    arbiter = MarketMetaArbiter()
    executable_projection = _projection(market_key="BTTS", side="NO", expected_value=0.10, edge=0.08, executable=True)
    non_executable_projection = _projection(
        market_key="OU_FT",
        side="OVER",
        expected_value=0.45,
        edge=0.22,
        executable=False,
        price_state="DEGRADE_MAIS_VIVANT",
    )

    match_best = arbiter.select_match_best(1001, [non_executable_projection, executable_projection])

    assert match_best.best_projection is not None
    assert match_best.best_projection.market_key == "BTTS"
    assert match_best.best_projection.executable is True


def test_match_level_hard_veto_market_cannot_dominate_live_market() -> None:
    arbiter = MarketMetaArbiter()
    live_projection = _projection(market_key="TEAM_TOTAL", side="AWAY_UNDER", expected_value=0.12, edge=0.09, executable=True)
    hard_veto_projection = _projection(
        market_key="RESULT",
        side="HOME",
        expected_value=0.75,
        edge=0.30,
        executable=True,
        vetoes=["result_home_already_won_at_score"],
    )

    match_best = arbiter.select_match_best(1002, [hard_veto_projection, live_projection])

    assert match_best.best_projection is not None
    assert match_best.best_projection.market_key == "TEAM_TOTAL"
    assert "result_home_already_won_at_score" in match_best.rejected_same_match_candidates[0].vetoes


def test_match_level_dominance_and_rejections_are_exposed_cleanly() -> None:
    arbiter = MarketMetaArbiter()
    best_projection = _projection(market_key="OU_1H", side="UNDER", line=0.5, expected_value=0.22, edge=0.14)
    second_projection = _projection(market_key="OU_FT", side="UNDER", expected_value=0.08, edge=0.05)
    third_projection = _projection(market_key="BTTS", side="NO", expected_value=0.04, edge=0.02)

    match_best = arbiter.select_match_best(1003, [best_projection, second_projection, third_projection])

    assert match_best.best_projection is not None
    assert match_best.second_best_projection is not None
    assert match_best.best_projection.market_key == "OU_1H"
    assert match_best.second_best_projection.market_key == "OU_FT"
    assert match_best.candidate_count == 3
    assert len(match_best.rejected_same_match_candidates) == 2
    assert match_best.diagnostics["market_count_by_key"]["OU_1H"] == 1
    assert abs(match_best.dominance_score - (match_best.diagnostics["best_score"] - match_best.diagnostics["second_score"])) < 1e-9


def test_board_level_selects_best_match_vehicle() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    weak_match = match_arbiter.select_match_best(2001, [_projection(expected_value=0.08, edge=0.04)])
    strong_match = match_arbiter.select_match_best(
        2002,
        [
            _projection(market_key="RESULT", side="HOME", line=None, expected_value=0.42, edge=0.20),
            _projection(market_key="OU_FT", side="UNDER", expected_value=0.04, edge=0.02),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [weak_match, strong_match],
        priority_by_fixture={
            2001: _priority(2001, q_match=5.4, q_competition=4.5, q_noise=4.3, priority_tier="WATCHLIST_CANDIDATE"),
            2002: _priority(2002, q_match=8.1, q_competition=7.2, q_noise=2.4, priority_tier="ELITE_CANDIDATE"),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.best_projection.market_key == "RESULT"
    assert board_best.diagnostics["best_fixture_id"] == 2002
    assert board_best.shadow_alert_tier == "ELITE"
    assert board_best.board_gate_state == "PROMOTED_ELITE"


def test_top_bet_stays_false_when_relative_dominance_is_insufficient() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    match_a = match_arbiter.select_match_best(
        2101,
        [
            _projection(expected_value=0.14, edge=0.08),
            _projection(expected_value=0.11, edge=0.07),
        ],
    )
    match_b = match_arbiter.select_match_best(
        2102,
        [
            _projection(market_key="BTTS", side="NO", line=None, expected_value=0.13, edge=0.075),
            _projection(market_key="RESULT", side="DRAW", line=None, expected_value=0.12, edge=0.07),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [match_a, match_b],
        priority_by_fixture={
            2101: _priority(2101, q_match=6.2, q_competition=5.6, q_noise=3.9, priority_tier="WATCHLIST_CANDIDATE"),
            2102: _priority(2102, q_match=6.1, q_competition=5.5, q_noise=3.8, priority_tier="WATCHLIST_CANDIDATE"),
        },
    )

    assert board_best.top_bet_eligible is False
    assert board_best.board_dominance_score < board_arbiter.min_board_dominance
    assert board_best.shadow_alert_tier == "WATCHLIST"
    assert board_best.board_gate_state == "PROMOTED_WATCHLIST"


def test_match_under_review_can_still_be_promoted_to_watchlist_when_clean() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    reviewed_match = match_arbiter.select_match_best(
        2151,
        [
            _projection(market_key="OU_FT", side="UNDER", expected_value=0.16, edge=0.09),
            _projection(market_key="RESULT", side="DRAW", line=None, expected_value=0.04, edge=0.02),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [reviewed_match],
        priority_by_fixture={
            2151: _priority(
                2151,
                q_match=4.75,
                q_stats=5.2,
                q_odds=5.0,
                q_competition=5.4,
                q_noise=3.2,
                priority_tier="WATCHLIST_CANDIDATE",
                match_gate_state="MATCH_UNDER_REVIEW",
            ),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.shadow_alert_tier == "WATCHLIST"
    assert board_best.board_gate_state == "PROMOTED_WATCHLIST"
    assert board_best.diagnostics["gate_states"]["match_eligible"] == "PASS_UNDER_REVIEW"


def test_result_family_on_probation_is_kept_out_of_watchlist_when_manual_is_unconfirmed() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    result_match = match_arbiter.select_match_best(
        2161,
        [
            _projection(
                market_key="RESULT",
                side="AWAY",
                line=None,
                odds_decimal=9.5,
                expected_value=1.10,
                edge=0.14,
                market_findability_score=1.0,
                publishability_score=0.95,
            ),
            _projection(
                market_key="OU_FT",
                side="UNDER",
                expected_value=0.02,
                edge=0.01,
            ),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [result_match],
        priority_by_fixture={
            2161: _priority(
                2161,
                q_match=5.2,
                q_stats=6.4,
                q_odds=5.4,
                q_competition=6.2,
                q_noise=2.5,
                priority_tier="WATCHLIST_CANDIDATE",
                match_gate_state="MATCH_ELIGIBLE",
            ),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.best_projection.market_key == "RESULT"
    assert board_best.shadow_alert_tier == "NONE"
    assert board_best.board_gate_state == "NO_BET"
    assert "family_on_probation" in board_best.diagnostics["watchlist_refusal_reasons"]
    assert "speculative_price" in board_best.diagnostics["watchlist_refusal_reasons"]
    assert "manual_not_confirmed" in board_best.diagnostics["watchlist_refusal_reasons"]


def test_watchlist_longshot_cap_blocks_speculative_family_even_when_approved() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    longshot_match = match_arbiter.select_match_best(
        2162,
        [
            _projection(
                market_key="TEAM_TOTAL",
                side="AWAY_OVER",
                line=1.5,
                odds_decimal=11.0,
                expected_value=1.40,
                edge=0.18,
                market_findability_score=0.94,
                publishability_score=0.95,
            ),
            _projection(
                market_key="OU_FT",
                side="UNDER",
                expected_value=0.02,
                edge=0.01,
            ),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [longshot_match],
        priority_by_fixture={
            2162: _priority(
                2162,
                q_match=5.3,
                q_stats=6.3,
                q_odds=5.5,
                q_competition=6.1,
                q_noise=2.6,
                priority_tier="WATCHLIST_CANDIDATE",
                match_gate_state="MATCH_ELIGIBLE",
            ),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.best_projection.market_key == "TEAM_TOTAL"
    assert board_best.shadow_alert_tier == "NONE"
    assert board_best.board_gate_state == "NO_BET"
    assert "speculative_price" in board_best.diagnostics["watchlist_refusal_reasons"]
    assert "manual_not_confirmed" in board_best.diagnostics["watchlist_refusal_reasons"]


def test_top_bet_can_become_true_when_board_dominance_is_clear() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    dominant_match = match_arbiter.select_match_best(
        2201,
        [
            _projection(
                market_key="RESULT",
                side="HOME",
                line=None,
                expected_value=0.55,
                edge=0.28,
                calibrated_probability=0.72,
                feed_quality=0.84,
                market_quality=0.79,
                resolution_pressure=0.40,
                state_fragility_score=0.14,
                late_fragility_score=0.09,
                early_fragility_score=0.06,
            ),
            _projection(expected_value=0.02, edge=0.01, calibrated_probability=0.48),
        ],
    )
    runner_up_match = match_arbiter.select_match_best(
        2202,
        [
            _projection(
                market_key="OU_FT",
                side="UNDER",
                expected_value=0.10,
                edge=0.05,
                calibrated_probability=0.56,
                feed_quality=0.75,
                market_quality=0.72,
                resolution_pressure=0.70,
                state_fragility_score=0.28,
            ),
            _projection(expected_value=0.03, edge=0.01, calibrated_probability=0.49),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [dominant_match, runner_up_match],
        priority_by_fixture={
            2201: _priority(2201, q_match=8.7, q_competition=7.9, q_noise=2.1, priority_tier="ELITE_CANDIDATE"),
            2202: _priority(2202, q_match=6.0, q_competition=5.9, q_noise=3.9, priority_tier="WATCHLIST_CANDIDATE"),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.best_projection.market_key == "RESULT"
    assert board_best.top_bet_eligible is True
    assert board_best.board_dominance_score >= board_arbiter.min_board_dominance
    assert dominant_match.dominance_score >= board_arbiter.min_match_dominance
    assert board_best.shadow_alert_tier == "ELITE"
    assert board_best.diagnostics["gate_states"]["match_eligible"] == "PASS"


def test_non_executable_vehicle_cannot_become_board_best() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    non_executable_match = match_arbiter.select_match_best(
        2301,
        [
            _projection(
                market_key="RESULT",
                side="HOME",
                line=None,
                expected_value=0.90,
                edge=0.45,
                executable=False,
                price_state="DEGRADE_MAIS_VIVANT",
                vetoes=["pair_or_triplet_not_fully_live_same_book"],
            )
        ],
    )
    executable_match = match_arbiter.select_match_best(
        2302,
        [
            _projection(market_key="BTTS", side="NO", line=None, expected_value=0.14, edge=0.09),
            _projection(expected_value=0.03, edge=0.01),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [non_executable_match, executable_match],
        priority_by_fixture={
            2301: _priority(2301, q_match=8.9, q_competition=8.0, q_noise=2.0, priority_tier="ELITE_CANDIDATE"),
            2302: _priority(2302, q_match=6.4, q_competition=5.8, q_noise=3.2, priority_tier="WATCHLIST_CANDIDATE"),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.best_projection.market_key == "BTTS"
    assert board_best.diagnostics["best_fixture_id"] == 2302


def test_low_structure_match_cannot_dominate_elite_board_with_edge_only() -> None:
    match_arbiter = MarketMetaArbiter()
    board_arbiter = BoardMetaArbiter(match_arbiter)
    noisy_minor_match = match_arbiter.select_match_best(
        2351,
        [
            _projection(market_key="TEAM_TOTAL", side="HOME_UNDER", expected_value=0.58, edge=0.27),
            _projection(expected_value=0.02, edge=0.01),
        ],
    )
    elite_match = match_arbiter.select_match_best(
        2352,
        [
            _projection(market_key="RESULT", side="AWAY", line=None, expected_value=0.34, edge=0.18),
            _projection(expected_value=0.04, edge=0.02),
        ],
    )

    board_best = board_arbiter.select_board_best(
        [noisy_minor_match, elite_match],
        priority_by_fixture={
            2351: _priority(2351, q_match=4.8, q_competition=3.2, q_noise=6.1, priority_tier="LOW_PRIORITY"),
            2352: _priority(2352, q_match=8.0, q_competition=7.4, q_noise=2.5, priority_tier="ELITE_CANDIDATE"),
        },
    )

    assert board_best.best_projection is not None
    assert board_best.diagnostics["best_fixture_id"] == 2352
    assert board_best.best_projection.market_key == "RESULT"


def test_runtime_shadow_exports_match_and_board_level_outputs() -> None:
    state_one = _state_with_runtime_quotes(
        fixture_id=2401,
        quotes=[
            MarketQuote(market_key="RESULT", scope="FT", side="HOME", line=None, bookmaker="bet365", odds_decimal=3.50, raw={}),
            MarketQuote(market_key="RESULT", scope="FT", side="DRAW", line=None, bookmaker="bet365", odds_decimal=2.20, raw={}),
            MarketQuote(market_key="RESULT", scope="FT", side="AWAY", line=None, bookmaker="bet365", odds_decimal=1.45, raw={}),
        ],
    )
    state_two = _state_with_runtime_quotes(
        fixture_id=2402,
        quotes=[
            MarketQuote(market_key="OU_FT", scope="FT", side="OVER", line=2.5, bookmaker="bet365", odds_decimal=2.20, raw={}),
            MarketQuote(market_key="OU_FT", scope="FT", side="UNDER", line=2.5, bookmaker="bet365", odds_decimal=1.60, raw={}),
        ],
    )

    captured_exports: list[dict[str, object]] = []
    runtime = RuntimeCycleV2(export_path="tests/v2/runtime_cycle_v2_phase6_test.jsonl")
    runtime._write_export = captured_exports.append
    payload = runtime.run_states([state_one, state_two])

    assert len(payload["match_results"]) == 2
    assert payload["board_best"]["best_projection"] is not None
    assert len(payload["board_rankings"]) == 2
    assert isinstance(payload["top_bet_eligible"], bool)
    assert "priority" in payload["match_results"][0]
    assert payload["shadow_alert_tier"] in {"ELITE", "WATCHLIST", "NO_BET"}
    assert "shadow_governance" in payload
    assert "product" in payload
    assert "debug" in payload

    assert len(captured_exports) == 2
    exported_line = json.loads(json.dumps(captured_exports[0]))
    assert "match_best" in exported_line
    assert "board_best" in exported_line
    assert "board_rankings" in exported_line
    assert "top_bet_eligible" in exported_line
    assert "priority" in exported_line
    assert "shadow_governance" in exported_line
    assert "product" in exported_line
    assert "debug" in exported_line
