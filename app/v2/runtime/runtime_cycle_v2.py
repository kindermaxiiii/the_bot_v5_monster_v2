from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

from app.core.match_state import MatchState, build_match_state
from app.v2.arbiter.board_meta_arbiter import BoardMetaArbiter
from app.v2.arbiter.market_meta_arbiter import MarketMetaArbiter
from app.v2.contracts import MarketProjectionV2
from app.v2.intelligence.match_intelligence_layer import MatchIntelligenceLayer
from app.v2.markets.btts_translator import BTTSTranslator
from app.v2.markets.ou_1h_translator import OU1HTranslator
from app.v2.markets.ou_ft_translator import OUFTTranslator
from app.v2.markets.quote_alias_adapter import QuoteAliasAdapter
from app.v2.markets.result_translator import ResultTranslator
from app.v2.markets.team_total_translator import TeamTotalTranslator
from app.v2.prioritization.match_priority_engine import MatchPriorityEngine
from app.v2.probability.unified_probability_core import UnifiedProbabilityCore
from app.v2.runtime.shadow_recorder import ShadowRecorder


class RuntimeCycleV2:
    """
    Phase 9 shadow runtime.

    Important guarantees:
    - no dispatch
    - no Discord
    - no writes to live tables
    - documentary export only
    """

    export_version = "v2_phase9_shadow_v1"

    def __init__(self, export_path: str | Path | None = None) -> None:
        self.intelligence = MatchIntelligenceLayer()
        self.probability = UnifiedProbabilityCore()
        self.quote_alias_adapter = QuoteAliasAdapter()
        self.ou_ft = OUFTTranslator()
        self.ou_1h = OU1HTranslator()
        self.btts = BTTSTranslator()
        self.team_total = TeamTotalTranslator()
        self.result = ResultTranslator()
        self.priority = MatchPriorityEngine()
        self.match_arbiter = MarketMetaArbiter()
        self.board_arbiter = BoardMetaArbiter(self.match_arbiter)
        self.shadow_recorder = ShadowRecorder()
        self.export_path = Path(export_path or "exports/v2/runtime_cycle_v2.jsonl")
        self._light_match_memory: dict[int, dict[str, Any]] = {}

    def _public_shadow_alert_tier(self, raw_tier: str | None) -> str:
        tier = str(raw_tier or "").upper().strip()
        return tier if tier in {"ELITE", "WATCHLIST"} else "NO_BET"

    def _public_projection_dict(self, projection: dict[str, Any] | None) -> dict[str, Any] | None:
        if not projection:
            return None
        return {
            "market_key": projection.get("market_key"),
            "side": projection.get("side"),
            "line": projection.get("line"),
            "bookmaker": projection.get("bookmaker"),
            "odds_decimal": projection.get("odds_decimal"),
            "calibrated_probability": projection.get("calibrated_probability"),
            "market_no_vig_probability": projection.get("market_no_vig_probability"),
            "edge": projection.get("edge"),
            "expected_value": projection.get("expected_value"),
            "executable": projection.get("executable"),
            "price_state": projection.get("price_state"),
        }

    def _public_priority_dict(self, priority: dict[str, Any]) -> dict[str, Any]:
        return {
            "fixture_id": priority.get("fixture_id"),
            "q_match": priority.get("q_match"),
            "q_stats": priority.get("q_stats"),
            "q_odds": priority.get("q_odds"),
            "priority_tier": priority.get("priority_tier"),
        }

    def _public_match_best_dict(self, match_best: dict[str, Any]) -> dict[str, Any]:
        return {
            "best_projection": self._public_projection_dict(match_best.get("best_projection")),
            "second_best_projection": self._public_projection_dict(match_best.get("second_best_projection")),
            "dominance_score": match_best.get("dominance_score"),
            "candidate_count": match_best.get("candidate_count"),
        }

    def _public_match_result_dict(self, item: dict[str, Any]) -> dict[str, Any]:
        return {
            "fixture_id": item.get("fixture_id"),
            "minute": item.get("minute"),
            "score": item.get("score"),
            "projection_counts": dict(item.get("projection_counts", {}) or {}),
            "priority": self._public_priority_dict(dict(item.get("priority", {}) or {})),
            "match_best": self._public_match_best_dict(dict(item.get("match_best", {}) or {})),
        }

    def _projection_identity(self, projection: MarketProjectionV2 | None) -> dict[str, Any]:
        if projection is None:
            return {
                "market_key": None,
                "side": None,
                "line": None,
                "bookmaker": None,
            }
        return {
            "market_key": projection.market_key,
            "side": projection.side,
            "line": projection.line,
            "bookmaker": projection.bookmaker,
        }

    def _light_memory_snapshot(
        self,
        *,
        fixture_id: int,
        state: MatchState,
        priority: dict[str, Any],
        match_best: dict[str, Any],
    ) -> dict[str, Any]:
        previous = dict(self._light_match_memory.get(fixture_id, {}) or {})
        current_projection = dict(match_best.get("best_projection", {}) or {})
        current_identity = {
            "market_key": current_projection.get("market_key"),
            "side": current_projection.get("side"),
            "line": current_projection.get("line"),
            "bookmaker": current_projection.get("bookmaker"),
        }
        previous_identity = {
            "market_key": previous.get("market_key"),
            "side": previous.get("side"),
            "line": previous.get("line"),
            "bookmaker": previous.get("bookmaker"),
        }
        same_best_vehicle_as_previous = bool(previous_identity["market_key"]) and previous_identity == current_identity
        previous_q_match = float(previous.get("q_match", 0.0) or 0.0)
        current_q_match = float(priority.get("q_match", 0.0) or 0.0)
        memory_snapshot = {
            "previous_score": previous.get("score"),
            "current_score": state.score_text,
            "previous_minute": previous.get("minute"),
            "current_minute": int(getattr(state, "minute", 0) or 0),
            "previous_best_projection": previous_identity,
            "current_best_projection": current_identity,
            "same_best_vehicle_as_previous": same_best_vehicle_as_previous,
            "previous_q_match": previous_q_match,
            "current_q_match": current_q_match,
            "q_match_delta": current_q_match - previous_q_match,
        }
        self._light_match_memory[fixture_id] = {
            "score": state.score_text,
            "minute": int(getattr(state, "minute", 0) or 0),
            "q_match": current_q_match,
            **current_identity,
        }
        return memory_snapshot

    def _write_export(self, payload: dict[str, Any]) -> None:
        self.export_path.parent.mkdir(parents=True, exist_ok=True)
        with self.export_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, ensure_ascii=True))
            handle.write("\n")

    def evaluate_state(self, state: MatchState) -> dict[str, Any]:
        state_for_v2 = self.quote_alias_adapter.adapt_state(state)
        intelligence = self.intelligence.build(state_for_v2)
        probability_state = self.probability.build(intelligence)
        ou_projections = self.ou_ft.translate(state_for_v2, intelligence, probability_state)
        ou_1h_projections = self.ou_1h.translate(state_for_v2, intelligence, probability_state)
        btts_projections = self.btts.translate(state_for_v2, intelligence, probability_state)
        team_total_projections = self.team_total.translate(state_for_v2, intelligence, probability_state)
        result_projections = self.result.translate(state_for_v2, intelligence, probability_state)
        projections = ou_projections + ou_1h_projections + btts_projections + team_total_projections + result_projections
        priority = self.priority.build(state_for_v2, intelligence, projections)
        match_best = self.match_arbiter.select_match_best(state_for_v2.fixture_id, projections)
        priority_dict = priority.to_dict()
        match_best_dict = match_best.to_dict()
        memory = self._light_memory_snapshot(
            fixture_id=state_for_v2.fixture_id,
            state=state_for_v2,
            priority=priority_dict,
            match_best=match_best_dict,
        )
        public_match_result = {
            "fixture_id": state_for_v2.fixture_id,
            "minute": int(getattr(state_for_v2, "minute", 0) or 0),
            "score": state_for_v2.score_text,
            "projection_counts": {
                "OU_FT": len(ou_projections),
                "OU_1H": len(ou_1h_projections),
                "BTTS": len(btts_projections),
                "TEAM_TOTAL": len(team_total_projections),
                "RESULT": len(result_projections),
            },
            "priority": self._public_priority_dict(priority_dict),
            "match_best": self._public_match_best_dict(match_best_dict),
        }

        return {
            "fixture_id": state_for_v2.fixture_id,
            "minute": int(getattr(state_for_v2, "minute", 0) or 0),
            "score": state_for_v2.score_text,
            "shadow_mode": True,
            "intelligence": intelligence.to_dict(),
            "probability": probability_state.to_dict(),
            "priority": priority_dict,
            "projections": [projection.to_dict() for projection in projections],
            "projection_counts": {
                "OU_FT": len(ou_projections),
                "OU_1H": len(ou_1h_projections),
                "BTTS": len(btts_projections),
                "TEAM_TOTAL": len(team_total_projections),
                "RESULT": len(result_projections),
            },
            "match_best": match_best_dict,
            "light_memory": memory,
            "product": public_match_result,
            "debug": {
                "priority": priority_dict,
                "match_best": match_best_dict,
                "light_memory": memory,
            },
        }

    def run_states(
        self,
        states: Iterable[MatchState],
        *,
        v1_match_documents: dict[int, Any] | None = None,
        v1_board_best: dict[str, Any] | None = None,
        source_mode: str = "runtime_shadow",
    ) -> dict[str, Any]:
        evaluated = [self.evaluate_state(state) for state in states]
        match_best_objects = [
            self.match_arbiter.select_match_best(
                item["fixture_id"],
                [MarketProjectionV2(**projection_dict) for projection_dict in item["projections"]],
            )
            for item in evaluated
        ]
        priority_by_fixture = {int(item["fixture_id"]): dict(item.get("priority", {}) or {}) for item in evaluated}
        board_best = self.board_arbiter.select_board_best(match_best_objects, priority_by_fixture=priority_by_fixture)
        generated_at_utc = datetime.now(timezone.utc).isoformat()
        board_rankings = board_best.diagnostics.get("ranking_summaries", [])
        public_shadow_alert_tier = self._public_shadow_alert_tier(board_best.shadow_alert_tier)
        public_board_best = board_best.to_public_dict(shadow_alert_tier=public_shadow_alert_tier)
        shadow_governance = {
            "shadow_alert_tier": public_shadow_alert_tier,
            "internal_shadow_alert_tier": board_best.shadow_alert_tier,
            "elite_shadow_eligible": board_best.elite_shadow_eligible,
            "watchlist_shadow_eligible": board_best.watchlist_shadow_eligible,
            "board_gate_state": board_best.board_gate_state,
            "best_fixture_id": board_best.diagnostics.get("best_fixture_id"),
            "best_match_priority": board_best.diagnostics.get("best_match_priority"),
            "gate_states": board_best.diagnostics.get("gate_states", {}),
            "elite_refusal_reasons": board_best.diagnostics.get("elite_refusal_reasons", []),
            "watchlist_refusal_reasons": board_best.diagnostics.get("watchlist_refusal_reasons", []),
            "governance_reasons": board_best.diagnostics.get("governance_reasons", []),
        }
        shadow_bundle = self.shadow_recorder.build_export_records(
            export_version=self.export_version,
            generated_at_utc=generated_at_utc,
            source_mode=source_mode,
            match_results=evaluated,
            board_best=board_best,
            board_rankings=board_rankings,
            top_bet_eligible=board_best.top_bet_eligible,
            v1_match_documents=v1_match_documents,
            v1_board_best=v1_board_best,
        )

        for export_record in shadow_bundle["export_records"]:
            fixture_id = int(export_record["fixture_id"])
            export_record["priority"] = priority_by_fixture.get(fixture_id, {})
            export_record["shadow_governance"] = shadow_governance
            export_record["shadow_alert_tier"] = public_shadow_alert_tier
            export_record["light_memory"] = next(
                (dict(item.get("light_memory", {}) or {}) for item in evaluated if int(item["fixture_id"]) == fixture_id),
                {},
            )
            export_record["product"] = {
                "shadow_alert_tier": public_shadow_alert_tier,
                "top_bet_eligible": board_best.top_bet_eligible,
                "board_best_flag": export_record["board_best_flag"],
                "priority": self._public_priority_dict(priority_by_fixture.get(fixture_id, {})),
                "match_best": self._public_match_best_dict(dict(export_record.get("match_best", {}) or {})),
                "board_best": public_board_best,
            }
            export_record["debug"] = {
                "priority": priority_by_fixture.get(fixture_id, {}),
                "light_memory": export_record["light_memory"],
                "shadow_governance": shadow_governance,
            }
            self._write_export(export_record)

        product_payload = {
            "shadow_alert_tier": public_shadow_alert_tier,
            "top_bet_eligible": board_best.top_bet_eligible,
            "board_best": public_board_best,
            "board_rankings": board_rankings,
            "match_results": [dict(item.get("product", {}) or {}) for item in evaluated],
        }
        debug_payload = {
            "board_best": board_best.to_debug_dict(),
            "board_rankings": board_rankings,
            "match_results": evaluated,
            "shadow_governance": shadow_governance,
            "shadow_comparison": shadow_bundle["shadow_comparison"],
            "comparison_summary": shadow_bundle["comparison_summary"],
        }
        payload = {
            "event": "runtime_cycle_v2",
            "export_version": self.export_version,
            "generated_at_utc": generated_at_utc,
            "shadow_mode": True,
            "source_mode": source_mode,
            "fixture_count": len(evaluated),
            "match_results": evaluated,
            "board_best": public_board_best,
            "board_rankings": board_rankings,
            "top_bet_eligible": board_best.top_bet_eligible,
            "shadow_governance": shadow_governance,
            "shadow_alert_tier": public_shadow_alert_tier,
            "shadow_comparison": shadow_bundle["shadow_comparison"],
            "comparison_summary": shadow_bundle["comparison_summary"],
            "product": product_payload,
            "debug": debug_payload,
        }
        return payload

    def run_fixture_rows(
        self,
        fixture_rows: Iterable[dict[str, Any]],
        *,
        stats_rows_by_fixture: dict[int, dict[str, Any]] | None = None,
        odds_rows_by_fixture: dict[int, list[dict[str, Any]]] | None = None,
        lineups_rows_by_fixture: dict[int, list[dict[str, Any]]] | None = None,
        players_rows_by_fixture: dict[int, list[dict[str, Any]]] | None = None,
        v1_match_documents: dict[int, Any] | None = None,
        v1_board_best: dict[str, Any] | None = None,
        source_mode: str = "runtime_shadow",
    ) -> dict[str, Any]:
        stats_rows_by_fixture = stats_rows_by_fixture or {}
        odds_rows_by_fixture = odds_rows_by_fixture or {}
        lineups_rows_by_fixture = lineups_rows_by_fixture or {}
        players_rows_by_fixture = players_rows_by_fixture or {}

        states: list[MatchState] = []
        for fixture_row in fixture_rows:
            fixture_id = int(fixture_row["fixture_id"])
            states.append(
                build_match_state(
                    fixture_row,
                    stats_row=stats_rows_by_fixture.get(fixture_id, {}),
                    odds_rows=odds_rows_by_fixture.get(fixture_id, []),
                    lineups_rows=lineups_rows_by_fixture.get(fixture_id, []),
                    players_rows=players_rows_by_fixture.get(fixture_id, []),
                )
            )
        return self.run_states(
            states,
            v1_match_documents=v1_match_documents,
            v1_board_best=v1_board_best,
            source_mode=source_mode,
        )
