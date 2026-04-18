from __future__ import annotations

from app.v2.arbiter.market_meta_arbiter import MarketMetaArbiter
from app.v2.contracts import BoardBestVehicle, MatchBestVehicle, MatchPrioritySnapshot


class BoardMetaArbiter:
    """
    Phase 6 board-level arbiter.

    Mission:
    - compare the best vehicle from each match
    - keep one best vehicle for the whole board
    - activate a shadow-only top_bet_eligible flag when dominance is real
    """

    min_match_dominance = 0.50
    min_board_dominance = 0.80
    min_top_bet_q_match = 6.2
    min_top_bet_q_competition = 6.1
    max_top_bet_q_noise = 3.6
    min_top_bet_q_stats = 6.4
    min_top_bet_q_odds = 5.8
    min_top_bet_stats_coherence = 0.78
    min_top_bet_executable_depth = 4
    min_top_bet_market_family_depth = 3
    elite_q_match_min = 8.0
    elite_q_competition_min = 7.2
    elite_q_noise_max = 2.8
    elite_q_stats_min = 7.0
    elite_q_odds_min = 6.4
    watchlist_q_match_min = 4.7
    watchlist_q_competition_min = 3.8
    watchlist_q_noise_max = 5.8
    watchlist_q_stats_min = 3.8
    watchlist_q_odds_min = 3.0
    watchlist_min_match_dominance = 0.08
    min_watchlist_market_findability = 0.54
    min_watchlist_publishability = 0.60
    min_elite_market_findability = 0.68
    min_elite_publishability = 0.74
    watchlist_longshot_cap_odds = 5.0
    market_family_maturity = {
        "OU_FT": "APPROVED",
        "BTTS": "APPROVED",
        "TEAM_TOTAL": "APPROVED",
        "RESULT": "PROBATION",
        "OU_1H": "LAB_ONLY",
    }

    def __init__(self, market_arbiter: MarketMetaArbiter | None = None) -> None:
        self.market_arbiter = market_arbiter or MarketMetaArbiter()

    def _coerce_priority_snapshot(self, source: object) -> MatchPrioritySnapshot | None:
        if isinstance(source, MatchPrioritySnapshot):
            return source
        if not isinstance(source, dict):
            return None
        try:
            return MatchPrioritySnapshot(
                fixture_id=int(source.get("fixture_id", 0)),
                q_match=float(source.get("q_match", 0.0)),
                q_stats=float(source.get("q_stats", 0.0)),
                q_odds=float(source.get("q_odds", 0.0)),
                q_live=float(source.get("q_live", 0.0)),
                q_competition=float(source.get("q_competition", 0.0)),
                q_noise=float(source.get("q_noise", 10.0)),
                priority_tier=str(source.get("priority_tier", "LOW_PRIORITY")),
                match_gate_state=str(source.get("match_gate_state", "DOC_ONLY")),
                diagnostics=dict(source.get("diagnostics", {}) or {}),
            )
        except (TypeError, ValueError):
            return None

    def _priority_for_fixture(
        self,
        fixture_id: int,
        priority_by_fixture: dict[int, MatchPrioritySnapshot | dict[str, object]] | None,
    ) -> MatchPrioritySnapshot | None:
        if not priority_by_fixture:
            return None
        return self._coerce_priority_snapshot(priority_by_fixture.get(fixture_id))

    def _tier_bonus(self, priority: MatchPrioritySnapshot | None) -> float:
        if priority is None:
            return 0.0
        if priority.priority_tier == "ELITE_CANDIDATE":
            return 0.55
        if priority.priority_tier == "WATCHLIST_CANDIDATE":
            return 0.08
        if priority.priority_tier == "LOW_PRIORITY":
            return -0.45
        return -1.15

    def _competition_bucket(self, priority: MatchPrioritySnapshot | None) -> str:
        if priority is None:
            return "unknown"
        return str(priority.diagnostics.get("competition", {}).get("competition_bucket", "unknown")).lower().strip()

    def _stats_coherence(self, priority: MatchPrioritySnapshot | None) -> float:
        if priority is None:
            return 0.0
        raw = priority.diagnostics.get("stats", {}).get("coherence_score")
        if raw is None:
            return 0.84 if priority.q_stats >= self.min_top_bet_q_stats else 0.70
        return float(raw or 0.0)

    def _odds_executable_depth(self, priority: MatchPrioritySnapshot | None) -> int:
        if priority is None:
            return 0
        raw = priority.diagnostics.get("odds", {}).get("executable_projection_count")
        if raw is None:
            return 4 if priority.q_odds >= self.min_top_bet_q_odds else 2
        return int(raw or 0)

    def _odds_market_family_depth(self, priority: MatchPrioritySnapshot | None) -> int:
        if priority is None:
            return 0
        raw = priority.diagnostics.get("odds", {}).get("market_family_count")
        if raw is None:
            return 3 if priority.q_odds >= self.min_top_bet_q_odds else 2
        return int(raw or 0)

    def _competition_bucket_penalty(self, priority: MatchPrioritySnapshot | None) -> float:
        bucket = self._competition_bucket(priority)
        if bucket == "very_weak":
            return 1.80
        if bucket == "weak":
            return 1.30
        if bucket == "neutral":
            return 0.20
        return 0.0

    def _match_gate_state(self, priority: MatchPrioritySnapshot | None) -> str:
        if priority is None:
            return "DOC_ONLY"
        return str(priority.match_gate_state or "DOC_ONLY")

    def _dedupe_reasons(self, reasons: list[str]) -> list[str]:
        return list(dict.fromkeys(reason for reason in reasons if reason))

    def _projection_requires_line(self, projection: object | None) -> bool:
        if projection is None:
            return False
        market_key = str(getattr(projection, "market_key", "") or "").upper().strip()
        return market_key in {"OU_FT", "OU_1H", "TEAM_TOTAL"}

    def _market_family_maturity(self, projection: MatchBestVehicle | None | object) -> str:
        if projection is None:
            return "LAB_ONLY"
        market_key = None
        if hasattr(projection, "market_key"):
            market_key = getattr(projection, "market_key", None)
        elif isinstance(projection, dict):
            market_key = projection.get("market_key")
        normalized = str(market_key or "").upper().strip()
        return self.market_family_maturity.get(normalized, "PROBATION")

    def _manual_confirmation_available(self, match_best: MatchBestVehicle | None) -> bool:
        if match_best is None or match_best.best_projection is None:
            return False
        projection = match_best.best_projection
        payload = getattr(projection, "payload", {}) or {}
        if "manual_confirmation_available" in payload:
            return bool(payload.get("manual_confirmation_available"))
        return False

    def _market_eligible(self, match_best: MatchBestVehicle | None) -> tuple[bool, list[str]]:
        if match_best is None or match_best.best_projection is None:
            return False, ["market_not_actionable"]
        projection = match_best.best_projection
        reasons: list[str] = []
        if not self.market_arbiter.is_actionable_projection(projection):
            reasons.append("market_not_actionable")
        if not projection.bookmaker:
            reasons.append("missing_bookmaker")
        if projection.odds_decimal is None:
            reasons.append("missing_price")
        if self._projection_requires_line(projection) and projection.line is None:
            reasons.append("missing_line")
        return len(reasons) == 0, self._dedupe_reasons(reasons)

    def _thesis_publishable(self, match_best: MatchBestVehicle | None) -> tuple[bool, list[str]]:
        if match_best is None or match_best.best_projection is None:
            return False, ["market_not_publishable"]
        projection = match_best.best_projection
        reasons: list[str] = []
        if projection.price_state != "VIVANT":
            reasons.append("price_not_live")
        if not projection.executable:
            reasons.append("market_not_publishable")
        if projection.edge <= 0.0 or projection.expected_value <= 0.0:
            reasons.append("market_not_publishable")
        return len(reasons) == 0, self._dedupe_reasons(reasons)

    def _match_eligible(
        self,
        priority: MatchPrioritySnapshot | None,
        *,
        allow_under_review: bool = False,
    ) -> tuple[bool, list[str]]:
        if priority is None:
            return False, ["missing_priority_snapshot"]
        reasons = list(priority.diagnostics.get("match_gate_reasons", []) or [])
        allowed_states = {"MATCH_ELIGIBLE"}
        if allow_under_review:
            allowed_states.add("MATCH_UNDER_REVIEW")
        return priority.match_gate_state in allowed_states, reasons

    def _board_score(
        self,
        match_best: MatchBestVehicle,
        priority: MatchPrioritySnapshot | None = None,
    ) -> float:
        projection = match_best.best_projection
        if projection is None:
            return -1_000_000.0

        base_score = self.market_arbiter.projection_score(projection)
        dominance_bonus = 0.85 * match_best.dominance_score
        candidate_depth_bonus = 0.02 * min(match_best.candidate_count, 8)
        q_match_bonus = 0.38 * (0.0 if priority is None else priority.q_match)
        return (
            0.80 * base_score
            + dominance_bonus
            + candidate_depth_bonus
            + q_match_bonus
            - self._competition_bucket_penalty(priority)
            + self._tier_bonus(priority)
        )

    def _is_board_viable(self, match_best: MatchBestVehicle) -> bool:
        projection = match_best.best_projection
        if projection is None:
            return False
        return self.market_arbiter.is_actionable_projection(projection)

    def _ranking_key(
        self,
        match_best: MatchBestVehicle,
        priority: MatchPrioritySnapshot | None = None,
    ) -> tuple[int, float]:
        return (1 if self._is_board_viable(match_best) else 0, self._board_score(match_best, priority))

    def _ranking_summary(
        self,
        match_best: MatchBestVehicle,
        board_score: float,
        priority: MatchPrioritySnapshot | None = None,
    ) -> dict[str, object]:
        projection = match_best.best_projection
        return {
            "fixture_id": match_best.fixture_id,
            "board_score": board_score,
            "dominance_score": match_best.dominance_score,
            "candidate_count": match_best.candidate_count,
            "q_match": None if priority is None else priority.q_match,
            "q_competition": None if priority is None else priority.q_competition,
            "q_noise": None if priority is None else priority.q_noise,
            "priority_tier": None if priority is None else priority.priority_tier,
            "market_key": None if projection is None else projection.market_key,
            "market_family_maturity": None if projection is None else self._market_family_maturity(projection),
            "side": None if projection is None else projection.side,
            "line": None if projection is None else projection.line,
            "executable": False if projection is None else projection.executable,
            "vetoes": [] if projection is None else list(projection.vetoes or []),
        }

    def _top_bet_eligible(
        self,
        best: MatchBestVehicle | None,
        *,
        priority: MatchPrioritySnapshot | None,
        board_dominance_score: float,
        viable_count: int,
    ) -> bool:
        if best is None or best.best_projection is None:
            return False
        projection = best.best_projection
        market_eligible, _ = self._market_eligible(best)
        thesis_publishable, _ = self._thesis_publishable(best)
        if viable_count < 2:
            return False
        if not market_eligible or not thesis_publishable:
            return False
        if projection.price_state != "VIVANT":
            return False
        if projection.vetoes:
            return False
        if projection.edge <= 0.0 or projection.expected_value <= 0.0:
            return False
        if priority is None:
            return False
        if priority.match_gate_state != "MATCH_ELIGIBLE":
            return False
        if priority.q_match < self.min_top_bet_q_match:
            return False
        if priority.q_competition < self.min_top_bet_q_competition:
            return False
        if priority.q_noise > self.max_top_bet_q_noise:
            return False
        if priority.q_stats < self.min_top_bet_q_stats:
            return False
        if priority.q_odds < self.min_top_bet_q_odds:
            return False
        if self._stats_coherence(priority) < self.min_top_bet_stats_coherence:
            return False
        if self._odds_executable_depth(priority) < self.min_top_bet_executable_depth:
            return False
        if self._odds_market_family_depth(priority) < self.min_top_bet_market_family_depth:
            return False
        if self._competition_bucket(priority) == "weak":
            return False
        if best.dominance_score < self.min_match_dominance:
            return False
        if board_dominance_score < self.min_board_dominance:
            return False
        return True

    def _watchlist_shadow_eligible(
        self,
        best: MatchBestVehicle | None,
        *,
        priority: MatchPrioritySnapshot | None,
    ) -> tuple[bool, list[str]]:
        if best is None or best.best_projection is None or priority is None:
            return False, ["missing_watchlist_context"]
        projection = best.best_projection
        reasons: list[str] = []
        if priority.q_match < self.watchlist_q_match_min:
            reasons.append("match_quality_below_floor")
        if priority.q_competition < self.watchlist_q_competition_min:
            reasons.append("competition_too_weak")
        if priority.q_noise > self.watchlist_q_noise_max:
            reasons.append("match_too_noisy")
        if priority.q_stats < self.watchlist_q_stats_min:
            reasons.append("insufficient_stats")
        if priority.q_odds < self.watchlist_q_odds_min:
            reasons.append("insufficient_market_depth")
        if priority.match_gate_state not in {"MATCH_ELIGIBLE", "MATCH_UNDER_REVIEW"}:
            reasons.append("match_not_eligible")
        if best.dominance_score < self.watchlist_min_match_dominance:
            reasons.append("match_not_dominant")
        if priority.priority_tier not in {"ELITE_CANDIDATE", "WATCHLIST_CANDIDATE"}:
            reasons.append("match_quality_below_floor")
        family_maturity = self._market_family_maturity(projection)
        odds_decimal = projection.odds_decimal
        manual_confirmation_available = self._manual_confirmation_available(best)

        if odds_decimal is None:
            reasons.append("missing_price")
        elif odds_decimal > self.watchlist_longshot_cap_odds:
            reasons.append("speculative_price")

        if self._projection_requires_line(projection) and projection.line is None:
            reasons.append("missing_line")

        if not manual_confirmation_available and family_maturity in {"PROBATION", "LAB_ONLY"}:
            reasons.append("family_on_probation")
        if not manual_confirmation_available and (
            odds_decimal is None or odds_decimal > self.watchlist_longshot_cap_odds
        ):
            reasons.append("manual_not_confirmed")

        return len(reasons) == 0, self._dedupe_reasons(reasons)

    def _elite_shadow_eligible(
        self,
        best: MatchBestVehicle | None,
        *,
        priority: MatchPrioritySnapshot | None,
        top_bet_eligible: bool,
    ) -> bool:
        if not top_bet_eligible or best is None or priority is None:
            return False
        return (
            priority.priority_tier == "ELITE_CANDIDATE"
            and priority.match_gate_state == "MATCH_ELIGIBLE"
            and priority.q_match >= self.elite_q_match_min
            and priority.q_competition >= self.elite_q_competition_min
            and priority.q_noise <= self.elite_q_noise_max
            and priority.q_stats >= self.elite_q_stats_min
            and priority.q_odds >= self.elite_q_odds_min
            and self._competition_bucket(priority) != "weak"
        )

    def _governance_reasons(
        self,
        *,
        best: MatchBestVehicle | None,
        priority: MatchPrioritySnapshot | None,
        top_bet_eligible: bool,
        elite_shadow_eligible: bool,
        watchlist_shadow_eligible: bool,
    ) -> list[str]:
        if best is None or best.best_projection is None:
            return ["market_not_actionable"]
        reasons: list[str] = []
        projection = best.best_projection
        market_eligible, market_reasons = self._market_eligible(best)
        thesis_publishable, thesis_reasons = self._thesis_publishable(best)
        reasons.extend(market_reasons)
        reasons.extend(thesis_reasons)
        if priority is None:
            reasons.append("match_not_eligible")
            return self._dedupe_reasons(reasons)
        if priority.match_gate_state != "MATCH_ELIGIBLE":
            reasons.append("match_not_eligible")
        if elite_shadow_eligible:
            reasons.append("elite_ready")
            return self._dedupe_reasons(reasons)
        if not top_bet_eligible:
            reasons.append("board_not_dominant")
        if priority.priority_tier != "ELITE_CANDIDATE" or priority.q_match < self.elite_q_match_min:
            reasons.append("elite_thresholds_not_met")
        if priority.q_competition < self.elite_q_competition_min or self._competition_bucket(priority) in {"weak", "very_weak"}:
            reasons.append("competition_too_weak")
        if priority.q_noise > self.elite_q_noise_max:
            reasons.append("match_too_noisy")
        if priority.q_stats < self.elite_q_stats_min:
            reasons.append("insufficient_stats")
        if priority.q_odds < self.elite_q_odds_min:
            reasons.append("insufficient_market_depth")
        if not market_eligible or not thesis_publishable:
            reasons.append("market_not_publishable")
        return self._dedupe_reasons(reasons)

    def select_board_best(
        self,
        match_best_list: list[MatchBestVehicle],
        *,
        priority_by_fixture: dict[int, MatchPrioritySnapshot | dict[str, object]] | None = None,
    ) -> BoardBestVehicle:
        if not match_best_list:
            return BoardBestVehicle(
                best_projection=None,
                match_rankings=[],
                board_dominance_score=0.0,
                top_bet_eligible=False,
                board_gate_state="NO_BET",
                shadow_alert_tier="NONE",
                elite_shadow_eligible=False,
                watchlist_shadow_eligible=False,
                diagnostics={
                    "best_fixture_id": None,
                    "best_score": 0.0,
                    "second_score": 0.0,
                    "relative_gap": 0.0,
                    "normalized_gap": 0.0,
                    "eligible_match_count": 0,
                    "ranking_summaries": [],
                },
            )

        priorities = {item.fixture_id: self._priority_for_fixture(item.fixture_id, priority_by_fixture) for item in match_best_list}
        ranked = sorted(match_best_list, key=lambda item: self._ranking_key(item, priorities.get(item.fixture_id)), reverse=True)
        viable = [item for item in ranked if self._is_board_viable(item)]
        best = viable[0] if viable else None
        best_priority = None if best is None else priorities.get(best.fixture_id)

        best_score = self._board_score(best, best_priority) if best is not None else 0.0
        second_score = self._board_score(viable[1], priorities.get(viable[1].fixture_id)) if len(viable) > 1 else best_score
        board_dominance_score = max(0.0, best_score - second_score) if len(viable) > 1 else 0.0
        normalized_gap = board_dominance_score / max(abs(best_score), 1.0)
        relative_gap_ratio = board_dominance_score / max(abs(second_score), 1.0)
        top_bet_eligible = self._top_bet_eligible(
            best,
            priority=best_priority,
            board_dominance_score=board_dominance_score,
            viable_count=len(viable),
        )
        elite_shadow_eligible = self._elite_shadow_eligible(best, priority=best_priority, top_bet_eligible=top_bet_eligible)
        watchlist_shadow_eligible, watchlist_gate_reasons = self._watchlist_shadow_eligible(best, priority=best_priority)
        match_eligible, match_gate_reasons = self._match_eligible(best_priority)
        watchlist_match_eligible, _ = self._match_eligible(best_priority, allow_under_review=True)
        market_eligible, market_gate_reasons = self._market_eligible(best)
        thesis_publishable, thesis_gate_reasons = self._thesis_publishable(best)
        if elite_shadow_eligible:
            board_gate_state = "PROMOTED_ELITE"
            shadow_alert_tier = "ELITE"
        elif watchlist_shadow_eligible and watchlist_match_eligible and market_eligible and thesis_publishable:
            board_gate_state = "PROMOTED_WATCHLIST"
            shadow_alert_tier = "WATCHLIST"
        else:
            board_gate_state = "NO_BET"
            shadow_alert_tier = "NONE"
        governance_reasons = self._governance_reasons(
            best=best,
            priority=best_priority,
            top_bet_eligible=top_bet_eligible,
            elite_shadow_eligible=elite_shadow_eligible,
            watchlist_shadow_eligible=watchlist_shadow_eligible,
        )
        elite_refusal_reasons = [] if elite_shadow_eligible else self._dedupe_reasons(
            match_gate_reasons
            + market_gate_reasons
            + thesis_gate_reasons
            + governance_reasons
        )
        watchlist_refusal_reasons = [] if shadow_alert_tier == "WATCHLIST" else list(
            self._dedupe_reasons(match_gate_reasons + market_gate_reasons + thesis_gate_reasons + watchlist_gate_reasons)
        )

        if shadow_alert_tier == "WATCHLIST" and best_priority is not None and best_priority.match_gate_state == "MATCH_UNDER_REVIEW":
            match_gate_state_value = "PASS_UNDER_REVIEW"
        else:
            match_gate_state_value = "PASS" if match_eligible else self._match_gate_state(best_priority)

        return BoardBestVehicle(
            best_projection=None if best is None else best.best_projection,
            match_rankings=ranked,
            board_dominance_score=board_dominance_score,
            top_bet_eligible=top_bet_eligible,
            board_gate_state=board_gate_state,
            shadow_alert_tier=shadow_alert_tier,
            elite_shadow_eligible=elite_shadow_eligible,
            watchlist_shadow_eligible=watchlist_shadow_eligible,
            diagnostics={
                "best_fixture_id": None if best is None else best.fixture_id,
                "best_score": best_score,
                "second_score": second_score,
                "relative_gap": board_dominance_score,
                "normalized_gap": normalized_gap,
                "relative_gap_ratio": relative_gap_ratio,
                "eligible_match_count": len(viable),
                "best_match_priority": None if best_priority is None else best_priority.to_dict(),
                "ranking_summaries": [
                    self._ranking_summary(item, self._board_score(item, priorities.get(item.fixture_id)), priorities.get(item.fixture_id))
                    for item in ranked
                ],
                "gate_states": {
                    "match_eligible": match_gate_state_value,
                    "market_eligible": "PASS" if market_eligible else "FAIL",
                    "thesis_publishable": "PASS" if thesis_publishable else "FAIL",
                    "board_promotion": board_gate_state,
                },
                "match_gate_reasons": match_gate_reasons,
                "market_gate_reasons": market_gate_reasons,
                "thesis_gate_reasons": thesis_gate_reasons,
                "elite_shadow_eligible": elite_shadow_eligible,
                "watchlist_shadow_eligible": watchlist_shadow_eligible,
                "shadow_alert_tier": shadow_alert_tier,
                "elite_refusal_reasons": elite_refusal_reasons,
                "watchlist_refusal_reasons": watchlist_refusal_reasons,
                "watchlist_gate_reasons": watchlist_gate_reasons,
                "governance_reasons": governance_reasons,
            },
        )
