from __future__ import annotations

from app.v2.contracts import MarketProjectionV2, MatchBestVehicle


class MarketMetaArbiter:
    """
    Phase 6 match-level arbiter.

    Mission:
    - compare all V2 market families within the same match
    - keep one best vehicle per match
    - expose enough diagnostics to understand why it won
    """

    hard_vetoes = {
        "quote_not_live",
        "pair_not_fully_live_same_book",
        "pair_or_triplet_not_fully_live_same_book",
        "ou_1h_not_in_first_half_window",
        "under_already_lost_at_score",
        "over_already_won_at_score",
        "btts_yes_already_won_at_score",
        "btts_no_already_lost_at_score",
        "team_total_over_already_won_at_score",
        "team_total_under_already_lost_at_score",
        "result_home_already_won_at_score",
        "result_draw_already_won_at_score",
        "result_away_already_won_at_score",
    }

    def has_hard_veto(self, projection: MarketProjectionV2) -> bool:
        vetoes = set(projection.vetoes or [])
        return any(veto in self.hard_vetoes for veto in vetoes)

    def is_actionable_projection(self, projection: MarketProjectionV2) -> bool:
        return projection.executable and not self.has_hard_veto(projection)

    def projection_score(self, projection: MarketProjectionV2) -> float:
        feed_quality = float(projection.payload.get("feed_quality", 0.58) or 0.58)
        market_quality = float(projection.payload.get("market_quality", 0.62) or 0.62)
        executable_bonus = 0.35 if projection.executable else -0.80
        vetoes = list(projection.vetoes or [])
        hard_veto_penalty = 4.00 if self.has_hard_veto(projection) else 0.0
        veto_penalty = 0.20 * len(vetoes)
        return (
            4.0 * projection.expected_value
            + 2.4 * projection.edge
            + 0.40 * projection.calibrated_probability
            + 0.18 * feed_quality
            + 0.12 * market_quality
            + executable_bonus
            - veto_penalty
            - hard_veto_penalty
            - 0.45 * projection.state_fragility_score
            - 0.25 * projection.late_fragility_score
            - 0.15 * projection.early_fragility_score
            - 0.30 * min(projection.resolution_pressure, 3.0)
        )

    def _ranking_key(self, projection: MarketProjectionV2) -> tuple[int, float]:
        return (1 if self.is_actionable_projection(projection) else 0, self.projection_score(projection))

    def _projection_summary(self, projection: MarketProjectionV2, score: float) -> dict[str, object]:
        return {
            "market_key": projection.market_key,
            "side": projection.side,
            "line": projection.line,
            "bookmaker": projection.bookmaker,
            "odds_decimal": projection.odds_decimal,
            "score": score,
            "executable": projection.executable,
            "price_state": projection.price_state,
            "hard_veto": self.has_hard_veto(projection),
            "vetoes": list(projection.vetoes or []),
        }

    def _market_count_by_key(self, candidates: list[MarketProjectionV2]) -> dict[str, int]:
        counts: dict[str, int] = {}
        for candidate in candidates:
            counts[candidate.market_key] = counts.get(candidate.market_key, 0) + 1
        return counts

    def select_match_best(self, fixture_id: int, candidates: list[MarketProjectionV2]) -> MatchBestVehicle:
        if not candidates:
            return MatchBestVehicle(
                fixture_id=fixture_id,
                best_projection=None,
                dominance_score=0.0,
                candidate_count=0,
                second_best_projection=None,
                rejected_same_match_candidates=[],
                diagnostics={
                    "best_score": 0.0,
                    "second_score": 0.0,
                    "score_gap_normalized": 0.0,
                    "market_count_by_key": {},
                    "ranked_scores": [],
                },
            )

        ranked = sorted(
            ((self.projection_score(candidate), candidate) for candidate in candidates),
            key=lambda item: self._ranking_key(item[1]),
            reverse=True,
        )
        best_score, best_projection = ranked[0]
        second_best_projection = ranked[1][1] if len(ranked) > 1 else None
        second_score = ranked[1][0] if len(ranked) > 1 else best_score
        dominance_score = max(0.0, best_score - second_score) if len(ranked) > 1 else 0.0

        return MatchBestVehicle(
            fixture_id=fixture_id,
            best_projection=best_projection,
            dominance_score=dominance_score,
            candidate_count=len(candidates),
            second_best_projection=second_best_projection,
            rejected_same_match_candidates=[candidate for _, candidate in ranked[1:]],
            diagnostics={
                "best_score": best_score,
                "second_score": second_score,
                "market_count_by_key": self._market_count_by_key(candidates),
                "ranked_scores": [self._projection_summary(candidate, score) for score, candidate in ranked],
            },
        )
