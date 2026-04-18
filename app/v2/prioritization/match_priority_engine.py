from __future__ import annotations

from app.core.match_state import MatchState, TeamLiveStats
from app.v2.contracts import MatchIntelligenceSnapshot, MatchPrioritySnapshot, MarketProjectionV2


def _clamp(value: float, low: float, high: float) -> float:
    return max(low, min(high, value))


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        if value is None:
            return default
        return float(value)
    except (TypeError, ValueError):
        return default


def _safe_int(value: object, default: int = 0) -> int:
    try:
        if value is None:
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


class MatchPriorityEngine:
    """
    Match-level structural priority layer.

    Mission:
    - separate match quality from market quality
    - produce explicit eligibility floors
    - enrich projections with lightweight findability/publishability governance
    """

    elite_q_match_min = 6.9
    elite_q_competition_min = 6.8
    elite_q_noise_max = 3.2
    elite_q_odds_min = 5.8
    elite_q_stats_min = 5.5

    watchlist_q_match_min = 4.9
    watchlist_q_competition_min = 3.8
    watchlist_q_noise_max = 5.8
    watchlist_q_odds_min = 3.0
    stat_presence_floor = 0.64
    stat_coherence_floor = 0.72
    late_stat_activity_floor = 0.18
    early_stat_activity_floor = 0.10
    odds_quote_floor = 4
    odds_family_floor = 2
    odds_executable_floor = 3
    min_watchlist_findability = 0.54
    min_watchlist_publishability = 0.60

    tier_a_tokens = (
        "premier league",
        "la liga",
        "bundesliga",
        "serie a",
        "ligue 1",
        "champions league",
        "europa league",
        "eredivisie",
        "primeira liga",
        "championship",
    )
    tier_b_tokens = (
        "ligue 2",
        "serie b",
        "segunda division",
        "segunda liga",
        "jupiler",
        "superliga",
        "super league",
        "mls",
        "liga portugal 2",
        "pro league",
    )
    tier_c_tokens = (
        "first division",
        "second division",
        "national",
        "liga 2",
        "superettan",
        "eliteserien",
        "allsvenskan",
    )
    weak_tokens = (
        "liga 3",
        "3. liga",
        "third division",
        "division 3",
        "reserve",
        "reserves",
        "u19",
        "u20",
        "u21",
        "regional",
        "youth",
    )
    very_weak_tokens = (
        "liga 3",
        "u19",
        "u20",
        "u21",
        "youth",
        "regional",
    )
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
    findability_base_by_market = {
        "OU_FT": 0.86,
        "BTTS": 0.88,
        "TEAM_TOTAL": 0.76,
        "RESULT": 0.79,
        "OU_1H": 0.69,
    }

    def _avg(self, values: list[float]) -> float:
        if not values:
            return 0.0
        return _clamp(sum(values) / len(values), 0.0, 1.0)

    def _activity_floor(self, state: MatchState) -> float:
        minute = _safe_int(getattr(state, "minute", 0), 0)
        return self.early_stat_activity_floor if minute < 20 else self.late_stat_activity_floor

    def _is_projection_hard_veto(self, projection: MarketProjectionV2) -> bool:
        return any(veto in self.hard_vetoes for veto in (projection.vetoes or []))

    def _projection_findability_score(
        self,
        projection: MarketProjectionV2,
        *,
        bookmaker_count: int,
    ) -> tuple[float, list[str]]:
        score = self.findability_base_by_market.get(projection.market_key, 0.62)
        reasons: list[str] = []

        if projection.bookmaker:
            score += 0.08
        else:
            reasons.append("missing_bookmaker_feed")
            score -= 0.20

        if projection.odds_decimal is not None:
            score += 0.06
        else:
            reasons.append("missing_odds_decimal")
            score -= 0.18

        if projection.market_key in {"OU_FT", "OU_1H", "TEAM_TOTAL"}:
            if projection.line is None:
                reasons.append("missing_market_line")
                score -= 0.18
            else:
                score += 0.05
        else:
            score += 0.03

        if projection.price_state == "VIVANT":
            score += 0.10
        elif projection.price_state == "DEGRADE_MAIS_VIVANT":
            reasons.append("degraded_feed_state")
            score -= 0.08
        else:
            reasons.append("dead_feed_state")
            score -= 0.18

        if projection.executable:
            score += 0.10
        else:
            reasons.append("projection_not_executable")
            score -= 0.16

        if bookmaker_count <= 1:
            reasons.append("single_book_market")
            score -= 0.14

        if self._is_projection_hard_veto(projection):
            reasons.append("hard_veto_present")
            score -= 0.16

        if projection.market_key == "OU_1H":
            score -= 0.04

        return _clamp(score, 0.0, 1.0), reasons

    def _projection_publishability_score(
        self,
        projection: MarketProjectionV2,
        *,
        intelligence: MatchIntelligenceSnapshot,
        findability_score: float,
    ) -> tuple[float, list[str]]:
        reasons: list[str] = []
        executable_score = 1.0 if projection.executable else 0.0
        live_price_score = 1.0 if projection.price_state == "VIVANT" else (0.40 if projection.price_state == "DEGRADE_MAIS_VIVANT" else 0.0)
        positive_value_score = 1.0 if projection.edge > 0.0 and projection.expected_value > 0.0 else 0.0
        veto_penalty = 0.18 if self._is_projection_hard_veto(projection) else 0.08 * min(len(projection.vetoes or []), 2)

        score = (
            0.32 * findability_score
            + 0.22 * executable_score
            + 0.18 * live_price_score
            + 0.14 * positive_value_score
            + 0.08 * _clamp(intelligence.feed_quality, 0.0, 1.0)
            + 0.06 * _clamp(intelligence.market_quality, 0.0, 1.0)
            - veto_penalty
        )

        if projection.price_state != "VIVANT":
            reasons.append("price_state_not_vivant")
        if not projection.executable:
            reasons.append("projection_not_executable")
        if projection.edge <= 0.0 or projection.expected_value <= 0.0:
            reasons.append("non_positive_value")
        if self._is_projection_hard_veto(projection):
            reasons.append("hard_veto_present")

        return _clamp(score, 0.0, 1.0), reasons

    def enrich_projections(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        projections: list[MarketProjectionV2],
    ) -> list[MarketProjectionV2]:
        bookmaker_count = len({projection.bookmaker for projection in projections if projection.bookmaker})

        for projection in projections:
            findability_score, findability_reasons = self._projection_findability_score(
                projection,
                bookmaker_count=bookmaker_count,
            )
            publishability_score, publishability_reasons = self._projection_publishability_score(
                projection,
                intelligence=intelligence,
                findability_score=findability_score,
            )
            reasons_of_refusal = list(dict.fromkeys(findability_reasons + publishability_reasons))

            if (
                not projection.bookmaker
                or projection.odds_decimal is None
                or projection.price_state == "MORT"
                or self._is_projection_hard_veto(projection)
                or not projection.executable
            ):
                market_gate_state = "MARKET_REJECTED"
            elif findability_score < self.min_watchlist_findability:
                market_gate_state = "MARKET_REVIEW"
                reasons_of_refusal.append("low_market_findability")
            else:
                market_gate_state = "MARKET_ELIGIBLE"

            if market_gate_state == "MARKET_REJECTED" or publishability_score < 0.46:
                thesis_gate_state = "DOC_ONLY"
                reasons_of_refusal.append("low_publishability")
            elif publishability_score < self.min_watchlist_publishability:
                thesis_gate_state = "WATCHLIST_ONLY"
                reasons_of_refusal.append("publishability_below_watchlist_floor")
            else:
                thesis_gate_state = "PUBLISHABLE"

            projection.market_findability_score = findability_score
            projection.publishability_score = publishability_score
            projection.reasons_of_refusal = list(dict.fromkeys(reasons_of_refusal))
            projection.market_gate_state = market_gate_state
            projection.thesis_gate_state = thesis_gate_state

        return projections

    def _team_stat_presence(self, team: TeamLiveStats) -> float:
        metrics = [
            team.shots_total,
            team.shots_on_target,
            team.shots_inside_box,
            team.corners,
            team.dangerous_attacks,
            team.attacks,
            team.possession,
        ]
        present = sum(1 for value in metrics if value not in (None, ""))
        return _clamp(present / len(metrics), 0.0, 1.0)

    def _team_activity_signal(self, team: TeamLiveStats) -> float:
        shots_signal = _clamp((_safe_int(team.shots_total) + 1.5 * _safe_int(team.shots_on_target)) / 14.0, 0.0, 1.0)
        territory_signal = _clamp((_safe_int(team.dangerous_attacks) + 0.35 * _safe_int(team.attacks)) / 48.0, 0.0, 1.0)
        corners_signal = _clamp(_safe_int(team.corners) / 7.0, 0.0, 1.0)
        return _clamp(0.50 * shots_signal + 0.35 * territory_signal + 0.15 * corners_signal, 0.0, 1.0)

    def _stats_coherence_score(self, state: MatchState) -> float:
        checks: list[float] = []

        for team in (state.home, state.away):
            shots_total = _safe_int(team.shots_total)
            shots_on = _safe_int(team.shots_on_target)
            shots_inside_box = _safe_int(team.shots_inside_box)
            dangerous_attacks = _safe_int(team.dangerous_attacks)
            attacks = _safe_int(team.attacks)

            checks.append(1.0 if shots_on <= max(shots_total, shots_on) else 0.0)
            checks.append(1.0 if shots_inside_box <= max(shots_total, shots_inside_box) else 0.0)
            checks.append(1.0 if dangerous_attacks <= max(attacks, dangerous_attacks) else 0.0)

        home_possession = getattr(state.home, "possession", None)
        away_possession = getattr(state.away, "possession", None)
        if home_possession is None or away_possession is None:
            checks.append(0.65)
        else:
            possession_sum = _safe_float(home_possession) + _safe_float(away_possession)
            checks.append(1.0 if 92.0 <= possession_sum <= 108.0 else 0.45)

        if not checks:
            return 0.0
        return _clamp(sum(checks) / len(checks), 0.0, 1.0)

    def _q_stats(self, state: MatchState) -> tuple[float, dict[str, float | bool]]:
        home_presence = self._team_stat_presence(state.home)
        away_presence = self._team_stat_presence(state.away)
        presence_score = (home_presence + away_presence) / 2.0
        coherence_score = self._stats_coherence_score(state)
        home_activity = self._team_activity_signal(state.home)
        away_activity = self._team_activity_signal(state.away)
        bilateral_signal = min(home_activity, away_activity)
        depth_signal = _clamp((home_activity + away_activity) / 1.35, 0.0, 1.0)
        max_activity = max(home_activity, away_activity)
        activity_floor = self._activity_floor(state)
        stat_richness_floor_passed = (
            presence_score >= self.stat_presence_floor
            and coherence_score >= self.stat_coherence_floor
            and max_activity >= activity_floor
        )

        q_stats = 10.0 * (
            0.24 * presence_score
            + 0.24 * coherence_score
            + 0.22 * bilateral_signal
            + 0.20 * depth_signal
            + 0.10 * max_activity
        )
        if not stat_richness_floor_passed:
            q_stats *= 0.78

        return _clamp(q_stats, 0.0, 10.0), {
            "presence_score": presence_score,
            "coherence_score": coherence_score,
            "home_activity": home_activity,
            "away_activity": away_activity,
            "max_activity": max_activity,
            "activity_floor": activity_floor,
            "stat_richness_floor_passed": stat_richness_floor_passed,
        }

    def _q_odds(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        projections: list[MarketProjectionV2],
    ) -> tuple[float, dict[str, float | int | bool]]:
        if not state.quotes and not projections:
            return 0.0, {
                "quote_count": 0,
                "projection_count": 0,
                "executable_projection_count": 0,
                "bookmaker_count": 0,
                "market_family_count": 0,
                "findability_avg": 0.0,
                "publishability_avg": 0.0,
                "odds_depth_floor_passed": False,
            }

        quote_count = len(state.quotes or [])
        market_family_count = len({projection.market_key for projection in projections})
        executable_projection_count = sum(1 for projection in projections if projection.executable and projection.price_state == "VIVANT")
        bookmaker_count = len({projection.bookmaker for projection in projections if projection.bookmaker})
        findability_avg = self._avg([float(projection.market_findability_score or 0.0) for projection in projections])
        publishability_avg = self._avg([float(projection.publishability_score or 0.0) for projection in projections])
        quote_depth = _clamp(quote_count / 18.0, 0.0, 1.0)
        family_depth = _clamp(market_family_count / 5.0, 0.0, 1.0)
        executable_depth = _clamp(executable_projection_count / 10.0, 0.0, 1.0)
        bookmaker_depth = _clamp(bookmaker_count / 3.0, 0.0, 1.0)
        market_quality = _clamp(intelligence.market_quality, 0.0, 1.0)
        single_book_penalty = 0.26 if bookmaker_count <= 1 and len(projections) > 0 else 0.0
        thin_family_penalty = 0.12 if market_family_count < 3 else 0.0
        thin_executable_penalty = 0.14 if executable_projection_count < self.odds_executable_floor else 0.0
        odds_depth_floor_passed = (
            quote_count >= self.odds_quote_floor
            and market_family_count >= self.odds_family_floor
            and executable_projection_count >= self.odds_executable_floor
        )

        q_odds = 10.0 * (
            0.24 * executable_depth
            + 0.22 * family_depth
            + 0.18 * bookmaker_depth
            + 0.16 * market_quality
            + 0.12 * quote_depth
            - single_book_penalty
            - thin_family_penalty
            - thin_executable_penalty
        )
        if not odds_depth_floor_passed:
            q_odds *= 0.82
        return _clamp(q_odds, 0.0, 10.0), {
            "quote_count": quote_count,
            "projection_count": len(projections),
            "executable_projection_count": executable_projection_count,
            "bookmaker_count": bookmaker_count,
            "market_family_count": market_family_count,
            "findability_avg": findability_avg,
            "publishability_avg": publishability_avg,
            "single_book_penalty": single_book_penalty,
            "thin_family_penalty": thin_family_penalty,
            "thin_executable_penalty": thin_executable_penalty,
            "odds_depth_floor_passed": odds_depth_floor_passed,
        }

    def _minute_usefulness(self, state: MatchState) -> float:
        minute = _safe_int(getattr(state, "minute", 0), 0)
        phase = str(getattr(state, "phase", "") or getattr(state, "status", "")).upper().strip()

        if phase == "1H":
            if minute < 12:
                return 0.30
            if minute <= 35:
                return 0.88
            return 0.62
        if phase == "HT":
            return 0.46
        if phase == "2H":
            if minute <= 50:
                return 0.72
            if minute <= 78:
                return 0.94
            if minute <= 88:
                return 0.58
            return 0.34
        return 0.40

    def _q_live(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        projections: list[MarketProjectionV2],
    ) -> tuple[float, dict[str, float]]:
        minute_score = self._minute_usefulness(state)
        goal_gap = abs(state.home_goals - state.away_goals)
        if goal_gap == 0:
            score_usefulness = 1.0
        elif goal_gap == 1:
            score_usefulness = 0.82
        elif goal_gap == 2:
            score_usefulness = 0.58
        else:
            score_usefulness = 0.35

        intensity_score = _clamp(
            0.28 * ((intelligence.pressure_home + intelligence.pressure_away) / 2.0)
            + 0.24 * ((intelligence.threat_home + intelligence.threat_away) / 2.0)
            + 0.20 * intelligence.openness
            + 0.12 * intelligence.chaos
            + 0.16 * _clamp(intelligence.remaining_goal_expectancy / 2.4, 0.0, 1.0),
            0.0,
            1.0,
        )
        executable_family_count = len({projection.market_key for projection in projections if projection.executable})
        resolution_score = _clamp(executable_family_count / 5.0, 0.0, 1.0)

        q_live = 10.0 * (
            0.28 * minute_score
            + 0.22 * score_usefulness
            + 0.30 * intensity_score
            + 0.20 * resolution_score
        )
        return _clamp(q_live, 0.0, 10.0), {
            "minute_score": minute_score,
            "score_usefulness": score_usefulness,
            "intensity_score": intensity_score,
            "resolution_score": resolution_score,
        }

    def _competition_profile(self, state: MatchState) -> dict[str, object]:
        full_name = f"{state.country_name} {state.competition_name}".lower().strip()
        base_score = _clamp(_safe_float(getattr(state, "competition_quality_score", 0.60), 0.60) * 10.0, 0.0, 10.0)

        if any(token in full_name for token in self.very_weak_tokens):
            return {
                "competition_bucket": "very_weak",
                "competition_tier": "D",
                "competition_whitelisted": False,
                "score": _clamp(base_score - 3.1, 0.0, 10.0),
            }
        if any(token in full_name for token in self.weak_tokens):
            return {
                "competition_bucket": "weak",
                "competition_tier": "D",
                "competition_whitelisted": False,
                "score": _clamp(base_score - 2.6, 0.0, 10.0),
            }
        if any(token in full_name for token in self.tier_a_tokens):
            return {
                "competition_bucket": "elite",
                "competition_tier": "A",
                "competition_whitelisted": True,
                "score": _clamp(base_score + 1.2, 0.0, 10.0),
            }
        if any(token in full_name for token in self.tier_b_tokens):
            return {
                "competition_bucket": "strong",
                "competition_tier": "B",
                "competition_whitelisted": True,
                "score": _clamp(base_score + 0.6, 0.0, 10.0),
            }
        if any(token in full_name for token in self.tier_c_tokens) or base_score >= 6.0:
            return {
                "competition_bucket": "neutral",
                "competition_tier": "C",
                "competition_whitelisted": True,
                "score": _clamp(base_score - 0.1, 0.0, 10.0),
            }
        return {
            "competition_bucket": "neutral",
            "competition_tier": "D",
            "competition_whitelisted": False,
            "score": _clamp(base_score - 1.1, 0.0, 10.0),
        }

    def _competition_bucket(self, state: MatchState) -> tuple[str, float]:
        profile = self._competition_profile(state)
        return str(profile["competition_bucket"]), float(profile["score"])

    def _q_competition(self, state: MatchState) -> tuple[float, dict[str, object]]:
        profile = self._competition_profile(state)
        return float(profile["score"]), {
            "competition_bucket": profile["competition_bucket"],
            "competition_tier": profile["competition_tier"],
            "competition_whitelisted": profile["competition_whitelisted"],
            "competition_name": state.competition_name,
            "country_name": state.country_name,
        }

    def _q_noise(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        projections: list[MarketProjectionV2],
        *,
        q_stats: float,
        q_odds: float,
        odds_diag: dict[str, object],
    ) -> tuple[float, dict[str, float]]:
        competition_profile = self._competition_profile(state)
        projection_count = max(1, len(projections))
        degraded_ratio = sum(1 for projection in projections if projection.price_state != "VIVANT" or not projection.executable) / projection_count
        veto_ratio = sum(1 for projection in projections if projection.vetoes) / projection_count
        missing_stats = 1.0 - _clamp(q_stats / 10.0, 0.0, 1.0)
        low_odds_quality = 1.0 - _clamp(q_odds / 10.0, 0.0, 1.0)
        feed_gap = 1.0 - _clamp(intelligence.feed_quality, 0.0, 1.0)
        market_gap = 1.0 - _clamp(intelligence.market_quality, 0.0, 1.0)
        structural_penalty = (
            0.40
            if competition_profile["competition_bucket"] == "weak"
            else (0.65 if competition_profile["competition_bucket"] == "very_weak" else 0.0)
        )
        contradiction = 0.0
        if len(state.quotes or []) == 0 and intelligence.remaining_goal_expectancy > 0.95:
            contradiction += 0.55
        if _safe_int(state.home.shots_total) + _safe_int(state.away.shots_total) == 0 and intelligence.openness >= 0.55:
            contradiction += 0.45
        contradiction = _clamp(contradiction, 0.0, 1.0)
        findability_gap = 1.0 - _clamp(_safe_float(odds_diag.get("findability_avg"), 0.0), 0.0, 1.0)
        publishability_gap = 1.0 - _clamp(_safe_float(odds_diag.get("publishability_avg"), 0.0), 0.0, 1.0)

        q_noise = 10.0 * (
            0.18 * feed_gap
            + 0.14 * market_gap
            + 0.16 * missing_stats
            + 0.14 * low_odds_quality
            + 0.14 * degraded_ratio
            + 0.08 * veto_ratio
            + 0.08 * contradiction
            + 0.04 * findability_gap
            + 0.04 * publishability_gap
            + 0.10 * structural_penalty
        )
        return _clamp(q_noise, 0.0, 10.0), {
            "feed_gap": feed_gap,
            "market_gap": market_gap,
            "missing_stats": missing_stats,
            "low_odds_quality": low_odds_quality,
            "degraded_ratio": degraded_ratio,
            "veto_ratio": veto_ratio,
            "contradiction": contradiction,
            "findability_gap": findability_gap,
            "publishability_gap": publishability_gap,
            "structural_penalty": structural_penalty,
        }

    def _match_gate_state(
        self,
        *,
        q_match: float,
        q_noise: float,
        competition_diag: dict[str, object],
        stats_diag: dict[str, object],
        odds_diag: dict[str, object],
    ) -> tuple[str, list[str]]:
        reasons: list[str] = []

        if not bool(competition_diag.get("competition_whitelisted")):
            reasons.append("competition_too_weak")
        if not bool(stats_diag.get("stat_richness_floor_passed")):
            reasons.append("insufficient_stats")
        if not bool(odds_diag.get("odds_depth_floor_passed")):
            reasons.append("insufficient_market_depth")
        if q_noise > self.watchlist_q_noise_max:
            reasons.append("match_too_noisy")
        if q_match < self.watchlist_q_match_min:
            reasons.append("match_quality_below_floor")

        if not reasons:
            return "MATCH_ELIGIBLE", reasons
        if bool(competition_diag.get("competition_whitelisted")) and q_match >= 4.0 and q_noise <= 6.5:
            return "MATCH_UNDER_REVIEW", reasons
        return "DOC_ONLY", reasons

    def _priority_tier(
        self,
        *,
        q_match: float,
        q_stats: float,
        q_odds: float,
        q_competition: float,
        q_noise: float,
        competition_diag: dict[str, object],
        stats_diag: dict[str, object],
        odds_diag: dict[str, object],
    ) -> tuple[str, list[str]]:
        reasons: list[str] = []

        if not bool(competition_diag.get("competition_whitelisted")):
            reasons.append("competition_too_weak")
            return "NOISY_DOC_ONLY", reasons

        if (
            q_match >= self.elite_q_match_min
            and q_competition >= self.elite_q_competition_min
            and q_noise <= self.elite_q_noise_max
            and q_odds >= self.elite_q_odds_min
            and q_stats >= self.elite_q_stats_min
            and bool(stats_diag.get("stat_richness_floor_passed"))
            and bool(odds_diag.get("odds_depth_floor_passed"))
        ):
            reasons.append("elite_candidate")
            return "ELITE_CANDIDATE", reasons

        if (
            q_match >= self.watchlist_q_match_min
            and q_competition >= self.watchlist_q_competition_min
            and q_noise <= self.watchlist_q_noise_max
            and q_odds >= self.watchlist_q_odds_min
        ):
            reasons.append("watchlist_candidate")
            return "WATCHLIST_CANDIDATE", reasons

        if q_match >= 4.0 and q_noise <= 6.5:
            reasons.append("low_priority")
            return "LOW_PRIORITY", reasons

        reasons.append("doc_only")
        return "NOISY_DOC_ONLY", reasons

    def build(
        self,
        state: MatchState,
        intelligence: MatchIntelligenceSnapshot,
        projections: list[MarketProjectionV2],
    ) -> MatchPrioritySnapshot:
        self.enrich_projections(state, intelligence, projections)
        q_stats, stats_diag = self._q_stats(state)
        q_odds, odds_diag = self._q_odds(state, intelligence, projections)
        q_live, live_diag = self._q_live(state, intelligence, projections)
        q_competition, competition_diag = self._q_competition(state)
        q_noise, noise_diag = self._q_noise(
            state,
            intelligence,
            projections,
            q_stats=q_stats,
            q_odds=q_odds,
            odds_diag=odds_diag,
        )
        q_match = _clamp(
            0.34 * q_stats
            + 0.18 * q_odds
            + 0.14 * q_live
            + 0.30 * q_competition
            - 0.24 * q_noise,
            0.0,
            10.0,
        )
        if not bool(stats_diag.get("stat_richness_floor_passed")):
            q_match *= 0.84
        if not bool(odds_diag.get("odds_depth_floor_passed")):
            q_match *= 0.90
        if not bool(competition_diag.get("competition_whitelisted")):
            q_match *= 0.82
        q_match = _clamp(q_match, 0.0, 10.0)
        match_gate_state, match_gate_reasons = self._match_gate_state(
            q_match=q_match,
            q_noise=q_noise,
            competition_diag=competition_diag,
            stats_diag=stats_diag,
            odds_diag=odds_diag,
        )
        priority_tier, tier_reasons = self._priority_tier(
            q_match=q_match,
            q_stats=q_stats,
            q_odds=q_odds,
            q_competition=q_competition,
            q_noise=q_noise,
            competition_diag=competition_diag,
            stats_diag=stats_diag,
            odds_diag=odds_diag,
        )

        return MatchPrioritySnapshot(
            fixture_id=state.fixture_id,
            q_match=q_match,
            q_stats=q_stats,
            q_odds=q_odds,
            q_live=q_live,
            q_competition=q_competition,
            q_noise=q_noise,
            priority_tier=priority_tier,
            match_gate_state=match_gate_state,
            diagnostics={
                "stats": stats_diag,
                "odds": odds_diag,
                "live": live_diag,
                "competition": competition_diag,
                "noise": noise_diag,
                "tier_reasons": tier_reasons,
                "match_gate_reasons": match_gate_reasons,
            },
        )
