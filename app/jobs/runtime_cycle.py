from __future__ import annotations

import logging
import time
from typing import Any

from app.clients.api_football import APIFootballRequestError, api_client
from app.config import settings
from app.core.feature_engine import FeatureEngine
from app.core.hazard_engine import HazardEngine
from app.core.intensity_engine import IntensityEngine
from app.core.match_state import build_match_state
from app.core.regime_engine import RegimeEngine
from app.core.scoreline_distribution import ScorelineDistributionEngine
from app.db.models import DecisionLog, MatchSnapshot
from app.db.session import SessionLocal
from app.markets.over_under_engine import OverUnderEngine
from app.normalizers.fixtures import normalize_live_fixtures
from app.normalizers.lineups import normalize_fixture_lineups
from app.normalizers.odds import normalize_live_odds
from app.normalizers.players import normalize_fixture_players
from app.normalizers.statistics import normalize_fixture_statistics
from app.services.board_manager import BoardManager
from app.services.dispatcher import Dispatcher
from app.services.governance import GovernanceEngine
from app.utils.serialization import dumps_json_safe

logger = logging.getLogger(__name__)


class RuntimeCycle:
    def __init__(self) -> None:
        self.features = FeatureEngine()
        self.regimes = RegimeEngine()
        self.intensities = IntensityEngine()
        self.hazards = HazardEngine()
        self.distributions = ScorelineDistributionEngine()

        # Release live actuelle : OU_FT only
        self.ou = OverUnderEngine()

        self.dispatcher = Dispatcher()
        self.governance = GovernanceEngine()
        self.board = BoardManager()

        self._boot_ts = time.monotonic()
        self._cycle_index = 0
        self._fixture_observations: dict[int, int] = {}
        self._stats_cache: dict[int, tuple[float, dict[str, Any]]] = {}
        self._odds_cache: dict[int, tuple[float, list[dict[str, Any]]]] = {}

    # ------------------------------------------------------------------
    # Public run
    # ------------------------------------------------------------------
    def run_once(self) -> None:
        self._cycle_index += 1
        self.governance.start_cycle(self._cycle_index)
        self.board.reset()

        audit = {
            "fixtures_raw": 0,
            "fixtures_active": 0,
            "fixtures_in_window": 0,
            "fixtures_warm": 0,
            "snapshots_only": 0,
            "quotes_total": 0,
            "projections_raw": 0,
            "projections_actionable": 0,
            "doc_strong": 0,
            "real_valid": 0,
            "top_bet": 0,
            "blocked_warmup": 0,
            "blocked_governance": 0,
            "queued_real": 0,
            "queued_doc": 0,
            "sent_real": 0,
            "sent_doc": 0,
            "runtime_rescue": 0,
        }

        raw_live = api_client.get_live_fixtures()
        fixtures = normalize_live_fixtures(raw_live)
        audit["fixtures_raw"] = len(fixtures)

        fixtures = self._prepare_fixtures(fixtures)
        audit["fixtures_active"] = len(fixtures)

        if not fixtures:
            logger.info("runtime cycle: no active fixtures after filtering")
            self.board.export_csv()
            return

        deep_fetch_fixture_ids = self._select_deep_fetch_fixture_ids(fixtures)
        statistics_refresh_fixture_ids = self._select_statistics_refresh_fixture_ids(fixtures, deep_fetch_fixture_ids)
        statistics_analysis_fixture_ids = self._select_statistics_analysis_fixture_ids(
            fixtures,
            deep_fetch_fixture_ids,
            statistics_refresh_fixture_ids,
        )
        odds_fetch_fixture_ids = self._select_odds_fetch_fixture_ids(fixtures, statistics_analysis_fixture_ids)
        odds_priority_fixture_ids = self._select_odds_priority_fixture_ids(fixtures, odds_fetch_fixture_ids)
        prioritized_states: list[dict[str, Any]] = []

        for fixture_row in fixtures:
            fixture_id = fixture_row.get("fixture_id")

            try:
                fixture_id_int = self._int_setting_from_value(fixture_id, 0)
                if fixture_id_int not in statistics_analysis_fixture_ids or not self._should_fetch_deep_context(fixture_row):
                    state = build_match_state(
                        fixture_row,
                        stats_row={},
                        odds_rows=[],
                        lineups_rows=[],
                        players_rows=[],
                    )
                    self._persist_snapshot_only(state)
                    audit["snapshots_only"] += 1
                    continue

                next_observation_count = self._fixture_observations.get(fixture_id_int, 0) + 1
                odds_policy = self._odds_fetch_policy(
                    fixture_id_int,
                    odds_priority_fixture_ids,
                    odds_fetch_fixture_ids,
                )
                state = self._build_state(
                    fixture_row,
                    refresh_statistics=(fixture_id_int in statistics_refresh_fixture_ids),
                    include_odds=(odds_policy != "off"),
                    refresh_odds=self._should_refresh_live_odds(
                        fixture_row,
                        fixture_id_int,
                        next_observation_count,
                        odds_policy=odds_policy,
                    ),
                )
                if state is None:
                    continue

                self._fixture_observations[state.fixture_id] = self._fixture_observations.get(state.fixture_id, 0) + 1
                observation_count = self._fixture_observations[state.fixture_id]
                audit["quotes_total"] += len(getattr(state, "quotes", []) or [])

                if not self._is_inside_dispatch_window(state):
                    self._persist_snapshot_only(state)
                    audit["snapshots_only"] += 1
                    continue

                audit["fixtures_in_window"] += 1

                warmup_ready = self._warmup_ready(state, observation_count)
                if warmup_ready:
                    audit["fixtures_warm"] += 1

                feature_map = self.features.build(state)
                fixture_priority_score = self._fixture_priority_score(state, feature_map)
                prioritized_states.append(
                    {
                        "state": state,
                        "observation_count": observation_count,
                        "warmup_ready": warmup_ready,
                        "feature_map": feature_map,
                        "fixture_priority_score": fixture_priority_score,
                    }
                )

            except Exception:
                logger.exception("runtime cycle failed fixture_id=%s", fixture_id)

        prioritized_states.sort(
            key=lambda item: (
                self._float_value(item.get("fixture_priority_score"), 0.0),
                len(getattr(item.get("state"), "quotes", []) or []),
                int(getattr(item.get("state"), "minute", 0) or 0),
            ),
            reverse=True,
        )

        self._log_prioritized_fixtures(prioritized_states)

        for item in prioritized_states:
            remaining_real_slots = max(
                0,
                self._int_setting("max_real_alerts_per_cycle", 2) - audit["queued_real"],
            )
            remaining_doc_slots = max(
                0,
                self._int_setting("max_doc_alerts_per_cycle", 0) - audit["queued_doc"],
            )

            fixture_audit = self._evaluate_state(
                item["state"],
                observation_count=int(item["observation_count"]),
                warmup_ready=bool(item["warmup_ready"]),
                remaining_real_slots=remaining_real_slots,
                remaining_doc_slots=remaining_doc_slots,
                feature_map=item["feature_map"],
                fixture_priority_score=self._float_value(item["fixture_priority_score"], 0.0),
            )

            for k, v in fixture_audit.items():
                audit[k] = audit.get(k, 0) + v

        self.board.export_csv()

        logger.info(
            "cycle_audit idx=%s fixtures_raw=%s fixtures_active=%s fixtures_in_window=%s fixtures_warm=%s snapshots_only=%s "
            "quotes_total=%s projections_raw=%s actionable=%s doc_strong=%s real_valid=%s top_bet=%s "
            "warmup_blocked=%s governance_blocked=%s queued_real=%s queued_doc=%s sent_real=%s sent_doc=%s runtime_rescue=%s",
            self._cycle_index,
            audit["fixtures_raw"],
            audit["fixtures_active"],
            audit["fixtures_in_window"],
            audit["fixtures_warm"],
            audit["snapshots_only"],
            audit["quotes_total"],
            audit["projections_raw"],
            audit["projections_actionable"],
            audit["doc_strong"],
            audit["real_valid"],
            audit["top_bet"],
            audit["blocked_warmup"],
            audit["blocked_governance"],
            audit["queued_real"],
            audit["queued_doc"],
            audit["sent_real"],
            audit["sent_doc"],
            audit["runtime_rescue"],
        )

    # ------------------------------------------------------------------
    # Fixture preparation
    # ------------------------------------------------------------------
    def _prepare_fixtures(self, fixtures: list[dict[str, Any]]) -> list[dict[str, Any]]:
        prepared = []
        for row in fixtures:
            if self._is_finished_status(str(row.get("status") or "")):
                continue
            prepared.append(row)

        prepared.sort(
            key=lambda r: (self._phase_rank(str(r.get("status") or "")), int(r.get("minute") or 0)),
            reverse=True,
        )

        live_fetch_limit = self._int_setting("live_fetch_limit", 0)
        if live_fetch_limit > 0:
            prepared = prepared[:live_fetch_limit]

        max_active = self._int_setting("max_active_matches", 0)
        if max_active > 0:
            prepared = prepared[:max_active]

        return prepared

    def _phase_rank(self, status: str) -> int:
        s = (status or "").upper()
        if s == "2H":
            return 4
        if s == "HT":
            return 3
        if s == "1H":
            return 2
        if s in {"LIVE", "INPLAY"}:
            return 1
        return 0

    def _is_finished_status(self, status: str) -> bool:
        return self._bool_setting("skip_finished_statuses", True) and (status or "").upper() in {
            "FT", "AET", "PEN", "CANC", "ABD", "SUSP"
        }

    # ------------------------------------------------------------------
    # Warmup
    # ------------------------------------------------------------------
    def _warmup_ready(self, state, observation_count: int) -> bool:
        boot_age = time.monotonic() - self._boot_ts
        minute = int(getattr(state, "minute", 0) or 0)

        if boot_age < self._int_setting("boot_warmup_seconds", 90):
            return False
        if observation_count < self._int_setting("min_fixture_observations_before_dispatch", 2):
            return False
        if minute < self._int_setting("min_minute_after_boot_for_dispatch", 20):
            return False
        return True

    # ------------------------------------------------------------------
    # Fetch & build state
    # ------------------------------------------------------------------
    def _build_state(
        self,
        fixture_row: dict[str, Any],
        *,
        refresh_statistics: bool = True,
        include_odds: bool = True,
        refresh_odds: bool = True,
    ):
        fixture_id = int(fixture_row["fixture_id"])
        status = str(fixture_row.get("status") or "")
        minute = int(fixture_row.get("minute") or 0)

        stats = self._fetch_statistics(fixture_row, refresh=refresh_statistics)
        odds = self._fetch_odds(fixture_id, refresh=refresh_odds) if include_odds else []
        lineups = self._fetch_lineups(fixture_id, status=status, minute=minute)
        players = self._fetch_players(fixture_id, status=status, minute=minute)

        return build_match_state(
            fixture_row,
            stats_row=stats,
            odds_rows=odds,
            lineups_rows=lineups,
            players_rows=players,
        )

    def _fetch_statistics(self, fixture_row: dict[str, Any], *, refresh: bool = True) -> dict[str, Any]:
        if not self._bool_setting("api_include_statistics", True):
            return {}

        fixture_id = int(fixture_row["fixture_id"])
        home_team_id = fixture_row.get("home_team_id")
        away_team_id = fixture_row.get("away_team_id")
        fresh_ttl = self._int_setting("api_stats_cache_ttl_seconds", 120)
        stale_ttl = self._int_setting("api_stats_stale_cache_grace_seconds", 300)

        try:
            cached = self._cache_get_dict(self._stats_cache, fixture_id, fresh_ttl)
            if cached is not None:
                return cached

            if not refresh:
                stale = self._cache_get_dict(
                    self._stats_cache,
                    fixture_id,
                    fresh_ttl,
                    max_age_seconds=stale_ttl,
                )
                if stale is not None:
                    return stale
                return {}

            payload = normalize_fixture_statistics(
                api_client.get_fixture_statistics(fixture_id),
                expected_home_team_id=home_team_id,
                expected_away_team_id=away_team_id,
            )
            self._stats_cache[fixture_id] = (time.monotonic(), payload)
            return payload
        except APIFootballRequestError as exc:
            stale = self._cache_get_dict(
                self._stats_cache,
                fixture_id,
                fresh_ttl,
                max_age_seconds=stale_ttl,
            )
            if stale is not None:
                logger.warning("statistics stale cache fallback fixture_id=%s", fixture_id)
                return stale
            if self._is_api_backpressure_error(exc):
                logger.info("statistics fetch throttled fixture_id=%s reason=%s", fixture_id, exc)
                return {}
            logger.exception("statistics fetch failed fixture_id=%s", fixture_id)
            return {}
        except Exception:
            stale = self._cache_get_dict(
                self._stats_cache,
                fixture_id,
                fresh_ttl,
                max_age_seconds=stale_ttl,
            )
            if stale is not None:
                logger.warning("statistics stale cache fallback fixture_id=%s", fixture_id)
                return stale
            logger.exception("statistics fetch failed fixture_id=%s", fixture_id)
            return {}

    def _fetch_odds(self, fixture_id: int, *, refresh: bool = True) -> list[dict[str, Any]]:
        if not self._bool_setting("api_include_odds", True):
            return []
        fresh_ttl = self._int_setting("api_odds_cache_ttl_seconds", 60)
        stale_ttl = self._int_setting("api_odds_stale_cache_grace_seconds", 150)
        try:
            cached = self._cache_get_list(self._odds_cache, fixture_id, fresh_ttl)
            if cached is not None:
                return cached

            if not refresh:
                stale = self._cache_get_list(
                    self._odds_cache,
                    fixture_id,
                    fresh_ttl,
                    max_age_seconds=stale_ttl,
                )
                if stale is not None:
                    return stale
                return []

            payload = normalize_live_odds(api_client.get_live_odds(fixture_id))
            self._odds_cache[fixture_id] = (time.monotonic(), payload)
            return payload
        except APIFootballRequestError as exc:
            stale = self._cache_get_list(
                self._odds_cache,
                fixture_id,
                fresh_ttl,
                max_age_seconds=stale_ttl,
            )
            if stale is not None:
                logger.warning("odds stale cache fallback fixture_id=%s", fixture_id)
                return stale
            if self._is_api_backpressure_error(exc):
                logger.info("odds fetch throttled fixture_id=%s reason=%s", fixture_id, exc)
                return []
            logger.exception("odds fetch failed fixture_id=%s", fixture_id)
            return []
        except Exception:
            stale = self._cache_get_list(
                self._odds_cache,
                fixture_id,
                fresh_ttl,
                max_age_seconds=stale_ttl,
            )
            if stale is not None:
                logger.warning("odds stale cache fallback fixture_id=%s", fixture_id)
                return stale
            logger.exception("odds fetch failed fixture_id=%s", fixture_id)
            return []

    def _fetch_lineups(self, fixture_id: int, status: str, minute: int) -> list[dict[str, Any]]:
        if not self._bool_setting("api_include_lineups", False):
            return []
        if minute > 20 and (status or "").upper() not in {"NS", "1H", "HT"}:
            return []
        try:
            return normalize_fixture_lineups(api_client.get_fixture_lineups(fixture_id))
        except Exception:
            logger.exception("lineups fetch failed fixture_id=%s", fixture_id)
            return []

    def _fetch_players(self, fixture_id: int, status: str, minute: int) -> list[dict[str, Any]]:
        if not self._bool_setting("api_include_players", False):
            return []
        if minute < 55:
            return []
        try:
            return normalize_fixture_players(api_client.get_fixture_players(fixture_id))
        except Exception:
            logger.exception("players fetch failed fixture_id=%s", fixture_id)
            return []

    # ------------------------------------------------------------------
    # Dispatch window
    # ------------------------------------------------------------------
    def _is_inside_dispatch_window(self, state) -> bool:
        minute = int(getattr(state, "minute", 0) or 0)
        return self._int_setting("dispatch_minute_min", 1) <= minute <= self._int_setting("dispatch_minute_max", 92)

    # ------------------------------------------------------------------
    # Persistence only
    # ------------------------------------------------------------------
    def _persist_snapshot_only(self, state) -> None:
        with SessionLocal() as db:
            db.add(
                MatchSnapshot(
                    fixture_id=state.fixture_id,
                    minute=state.minute,
                    phase=state.phase,
                    status=state.status,
                    home_goals=state.home_goals,
                    away_goals=state.away_goals,
                    payload_json=dumps_json_safe(state.raw if self._bool_setting("store_raw_payloads", True) else None),
                )
            )
            db.commit()

    # ------------------------------------------------------------------
    # Core evaluation
    # ------------------------------------------------------------------
    def _evaluate_state(
        self,
        state,
        observation_count: int,
        warmup_ready: bool,
        remaining_real_slots: int,
        remaining_doc_slots: int,
        feature_map: dict[str, Any] | None = None,
        fixture_priority_score: float = 0.0,
    ) -> dict[str, int]:
        fixture_audit = {
            "projections_raw": 0,
            "projections_actionable": 0,
            "doc_strong": 0,
            "real_valid": 0,
            "top_bet": 0,
            "blocked_warmup": 0,
            "blocked_governance": 0,
            "queued_real": 0,
            "queued_doc": 0,
            "sent_real": 0,
            "sent_doc": 0,
            "runtime_rescue": 0,
        }

        feature_map = feature_map or self.features.build(state)
        regime = self.regimes.classify(feature_map)
        intensity = self.intensities.estimate(feature_map, regime)
        hazard = self.hazards.derive(intensity)
        distribution = self.distributions.build(state, intensity, hazard)

        # Release actuelle : OU_FT only
        projections = self.ou.evaluate(
            state,
            distribution,
            regime.regime_label,
            regime_confidence=getattr(regime, "regime_confidence", 0.60),
            chaos=(getattr(regime, "diagnostics", {}) or {}).get("chaos", 0.0),
            feature_map=feature_map,
        )

        fixture_audit["projections_raw"] = len(projections)

        if not projections:
            self._persist_snapshot_only(state)
            return fixture_audit

        quote_count = len(getattr(state, "quotes", []) or [])
        for projection in projections:
            projection.payload.setdefault("fixture_priority_score", round(fixture_priority_score, 3))
            projection.payload.setdefault("quote_count", quote_count)

            if (
                str(getattr(projection, "real_status", "") or "").upper() == "TOP_BET"
                and fixture_priority_score < self._float_setting("top_bet_min_fixture_priority_score", 6.0)
            ):
                projection.real_status = "REAL_VALID"
                projection.top_bet_flag = False
                if "fixture_priority_below_top_bet_floor" not in projection.vetoes:
                    projection.vetoes.append("fixture_priority_below_top_bet_floor")

        projections.sort(key=self._projection_rank, reverse=True)

        # Runtime rescue lane ultra stricte si rien d'actionnable ne survit
        if self._bool_setting("runtime_rescue_enabled", False) and not any(
            self._is_actionable_projection(p) for p in projections
        ):
            rescue = self._runtime_rescue_candidate(projections)
            if rescue is not None:
                rescue.real_status = "REAL_VALID"
                rescue.payload["runtime_rescue"] = True
                rescue.reasons.append("runtime_rescue_lane")
                fixture_audit["runtime_rescue"] += 1

        dispatch_queue: list = []

        with SessionLocal() as db:
            db.add(
                MatchSnapshot(
                    fixture_id=state.fixture_id,
                    minute=state.minute,
                    phase=state.phase,
                    status=state.status,
                    home_goals=state.home_goals,
                    away_goals=state.away_goals,
                    payload_json=dumps_json_safe(state.raw if self._bool_setting("store_raw_payloads", True) else None),
                )
            )

            for projection in projections:
                projection.payload.setdefault("regime_label", regime.regime_label)
                projection.payload.setdefault("regime_confidence", getattr(regime, "regime_confidence", 0.60))
                projection.payload.setdefault("chaos", (getattr(regime, "diagnostics", {}) or {}).get("chaos", 0.0))
                projection.payload.setdefault("minute", int(getattr(state, "minute", 0) or 0))
                projection.payload.setdefault("current_total", int(getattr(state, "total_goals", 0) or 0))
                projection.payload.setdefault("observation_count", observation_count)
                projection.payload.setdefault("boot_age_seconds", round(time.monotonic() - self._boot_ts, 1))
                projection.payload.setdefault("state_score", f"{state.home_goals}-{state.away_goals}")
                projection.payload.setdefault("state_status", state.status)
                projection.payload.setdefault("state_phase", state.phase)
                projection.payload.setdefault("state_fixture_id", state.fixture_id)
                projection.payload.setdefault("fixture_priority_score", round(fixture_priority_score, 3))
                projection.payload.setdefault("quote_count", quote_count)

                if projection.documentary_status == "DOC_STRONG":
                    fixture_audit["doc_strong"] += 1
                if projection.real_status == "REAL_VALID":
                    fixture_audit["real_valid"] += 1
                if projection.real_status == "TOP_BET":
                    fixture_audit["top_bet"] += 1
                if self._is_actionable_projection(projection):
                    fixture_audit["projections_actionable"] += 1

                allowed, governance_reason = self.governance.allow(state, projection)
                if not allowed:
                    if governance_reason and governance_reason not in projection.vetoes:
                        projection.vetoes.append(governance_reason)
                    fixture_audit["blocked_governance"] += 1

                actionable = self._is_actionable_projection(projection)

                if allowed and actionable and not warmup_ready:
                    if "warmup_not_ready" not in projection.vetoes:
                        projection.vetoes.append("warmup_not_ready")
                    fixture_audit["blocked_warmup"] += 1

                # Queue decision finale
                should_queue_real = False
                should_queue_doc = False

                if allowed and (not actionable or warmup_ready):
                    if projection.real_status in {"REAL_VALID", "TOP_BET"}:
                        if fixture_audit["queued_real"] >= remaining_real_slots:
                            if "max_real_alerts_per_cycle" not in projection.vetoes:
                                projection.vetoes.append("max_real_alerts_per_cycle")
                        else:
                            should_queue_real = True
                            fixture_audit["queued_real"] += 1

                    elif self._bool_setting("allow_documentary_dispatch", False) and projection.documentary_status == "DOC_STRONG":
                        if fixture_audit["queued_doc"] >= remaining_doc_slots:
                            if "max_doc_alerts_per_cycle" not in projection.vetoes:
                                projection.vetoes.append("max_doc_alerts_per_cycle")
                        else:
                            should_queue_doc = True
                            fixture_audit["queued_doc"] += 1

                # Board = après enrichissement et vetoes finaux
                self.board.add(state, projection)

                # Logging DB = après décisions finales
                db.add(
                    DecisionLog(
                        fixture_id=state.fixture_id,
                        market_key=projection.market_key,
                        side=projection.side,
                        line_value=projection.line,
                        odds_decimal=projection.odds_decimal,
                        bookmaker=projection.bookmaker,
                        regime_label=regime.regime_label,
                        p_raw=projection.raw_probability,
                        p_cal=projection.calibrated_probability,
                        p_market_no_vig=projection.market_no_vig_probability,
                        edge=projection.edge,
                        ev=projection.expected_value,
                        executable=projection.executable,
                        documentary_status=projection.documentary_status,
                        real_status=projection.real_status,
                        reasons_json=dumps_json_safe(projection.reasons),
                        vetoes_json=dumps_json_safe(projection.vetoes),
                        payload_json=dumps_json_safe(
                            projection.payload if self._bool_setting("store_raw_payloads", True) else None
                        ),
                    )
                )

                if should_queue_real or should_queue_doc:
                    self.governance.register(state, projection)
                    dispatch_queue.append(projection)

            db.commit()

        for projection in dispatch_queue:
            sent = self.dispatcher.dispatch(state, projection)
            if sent:
                self.governance.mark_dispatched(state, projection)
                if projection.real_status in {"REAL_VALID", "TOP_BET"}:
                    fixture_audit["sent_real"] += 1
                else:
                    fixture_audit["sent_doc"] += 1

        return fixture_audit

    # ------------------------------------------------------------------
    # Runtime rescue
    # ------------------------------------------------------------------
    def _runtime_rescue_candidate(self, projections: list):
        """
        Rescue lane ultra stricte :
        - OU_FT uniquement
        - OVER uniquement
        - exécutable
        - prix vivant
        - un seul but requis max
        - edge/EV positifs
        - calibration + qualité propres
        """
        best = None
        best_rank = None

        for p in projections:
            market_key = str(getattr(p, "market_key", "") or "").upper()
            side = str(getattr(p, "side", "") or "").upper()
            price_state = str(getattr(p, "price_state", "") or "").upper()
            payload = getattr(p, "payload", {}) or {}

            if market_key != "OU_FT":
                continue
            if "OVER" not in side:
                continue
            if not bool(getattr(p, "executable", False)):
                continue
            if price_state != "VIVANT":
                continue

            edge = self._float_value(getattr(p, "edge", 0.0))
            ev = self._float_value(getattr(p, "expected_value", 0.0))
            odds = self._float_value(getattr(p, "odds_decimal", 0.0))
            goals_needed = self._float_value(payload.get("goals_needed_for_over"), 99.0)
            regime = str(payload.get("regime_label", "") or "").upper()
            cal_conf = self._float_value(payload.get("calibration_confidence"), 0.0)
            feed_quality = self._float_value(
                payload.get("data_quality_score", payload.get("feed_quality")),
                0.0,
            )

            if edge < 0.040:
                continue
            if ev < 0.015:
                continue
            if not (1.45 <= odds <= 2.35):
                continue
            if goals_needed > 1.0:
                continue
            if cal_conf < 0.50:
                continue
            if feed_quality < 0.50:
                continue
            if regime not in {
                "OPEN_EXCHANGE",
                "ASYMMETRIC_SIEGE_HOME",
                "ASYMMETRIC_SIEGE_AWAY",
                "CONTROLLED_HOME_PRESSURE",
                "CONTROLLED_AWAY_PRESSURE",
            }:
                continue

            hard_vetoes = {
                "dead_price",
                "non_positive_edge",
                "invalid_calibrated_probability",
                "invalid_market_probability",
                "red_card_real_ban",
                "over_needs_too_many_goals_for_real",
                "late_over_not_clean_enough",
            }
            if any(v in hard_vetoes for v in getattr(p, "vetoes", []) or []):
                continue

            rank = (edge, ev, cal_conf, feed_quality)
            if best is None or rank > best_rank:
                best = p
                best_rank = rank

        return best

    # ------------------------------------------------------------------
    # Projection rank
    # ------------------------------------------------------------------
    def _projection_rank(self, projection) -> tuple:
        payload = getattr(projection, "payload", {}) or {}
        real_status = str(getattr(projection, "real_status", "") or "").upper()
        documentary_status = str(getattr(projection, "documentary_status", "") or "").upper()
        price_state = str(getattr(projection, "price_state", "") or "").upper()
        edge = self._float_value(getattr(projection, "edge", 0.0))
        ev = self._float_value(getattr(projection, "expected_value", 0.0))
        selection_score = self._float_value(payload.get("selection_score"), 0.0)
        display_confidence = self._float_value(payload.get("display_confidence_score"), 0.0)
        fixture_priority_score = self._float_value(payload.get("fixture_priority_score"), 0.0)
        executable = 1 if bool(getattr(projection, "executable", False)) else 0
        top_bet = 1 if bool(getattr(projection, "top_bet_flag", False)) else 0
        real_valid = 1 if real_status in {"REAL_VALID", "TOP_BET"} else 0
        doc_strong = 1 if documentary_status == "DOC_STRONG" else 0
        price_rank = 2 if price_state == "VIVANT" else 1 if price_state == "DEGRADE_MAIS_VIVANT" else 0

        return (
            top_bet,
            real_valid,
            doc_strong,
            executable,
            price_rank,
            selection_score,
            display_confidence,
            fixture_priority_score,
            edge,
            ev,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------
    def _is_actionable_projection(self, projection) -> bool:
        return (
            str(getattr(projection, "real_status", "") or "").upper() in {"REAL_VALID", "TOP_BET"}
            or str(getattr(projection, "documentary_status", "") or "").upper() == "DOC_STRONG"
        )

    def _bool_setting(self, name: str, default: bool) -> bool:
        value = getattr(settings, name, default)
        if isinstance(value, bool):
            return value
        if isinstance(value, str):
            return value.strip().lower() in {"1", "true", "yes", "y", "on"}
        return bool(value)

    def _int_setting(self, name: str, default: int) -> int:
        try:
            return int(getattr(settings, name, default) or default)
        except (TypeError, ValueError):
            return default

    def _float_value(self, value: Any, default: float = 0.0) -> float:
        try:
            if value is None:
                return default
            return float(value)
        except (TypeError, ValueError):
            return default

    def _float_setting(self, name: str, default: float) -> float:
        try:
            return float(getattr(settings, name, default) or default)
        except (TypeError, ValueError):
            return default

    def _fixture_priority_score(self, state, feature_map: dict[str, Any]) -> float:
        minute = int(getattr(state, "minute", 0) or 0)
        goal_diff = abs(int(getattr(state, "goal_diff", 0) or 0))
        total_goals = int(getattr(state, "total_goals", 0) or 0)
        quote_count = len(getattr(state, "quotes", []) or [])

        feed_quality = self._float_value(getattr(state, "feed_quality_score", 0.58), 0.58)
        competition_quality = self._float_value(getattr(state, "competition_quality_score", 0.60), 0.60)
        market_quality = self._float_value(getattr(state, "market_quality_score", 0.62), 0.62)

        two_sided_liveness = self._float_value(feature_map.get("two_sided_liveness"), 0.0)
        openness = self._float_value(feature_map.get("openness_qadj", feature_map.get("openness")), 0.0)
        recent_pressure_ratio = self._float_value(feature_map.get("recent_pressure_ratio"), 0.0)
        pressure_total = self._float_value(feature_map.get("pressure_total_qadj", feature_map.get("pressure_total")), 0.0)
        danger_confirmation = self._float_value(feature_map.get("danger_confirmation"), 0.0)

        minute_score = self._minute_priority_score(minute)
        quote_score = min(1.0, quote_count / 16.0)
        quality_score = min(
            1.0,
            0.42 * feed_quality + 0.24 * market_quality + 0.18 * competition_quality + 0.16 * quote_score,
        )

        score_state_score = 1.0
        if goal_diff >= 2:
            score_state_score = 0.62
        elif goal_diff == 1:
            score_state_score = 0.86
        if total_goals >= 5:
            score_state_score *= 0.78

        liveness_score = min(
            1.0,
            0.28 * two_sided_liveness
            + 0.20 * openness
            + 0.16 * min(1.0, recent_pressure_ratio / 1.35)
            + 0.16 * min(1.0, pressure_total / 7.5)
            + 0.12 * danger_confirmation
            + 0.08 * score_state_score,
        )

        priority = 10.0 * (
            0.42 * quality_score
            + 0.34 * liveness_score
            + 0.16 * minute_score
            + 0.08 * score_state_score
        )
        return max(0.0, min(10.0, round(priority, 3)))

    def _minute_priority_score(self, minute: int) -> float:
        if minute < 18:
            return 0.22
        if minute < 24:
            return 0.48
        if minute <= 72:
            return 1.00
        if minute <= 82:
            return 0.82
        if minute <= 88:
            return 0.58
        return 0.34

    def _log_prioritized_fixtures(self, prioritized_states: list[dict[str, Any]]) -> None:
        if not prioritized_states or not self._bool_setting("runtime_log_top_candidates", True):
            return

        limit = max(1, self._int_setting("runtime_top_candidates_limit", 3))
        samples = []
        for item in prioritized_states[:limit]:
            state = item["state"]
            samples.append(
                {
                    "fixture_id": getattr(state, "fixture_id", None),
                    "minute": getattr(state, "minute", None),
                    "score": f"{getattr(state, 'home_goals', 0)}-{getattr(state, 'away_goals', 0)}",
                    "priority": round(self._float_value(item.get("fixture_priority_score"), 0.0), 2),
                    "quotes": len(getattr(state, "quotes", []) or []),
                }
            )

        logger.info("runtime fixture priorities idx=%s top=%s", self._cycle_index, samples)

    def _select_deep_fetch_fixture_ids(self, fixtures: list[dict[str, Any]]) -> set[int]:
        eligible = [row for row in fixtures if self._should_fetch_deep_context(row)]
        limit = self._int_setting("runtime_deep_fetch_fixture_limit", 14)

        if limit <= 0 or len(eligible) <= limit:
            return {self._int_setting_from_value(row.get("fixture_id"), 0) for row in eligible}

        ranked = sorted(eligible, key=self._pre_context_priority_score, reverse=True)
        return {
            self._int_setting_from_value(row.get("fixture_id"), 0)
            for row in ranked[:limit]
            if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
        }

    def _select_statistics_refresh_fixture_ids(
        self,
        fixtures: list[dict[str, Any]],
        deep_fetch_fixture_ids: set[int],
    ) -> set[int]:
        if not self._bool_setting("api_include_statistics", True):
            return set()

        limit = self._int_setting("runtime_statistics_fetch_fixture_limit", 8)
        eligible = [
            row
            for row in fixtures
            if self._int_setting_from_value(row.get("fixture_id"), 0) in deep_fetch_fixture_ids
            and self._should_fetch_deep_context(row)
        ]

        if limit <= 0 or len(eligible) <= limit:
            return {
                self._int_setting_from_value(row.get("fixture_id"), 0)
                for row in eligible
                if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
            }

        ranked = sorted(eligible, key=self._pre_context_priority_score, reverse=True)
        return {
            self._int_setting_from_value(row.get("fixture_id"), 0)
            for row in ranked[:limit]
            if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
        }

    def _select_statistics_analysis_fixture_ids(
        self,
        fixtures: list[dict[str, Any]],
        deep_fetch_fixture_ids: set[int],
        statistics_refresh_fixture_ids: set[int],
    ) -> set[int]:
        analysis_fixture_ids = set(statistics_refresh_fixture_ids)
        stale_ttl = self._int_setting("api_stats_stale_cache_grace_seconds", 300)

        for row in fixtures:
            fixture_id = self._int_setting_from_value(row.get("fixture_id"), 0)
            if fixture_id <= 0 or fixture_id not in deep_fetch_fixture_ids:
                continue
            if fixture_id in statistics_refresh_fixture_ids:
                continue
            if not self._should_fetch_deep_context(row):
                continue
            if self._cache_has_entry(self._stats_cache, fixture_id, stale_ttl):
                analysis_fixture_ids.add(fixture_id)

        return analysis_fixture_ids

    def _select_odds_fetch_fixture_ids(
        self,
        fixtures: list[dict[str, Any]],
        statistics_analysis_fixture_ids: set[int],
    ) -> set[int]:
        if not self._bool_setting("api_include_odds", True):
            return set()

        limit = self._int_setting("runtime_odds_fetch_fixture_limit", 6)
        eligible = [
            row
            for row in fixtures
            if self._int_setting_from_value(row.get("fixture_id"), 0) in statistics_analysis_fixture_ids
            and self._should_fetch_deep_context(row)
        ]

        if limit <= 0 or len(eligible) <= limit:
            return {
                self._int_setting_from_value(row.get("fixture_id"), 0)
                for row in eligible
                if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
            }

        ranked = sorted(eligible, key=self._pre_context_priority_score, reverse=True)
        return {
            self._int_setting_from_value(row.get("fixture_id"), 0)
            for row in ranked[:limit]
            if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
        }

    def _select_odds_priority_fixture_ids(
        self,
        fixtures: list[dict[str, Any]],
        odds_fetch_fixture_ids: set[int],
    ) -> set[int]:
        if not odds_fetch_fixture_ids:
            return set()

        limit = self._int_setting("runtime_odds_priority_fixture_limit", 6)
        eligible = [
            row
            for row in fixtures
            if self._int_setting_from_value(row.get("fixture_id"), 0) in odds_fetch_fixture_ids
            and self._should_fetch_deep_context(row)
        ]

        if limit <= 0 or len(eligible) <= limit:
            return {
                self._int_setting_from_value(row.get("fixture_id"), 0)
                for row in eligible
                if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
            }

        ranked = sorted(eligible, key=self._pre_context_priority_score, reverse=True)
        return {
            self._int_setting_from_value(row.get("fixture_id"), 0)
            for row in ranked[:limit]
            if self._int_setting_from_value(row.get("fixture_id"), 0) > 0
        }

    def _odds_fetch_policy(
        self,
        fixture_id: int,
        odds_priority_fixture_ids: set[int],
        odds_fetch_fixture_ids: set[int],
    ) -> str:
        if fixture_id in odds_priority_fixture_ids:
            return "fast"
        if fixture_id in odds_fetch_fixture_ids:
            return "slow"
        return "off"

    def _should_fetch_deep_context(self, fixture_row: dict[str, Any]) -> bool:
        status = str(fixture_row.get("status") or "").upper()
        minute = self._int_setting_from_value(fixture_row.get("minute"), 0)
        min_minute = self._int_setting("runtime_deep_fetch_minute_min", 24)
        max_minute = self._int_setting("runtime_deep_fetch_minute_max", 92)

        if status in {"HT", "2H"}:
            return True
        return min_minute <= minute <= max_minute

    def _should_refresh_live_odds(
        self,
        fixture_row: dict[str, Any],
        fixture_id: int,
        next_observation_count: int,
        *,
        odds_policy: str,
    ) -> bool:
        if not self._bool_setting("api_include_odds", True):
            return False
        if odds_policy == "off":
            return False

        if not self._bool_setting("runtime_skip_odds_until_warmup_ready", True):
            return odds_policy == "fast" or (odds_policy == "slow" and next_observation_count % 2 == 0)

        boot_age = time.monotonic() - self._boot_ts
        minute = self._int_setting_from_value(fixture_row.get("minute"), 0)
        min_minute = max(
            self._int_setting("dispatch_minute_min", 1),
            self._int_setting("min_minute_after_boot_for_dispatch", 20),
        )
        dispatch_max = self._int_setting("dispatch_minute_max", 92)
        pre_context_priority_score = self._pre_context_priority_score(fixture_row) * 10.0
        warmup_ready = (
            boot_age >= self._int_setting("boot_warmup_seconds", 90)
            and next_observation_count >= self._int_setting("min_fixture_observations_before_dispatch", 2)
            and min_minute <= minute <= dispatch_max
        )

        if minute < self._int_setting("runtime_deep_fetch_minute_min", 24):
            return False
        if minute > dispatch_max:
            return False

        if warmup_ready:
            if odds_policy == "fast":
                return True
            if self._cache_get_list(
                self._odds_cache,
                fixture_id,
                self._int_setting("api_odds_cache_ttl_seconds", 25),
            ) is not None:
                return False
            return next_observation_count % 2 == 0

        if odds_policy != "fast":
            return False
        return pre_context_priority_score >= self._float_setting("runtime_pre_warmup_odds_priority_min", 7.2)

    def _pre_context_priority_score(self, fixture_row: dict[str, Any]) -> float:
        status = str(fixture_row.get("status") or "").upper()
        minute = self._int_setting_from_value(fixture_row.get("minute"), 0)
        home_goals = self._int_setting_from_value(fixture_row.get("home_goals"), 0)
        away_goals = self._int_setting_from_value(fixture_row.get("away_goals"), 0)
        total_goals = home_goals + away_goals
        goal_diff = abs(home_goals - away_goals)

        phase_score = min(1.0, max(0.20, self._phase_rank(status) / 4.0))
        minute_score = self._minute_priority_score(minute)

        score_state_score = 1.0
        if goal_diff >= 2:
            score_state_score = 0.64
        elif goal_diff == 1:
            score_state_score = 0.88

        if total_goals >= 5:
            score_state_score *= 0.76
        elif total_goals == 0 and minute < 28:
            score_state_score *= 0.82

        return (
            0.42 * minute_score
            + 0.30 * phase_score
            + 0.28 * score_state_score
        )

    def _cache_get_dict(
        self,
        cache: dict[int, tuple[float, dict[str, Any]]],
        fixture_id: int,
        ttl_seconds: int,
        max_age_seconds: int | None = None,
    ) -> dict[str, Any] | None:
        row = cache.get(fixture_id)
        if not row:
            return None
        ts, payload = row
        age_seconds = time.monotonic() - ts
        max_age = max(1, max_age_seconds if max_age_seconds is not None else ttl_seconds)
        if age_seconds > max_age:
            cache.pop(fixture_id, None)
            return None
        if max_age_seconds is None and age_seconds > max(1, ttl_seconds):
            return None
        return dict(payload)

    def _cache_has_entry(
        self,
        cache: dict[int, tuple[float, Any]],
        fixture_id: int,
        max_age_seconds: int,
    ) -> bool:
        row = cache.get(fixture_id)
        if not row:
            return False
        ts = row[0]
        age_seconds = time.monotonic() - ts
        if age_seconds > max(1, max_age_seconds):
            cache.pop(fixture_id, None)
            return False
        return True

    def _cache_get_list(
        self,
        cache: dict[int, tuple[float, list[dict[str, Any]]]],
        fixture_id: int,
        ttl_seconds: int,
        max_age_seconds: int | None = None,
    ) -> list[dict[str, Any]] | None:
        row = cache.get(fixture_id)
        if not row:
            return None
        ts, payload = row
        age_seconds = time.monotonic() - ts
        max_age = max(1, max_age_seconds if max_age_seconds is not None else ttl_seconds)
        if age_seconds > max_age:
            cache.pop(fixture_id, None)
            return None
        if max_age_seconds is None and age_seconds > max(1, ttl_seconds):
            return None
        return list(payload)

    def _int_setting_from_value(self, value: Any, default: int) -> int:
        try:
            if value is None:
                return default
            return int(value)
        except (TypeError, ValueError):
            return default

    def _is_api_backpressure_error(self, exc: Exception) -> bool:
        text = str(exc).strip().lower()
        return "cooldown active" in text or "rate limited" in text or "too many requests" in text
