from __future__ import annotations

from collections.abc import Mapping
from typing import Any

from app.v2.contracts import (
    BoardBestVehicle,
    ShadowBoardComparison,
    ShadowComparisonSummary,
    ShadowMatchComparison,
)


class ShadowRecorder:
    """
    Documentary-only comparison layer between V1-style shadow selections and V2 outputs.
    """

    market_key_aliases = {
        "1X2": "RESULT",
        "ML": "RESULT",
        "MONEYLINE": "RESULT",
        "RESULT": "RESULT",
        "OU": "OU_FT",
        "OVER_UNDER": "OU_FT",
        "OU_FT": "OU_FT",
        "OU_1H": "OU_1H",
        "BTTS": "BTTS",
        "TEAM_TOTAL": "TEAM_TOTAL",
    }

    result_side_aliases = {
        "1": "HOME",
        "HOME": "HOME",
        "H": "HOME",
        "X": "DRAW",
        "DRAW": "DRAW",
        "D": "DRAW",
        "2": "AWAY",
        "AWAY": "AWAY",
        "A": "AWAY",
    }

    def _coerce_fixture_id(self, value: object) -> int | None:
        try:
            if value is None:
                return None
            return int(value)
        except (TypeError, ValueError):
            return None

    def _coerce_line(self, value: object) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _normalize_market_key(self, value: object) -> str | None:
        if value is None:
            return None
        key = str(value).strip().upper()
        return self.market_key_aliases.get(key, key or None)

    def _normalize_side(self, market_key: str | None, value: object) -> str | None:
        if value is None:
            return None
        side = str(value).strip().upper()
        if market_key == "RESULT":
            return self.result_side_aliases.get(side, side or None)
        return side or None

    def _extract_projection_fields(self, source: Any) -> dict[str, Any]:
        if source is None:
            return {
                "fixture_id": None,
                "market_key": None,
                "side": None,
                "line": None,
                "bookmaker": None,
                "odds_decimal": None,
                "diagnostics": {},
            }

        if hasattr(source, "to_dict"):
            raw = source.to_dict()
        elif isinstance(source, Mapping):
            raw = dict(source)
        else:
            raw = {
                "fixture_id": getattr(source, "fixture_id", None),
                "market_key": getattr(source, "market_key", None),
                "side": getattr(source, "side", None),
                "line": getattr(source, "line", None),
                "bookmaker": getattr(source, "bookmaker", None),
                "odds_decimal": getattr(source, "odds_decimal", None),
                "diagnostics": getattr(source, "diagnostics", {}) or {},
            }

        market_key = self._normalize_market_key(raw.get("market_key"))
        return {
            "fixture_id": self._coerce_fixture_id(raw.get("fixture_id")),
            "market_key": market_key,
            "side": self._normalize_side(market_key, raw.get("side")),
            "line": self._coerce_line(raw.get("line")),
            "bookmaker": raw.get("bookmaker"),
            "odds_decimal": raw.get("odds_decimal"),
            "diagnostics": dict(raw.get("diagnostics", {}) or {}),
        }

    def _match_v1_inputs(self, v1_match_documents: Mapping[int, Any] | None) -> dict[int, dict[str, Any]]:
        if not v1_match_documents:
            return {}

        matched: dict[int, dict[str, Any]] = {}
        for key, value in v1_match_documents.items():
            fixture_id = self._coerce_fixture_id(key)
            if fixture_id is None:
                fixture_id = self._coerce_fixture_id(getattr(value, "fixture_id", None))
            if fixture_id is None and isinstance(value, Mapping):
                fixture_id = self._coerce_fixture_id(value.get("fixture_id"))
            if fixture_id is None:
                continue
            matched[fixture_id] = self._extract_projection_fields(value)
        return matched

    def build_shadow_comparison(
        self,
        *,
        match_results: list[dict[str, Any]],
        board_best: BoardBestVehicle,
        v1_match_documents: Mapping[int, Any] | None = None,
        v1_board_best: Any = None,
    ) -> dict[str, Any]:
        v1_match_map = self._match_v1_inputs(v1_match_documents)
        v2_board_best_projection = None if board_best.best_projection is None else board_best.best_projection.to_dict()
        v2_board_best_fields = self._extract_projection_fields(
            {
                "fixture_id": board_best.diagnostics.get("best_fixture_id"),
                **(v2_board_best_projection or {}),
            }
        )
        v1_board_best_fields = self._extract_projection_fields(v1_board_best)
        board_best_fixture_id = self._coerce_fixture_id(board_best.diagnostics.get("best_fixture_id"))

        match_level: list[dict[str, Any]] = []
        compared_fixture_ids: list[int] = []
        unmatched_v2_fixture_ids: list[int] = []

        for item in match_results:
            fixture_id = int(item["fixture_id"])
            compared_fixture_ids.append(fixture_id)
            v2_best_projection = item.get("match_best", {}).get("best_projection")
            v2_best_fields = self._extract_projection_fields({"fixture_id": fixture_id, **(v2_best_projection or {})})
            v1_best_fields = v1_match_map.get(fixture_id, self._extract_projection_fields(None))

            same_market_family = (
                v1_best_fields["market_key"] is not None
                and v2_best_fields["market_key"] is not None
                and v1_best_fields["market_key"] == v2_best_fields["market_key"]
            )
            same_direction = same_market_family and v1_best_fields["side"] == v2_best_fields["side"]
            if v1_best_fields["market_key"] is None:
                unmatched_v2_fixture_ids.append(fixture_id)

            divergence_reasons: list[str] = []
            if v1_best_fields["market_key"] is None:
                divergence_reasons.append("missing_v1_reference")
            elif not same_market_family:
                divergence_reasons.append("market_family_differs")
            elif not same_direction:
                divergence_reasons.append("side_differs")

            comparison = ShadowMatchComparison(
                fixture_id=fixture_id,
                v1_best_market_key=v1_best_fields["market_key"],
                v1_best_side=v1_best_fields["side"],
                v1_best_line=v1_best_fields["line"],
                v2_best_market_key=v2_best_fields["market_key"],
                v2_best_side=v2_best_fields["side"],
                v2_best_line=v2_best_fields["line"],
                same_market_family=same_market_family,
                same_direction=same_direction,
                v2_board_best_flag=fixture_id == board_best_fixture_id,
                v2_top_bet_eligible=board_best.top_bet_eligible,
                diagnostics={
                    "v1_reference_present": v1_best_fields["market_key"] is not None,
                    "divergence_reasons": divergence_reasons,
                    "v2_dominance_score": item.get("match_best", {}).get("dominance_score"),
                    "v2_candidate_count": item.get("match_best", {}).get("candidate_count"),
                },
            )
            match_level.append(comparison.to_dict())

        board_same_market_family = (
            v1_board_best_fields["market_key"] is not None
            and v2_board_best_fields["market_key"] is not None
            and v1_board_best_fields["market_key"] == v2_board_best_fields["market_key"]
        )
        board_same_direction = board_same_market_family and v1_board_best_fields["side"] == v2_board_best_fields["side"]

        board_level = ShadowBoardComparison(
            v1_best_fixture_id=v1_board_best_fields["fixture_id"],
            v1_best_market_key=v1_board_best_fields["market_key"],
            v1_best_side=v1_board_best_fields["side"],
            v1_best_line=v1_board_best_fields["line"],
            v2_best_fixture_id=v2_board_best_fields["fixture_id"],
            v2_best_market_key=v2_board_best_fields["market_key"],
            v2_best_side=v2_board_best_fields["side"],
            v2_best_line=v2_board_best_fields["line"],
            same_market_family=board_same_market_family,
            same_direction=board_same_direction,
            diagnostics={
                "v1_reference_present": v1_board_best_fields["market_key"] is not None,
                "v2_board_dominance_score": board_best.board_dominance_score,
                "v2_top_bet_eligible": board_best.top_bet_eligible,
                "board_divergence_reasons": (
                    []
                    if board_same_market_family and board_same_direction
                    else (
                        ["missing_v1_board_reference"]
                        if v1_board_best_fields["market_key"] is None
                        else (
                            ["board_market_family_differs"]
                            if not board_same_market_family
                            else ["board_side_differs"]
                        )
                    )
                ),
            },
        ).to_dict()

        diagnostics = {
            "compared_fixture_ids": compared_fixture_ids,
            "matched_v1_fixture_ids": sorted(fixture_id for fixture_id in v1_match_map if fixture_id in compared_fixture_ids),
            "unmatched_v1_fixture_ids": sorted(fixture_id for fixture_id in v1_match_map if fixture_id not in compared_fixture_ids),
            "unmatched_v2_fixture_ids": sorted(unmatched_v2_fixture_ids),
        }

        return {
            "match_level": match_level,
            "board_level": board_level,
            "diagnostics": diagnostics,
        }

    def build_comparison_summary(self, shadow_comparison: Mapping[str, Any]) -> dict[str, Any]:
        match_level = list(shadow_comparison.get("match_level", []) or [])
        board_level = dict(shadow_comparison.get("board_level", {}) or {})
        diagnostics = dict(shadow_comparison.get("diagnostics", {}) or {})

        compared_match_count = sum(1 for item in match_level if item.get("v1_best_market_key") and item.get("v2_best_market_key"))
        same_market_family_count = sum(1 for item in match_level if bool(item.get("same_market_family")))
        same_direction_count = sum(1 for item in match_level if bool(item.get("same_direction")))
        v2_divergence_count = sum(
            1
            for item in match_level
            if item.get("v1_best_market_key") is not None
            and (
                not bool(item.get("same_market_family"))
                or not bool(item.get("same_direction"))
            )
        )
        board_best_difference_count = int(
            board_level.get("v1_best_market_key") is not None
            and (
                not bool(board_level.get("same_market_family"))
                or not bool(board_level.get("same_direction"))
                or board_level.get("v1_best_fixture_id") != board_level.get("v2_best_fixture_id")
            )
        )
        v2_top_bet_eligible_true_count = int(bool(board_level.get("diagnostics", {}).get("v2_top_bet_eligible")))

        summary = ShadowComparisonSummary(
            compared_match_count=compared_match_count,
            same_market_family_count=same_market_family_count,
            same_direction_count=same_direction_count,
            v2_divergence_count=v2_divergence_count,
            board_best_difference_count=board_best_difference_count,
            v2_top_bet_eligible_true_count=v2_top_bet_eligible_true_count,
            diagnostics={
                "total_v2_match_count": len(match_level),
                "unmatched_v1_fixture_count": len(diagnostics.get("unmatched_v1_fixture_ids", [])),
                "unmatched_v2_fixture_count": len(diagnostics.get("unmatched_v2_fixture_ids", [])),
            },
        )
        return summary.to_dict()

    def build_scoped_shadow_comparison(
        self,
        shadow_comparison: Mapping[str, Any],
        *,
        fixture_ids: list[int] | None = None,
    ) -> dict[str, Any]:
        fixture_id_set = {int(fixture_id) for fixture_id in (fixture_ids or [])}
        all_match_level = list(shadow_comparison.get("match_level", []) or [])
        scoped_match_level = (
            [item for item in all_match_level if int(item.get("fixture_id")) in fixture_id_set]
            if fixture_ids is not None
            else all_match_level
        )
        diagnostics = dict(shadow_comparison.get("diagnostics", {}) or {})
        diagnostics["scoped_fixture_ids"] = sorted(fixture_id_set) if fixture_ids is not None else diagnostics.get("compared_fixture_ids", [])
        return {
            "match_level": scoped_match_level,
            "board_level": dict(shadow_comparison.get("board_level", {}) or {}),
            "diagnostics": diagnostics,
        }

    def build_export_records(
        self,
        *,
        export_version: str,
        generated_at_utc: str,
        source_mode: str,
        match_results: list[dict[str, Any]],
        board_best: BoardBestVehicle,
        board_rankings: list[dict[str, Any]],
        top_bet_eligible: bool,
        v1_match_documents: Mapping[int, Any] | None = None,
        v1_board_best: Any = None,
    ) -> dict[str, Any]:
        shadow_comparison = self.build_shadow_comparison(
            match_results=match_results,
            board_best=board_best,
            v1_match_documents=v1_match_documents,
            v1_board_best=v1_board_best,
        )
        comparison_summary = self.build_comparison_summary(shadow_comparison)

        export_records: list[dict[str, Any]] = []
        for item in match_results:
            fixture_id = int(item["fixture_id"])
            scoped_shadow_comparison = self.build_scoped_shadow_comparison(shadow_comparison, fixture_ids=[fixture_id])
            export_records.append(
                {
                    "export_version": export_version,
                    "generated_at_utc": generated_at_utc,
                    "shadow_mode": True,
                    "source_mode": source_mode,
                    "fixture_id": fixture_id,
                    "minute": item["minute"],
                    "score": item["score"],
                    "fixture_priority_score": item["intelligence"]["fixture_priority_score"],
                    "regime_label": item["intelligence"]["regime_label"],
                    "candidate_count": item["match_best"]["candidate_count"],
                    "projection_counts": item["projection_counts"],
                    "dominance_score": item["match_best"]["dominance_score"],
                    "match_best": item["match_best"],
                    "best_projection": item["match_best"]["best_projection"],
                    "second_best_projection": item["match_best"]["second_best_projection"],
                    "market_count_by_key": item["match_best"]["diagnostics"].get("market_count_by_key", {}),
                    "rejected_same_match_candidates": item["match_best"]["rejected_same_match_candidates"],
                    "board_best_flag": fixture_id == board_best.diagnostics.get("best_fixture_id"),
                    "board_best": None if board_best.best_projection is None else board_best.best_projection.to_dict(),
                    "board_rankings": board_rankings,
                    "board_dominance_score": board_best.board_dominance_score,
                    "top_bet_eligible": top_bet_eligible,
                    "shadow_comparison": scoped_shadow_comparison,
                    "comparison_summary": comparison_summary,
                }
            )

        return {
            "shadow_comparison": shadow_comparison,
            "comparison_summary": comparison_summary,
            "export_records": export_records,
        }
