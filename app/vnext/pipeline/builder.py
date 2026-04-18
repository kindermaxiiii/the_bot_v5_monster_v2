from __future__ import annotations

from app.vnext.board.models import BoardSnapshot
from app.vnext.execution.models import ExecutableMarketSelectionResult
from app.vnext.pipeline.models import PipelineSnapshot, PublishableMatchResult


def build_publishable_pipeline(
    board_snapshot: BoardSnapshot,
    execution_results: tuple[ExecutableMarketSelectionResult, ...],
) -> PipelineSnapshot:
    execution_by_fixture = {result.fixture_id: result for result in execution_results}
    results: list[PublishableMatchResult] = []
    governed_counts: dict[str, int] = {}

    for entry in board_snapshot.entries:
        governed_counts[entry.public_status] = governed_counts.get(entry.public_status, 0) + 1
        governance_refusals = entry.match_refusals + entry.board_refusals
        execution_refusals: list[str] = []
        execution_candidate = None
        selected_offer = None
        publish_status = "DO_NOT_PUBLISH"

        if entry.public_status == "NO_BET":
            publish_status = "DO_NOT_PUBLISH"
        else:
            execution_result = execution_by_fixture.get(entry.fixture_id)
            if execution_result is None:
                execution_refusals.append("execution_missing_for_match")
            elif execution_result.fixture_id != entry.fixture_id:
                execution_refusals.append("pipeline_link_mismatch")
            elif execution_result.execution_candidate is None:
                if execution_result.no_executable_vehicle_reason:
                    execution_refusals.append(execution_result.no_executable_vehicle_reason)
                else:
                    execution_refusals.append("no_executable_vehicle")
            else:
                execution_candidate = execution_result.execution_candidate
                selected_offer = execution_result.offer_chosen
                best_candidate = entry.selection_result.best_candidate if entry.selection_result else None
                expected_template_key = (
                    best_candidate.candidate.line_template.key if best_candidate is not None else None
                )
                if expected_template_key is not None and (
                    execution_result.template_key != expected_template_key
                    or execution_candidate.template_key != expected_template_key
                ):
                    execution_refusals.append("pipeline_link_mismatch")
                elif selected_offer is None:
                    execution_refusals.append("execution_offer_missing")
                else:
                    publish_status = "PUBLISH"

        prior_result = None
        if entry.selection_result is not None:
            prior_result = entry.selection_result.translation_result.posterior_result.prior_result
        home_team = prior_result.home_team_name if prior_result else "Home"
        away_team = prior_result.away_team_name if prior_result else "Away"
        competition_label = prior_result.competition_name if prior_result else "Competition"
        match_label = f"{home_team} vs {away_team}"

        results.append(
            PublishableMatchResult(
                fixture_id=entry.fixture_id,
                match_label=match_label,
                competition_label=competition_label,
                governed_public_status=entry.public_status,
                publish_status=publish_status,  # type: ignore[arg-type]
                best_candidate=entry.selection_result.best_candidate if entry.selection_result else None,
                execution_candidate=execution_candidate,
                selected_offer=selected_offer,
                governance_refusal_summary=tuple(governance_refusals),
                execution_refusal_summary=tuple(execution_refusals),
            )
        )

    publish_count = sum(1 for result in results if result.publish_status == "PUBLISH")
    do_not_publish_count = len(results) - publish_count

    return PipelineSnapshot(
        publish_count=publish_count,
        do_not_publish_count=do_not_publish_count,
        results=tuple(results),
        governed_status_counts=governed_counts,
    )
