from __future__ import annotations

from app.vnext.live.blocks import build_live_context_pack
from app.vnext.live.models import LiveSnapshot
from app.vnext.posterior.models import ScenarioPosteriorResult
from app.vnext.posterior.updater import update_scenario_posterior
from app.vnext.scenario.models import ScenarioPriorResult


def build_scenario_posterior_result(
    prior_result: ScenarioPriorResult,
    current_live_snapshot: LiveSnapshot,
    previous_live_snapshot: LiveSnapshot | None = None,
) -> ScenarioPosteriorResult:
    context = build_live_context_pack(current_live_snapshot, previous_live_snapshot)
    return update_scenario_posterior(prior_result, context)
