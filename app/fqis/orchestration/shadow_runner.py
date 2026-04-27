from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.fqis.config.profiles import (
    ShadowProductionProfile,
    load_shadow_production_profile,
    shadow_production_profile_to_record,
)
from app.fqis.orchestration.shadow_production import (
    ShadowProductionOutcome,
    run_shadow_production,
    shadow_production_outcome_to_record,
    write_shadow_production_outcome_json,
)


@dataclass(slots=True, frozen=True)
class ShadowRunnerConfig:
    profile_name: str = "demo"
    profile_path: Path | None = None
    run_id: str | None = None
    outcome_output_path: Path | None = None
    latest_output_path: Path | None = None
    write_latest: bool = True


@dataclass(slots=True, frozen=True)
class ShadowRunnerOutcome:
    status: str
    runner_id: str
    generated_at_utc: str
    profile: ShadowProductionProfile
    shadow_outcome: ShadowProductionOutcome
    outcome_path: str
    latest_path: str | None

    @property
    def run_id(self) -> str:
        return self.shadow_outcome.run_id

    @property
    def readiness_status(self) -> str:
        return self.shadow_outcome.readiness_status

    @property
    def readiness_level(self) -> str:
        return self.shadow_outcome.readiness_level

    @property
    def is_go(self) -> bool:
        return self.shadow_outcome.is_go


def run_shadow_runner(config: ShadowRunnerConfig) -> ShadowRunnerOutcome:
    profile = load_shadow_production_profile(
        profile_name=config.profile_name,
        profile_path=config.profile_path,
    )

    shadow_outcome = run_shadow_production(
        profile.to_config(run_id=config.run_id)
    )

    outcome_path = config.outcome_output_path or Path(shadow_outcome.output_dir) / "shadow_outcome.json"
    write_shadow_production_outcome_json(shadow_outcome, outcome_path)

    latest_path: Path | None = None

    if config.write_latest:
        latest_path = config.latest_output_path or profile.output_root / "latest.json"
        write_shadow_production_outcome_json(shadow_outcome, latest_path)

    return ShadowRunnerOutcome(
        status="ok",
        runner_id=_default_runner_id(),
        generated_at_utc=datetime.now(UTC).isoformat(),
        profile=profile,
        shadow_outcome=shadow_outcome,
        outcome_path=str(outcome_path),
        latest_path=str(latest_path) if latest_path else None,
    )


def shadow_runner_outcome_to_record(outcome: ShadowRunnerOutcome) -> dict[str, Any]:
    shadow_record = shadow_production_outcome_to_record(outcome.shadow_outcome)

    return {
        "status": outcome.status,
        "source": "fqis_shadow_runner_outcome",
        "runner_id": outcome.runner_id,
        "generated_at_utc": outcome.generated_at_utc,
        "profile": shadow_production_profile_to_record(outcome.profile),
        "run_id": outcome.run_id,
        "readiness_status": outcome.readiness_status,
        "readiness_level": outcome.readiness_level,
        "is_go": outcome.is_go,
        "paths": {
            "outcome_path": outcome.outcome_path,
            "latest_path": outcome.latest_path,
            **shadow_record["paths"],
        },
        "headline": dict(shadow_record["headline"]),
        "shadow_production_outcome": shadow_record,
    }


def write_shadow_runner_outcome_json(
    outcome: ShadowRunnerOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            shadow_runner_outcome_to_record(outcome),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _default_runner_id() -> str:
    return datetime.now(UTC).strftime("fqis_shadow_runner_%Y%m%d_%H%M%S")
    