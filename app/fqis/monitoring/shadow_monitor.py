from __future__ import annotations

import json
import traceback
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any, Literal

from app.fqis.config.profiles import (
    ShadowProductionProfile,
    load_shadow_production_profile,
    shadow_production_profile_to_record,
)
from app.fqis.orchestration.shadow_runner import (
    ShadowRunnerConfig,
    ShadowRunnerOutcome,
    run_shadow_runner,
    shadow_runner_outcome_to_record,
)


ShadowRunEventType = Literal["STARTED", "COMPLETED", "FAILED"]


@dataclass(slots=True, frozen=True)
class ShadowRunEvent:
    event_type: ShadowRunEventType
    status: str
    run_id: str
    profile_name: str
    generated_at_utc: str
    detail: str
    headline: dict[str, Any]
    error: dict[str, Any] | None = None


@dataclass(slots=True, frozen=True)
class MonitoredShadowRunnerConfig:
    profile_name: str = "demo"
    profile_path: Path | None = None
    run_id: str | None = None
    outcome_output_path: Path | None = None
    latest_output_path: Path | None = None
    event_log_path: Path | None = None
    latest_status_path: Path | None = None
    write_latest: bool = True


@dataclass(slots=True, frozen=True)
class MonitoredShadowRunnerOutcome:
    status: str
    run_id: str
    profile: ShadowProductionProfile
    event_log_path: str
    latest_status_path: str
    started_at_utc: str
    completed_at_utc: str
    runner_outcome: ShadowRunnerOutcome | None
    error: dict[str, Any] | None

    @property
    def is_success(self) -> bool:
        return self.status == "ok"

    @property
    def is_go(self) -> bool:
        return bool(self.runner_outcome and self.runner_outcome.is_go)

    @property
    def readiness_status(self) -> str:
        if self.runner_outcome is None:
            return "UNKNOWN"

        return self.runner_outcome.readiness_status

    @property
    def readiness_level(self) -> str:
        if self.runner_outcome is None:
            return "UNKNOWN"

        return self.runner_outcome.readiness_level


def run_monitored_shadow_runner(
    config: MonitoredShadowRunnerConfig,
) -> MonitoredShadowRunnerOutcome:
    profile = load_shadow_production_profile(
        profile_name=config.profile_name,
        profile_path=config.profile_path,
    )
    run_id = config.run_id or _default_run_id()
    event_log_path = config.event_log_path or profile.output_root / "run_events.jsonl"
    latest_status_path = config.latest_status_path or profile.output_root / "latest_status.json"
    started_at_utc = _utc_now()

    started_event = ShadowRunEvent(
        event_type="STARTED",
        status="running",
        run_id=run_id,
        profile_name=profile.name,
        generated_at_utc=started_at_utc,
        detail="Shadow runner started.",
        headline={},
        error=None,
    )
    append_shadow_run_event(event_log_path, started_event)
    write_latest_shadow_status(
        path=latest_status_path,
        event=started_event,
        profile=profile,
        runner_outcome=None,
        error=None,
    )

    try:
        runner_outcome = run_shadow_runner(
            ShadowRunnerConfig(
                profile_name=profile.name,
                profile_path=config.profile_path,
                run_id=run_id,
                outcome_output_path=config.outcome_output_path,
                latest_output_path=config.latest_output_path,
                write_latest=config.write_latest,
            )
        )
        completed_at_utc = _utc_now()
        headline = dict(shadow_runner_outcome_to_record(runner_outcome)["headline"])

        completed_event = ShadowRunEvent(
            event_type="COMPLETED",
            status="ok",
            run_id=run_id,
            profile_name=profile.name,
            generated_at_utc=completed_at_utc,
            detail="Shadow runner completed.",
            headline=headline,
            error=None,
        )
        append_shadow_run_event(event_log_path, completed_event)
        write_latest_shadow_status(
            path=latest_status_path,
            event=completed_event,
            profile=profile,
            runner_outcome=runner_outcome,
            error=None,
        )

        return MonitoredShadowRunnerOutcome(
            status="ok",
            run_id=run_id,
            profile=profile,
            event_log_path=str(event_log_path),
            latest_status_path=str(latest_status_path),
            started_at_utc=started_at_utc,
            completed_at_utc=completed_at_utc,
            runner_outcome=runner_outcome,
            error=None,
        )

    except Exception as exc:  # noqa: BLE001 - monitoring must capture all run failures.
        completed_at_utc = _utc_now()
        error = _error_record(exc)

        failed_event = ShadowRunEvent(
            event_type="FAILED",
            status="failed",
            run_id=run_id,
            profile_name=profile.name,
            generated_at_utc=completed_at_utc,
            detail="Shadow runner failed.",
            headline={},
            error=error,
        )
        append_shadow_run_event(event_log_path, failed_event)
        write_latest_shadow_status(
            path=latest_status_path,
            event=failed_event,
            profile=profile,
            runner_outcome=None,
            error=error,
        )

        return MonitoredShadowRunnerOutcome(
            status="failed",
            run_id=run_id,
            profile=profile,
            event_log_path=str(event_log_path),
            latest_status_path=str(latest_status_path),
            started_at_utc=started_at_utc,
            completed_at_utc=completed_at_utc,
            runner_outcome=None,
            error=error,
        )


def shadow_run_event_to_record(event: ShadowRunEvent) -> dict[str, Any]:
    return {
        "source": "fqis_shadow_run_event",
        "event_type": event.event_type,
        "status": event.status,
        "run_id": event.run_id,
        "profile_name": event.profile_name,
        "generated_at_utc": event.generated_at_utc,
        "detail": event.detail,
        "headline": dict(event.headline),
        "error": dict(event.error) if event.error else None,
    }


def monitored_shadow_runner_outcome_to_record(
    outcome: MonitoredShadowRunnerOutcome,
) -> dict[str, Any]:
    runner_record = (
        shadow_runner_outcome_to_record(outcome.runner_outcome)
        if outcome.runner_outcome is not None
        else None
    )

    return {
        "source": "fqis_monitored_shadow_runner_outcome",
        "status": outcome.status,
        "is_success": outcome.is_success,
        "is_go": outcome.is_go,
        "run_id": outcome.run_id,
        "profile": shadow_production_profile_to_record(outcome.profile),
        "event_log_path": outcome.event_log_path,
        "latest_status_path": outcome.latest_status_path,
        "started_at_utc": outcome.started_at_utc,
        "completed_at_utc": outcome.completed_at_utc,
        "readiness_status": outcome.readiness_status,
        "readiness_level": outcome.readiness_level,
        "error": dict(outcome.error) if outcome.error else None,
        "runner_outcome": runner_record,
    }


def latest_shadow_status_record(
    *,
    event: ShadowRunEvent,
    profile: ShadowProductionProfile,
    runner_outcome: ShadowRunnerOutcome | None,
    error: dict[str, Any] | None,
) -> dict[str, Any]:
    runner_record = (
        shadow_runner_outcome_to_record(runner_outcome)
        if runner_outcome is not None
        else None
    )

    return {
        "source": "fqis_shadow_latest_status",
        "status": event.status,
        "event_type": event.event_type,
        "run_id": event.run_id,
        "profile": shadow_production_profile_to_record(profile),
        "generated_at_utc": event.generated_at_utc,
        "detail": event.detail,
        "headline": dict(event.headline),
        "error": dict(error) if error else None,
        "runner_outcome": runner_record,
    }


def append_shadow_run_event(path: Path, event: ShadowRunEvent) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                shadow_run_event_to_record(event),
                ensure_ascii=False,
                sort_keys=True,
            )
            + "\n"
        )

    return path


def read_shadow_run_events(path: Path) -> tuple[dict[str, Any], ...]:
    if not path.exists():
        return tuple()

    records: list[dict[str, Any]] = []

    for line in path.read_text(encoding="utf-8-sig").splitlines():
        stripped = line.strip()
        if stripped:
            records.append(json.loads(stripped))

    return tuple(records)


def write_latest_shadow_status(
    *,
    path: Path,
    event: ShadowRunEvent,
    profile: ShadowProductionProfile,
    runner_outcome: ShadowRunnerOutcome | None,
    error: dict[str, Any] | None,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            latest_shadow_status_record(
                event=event,
                profile=profile,
                runner_outcome=runner_outcome,
                error=error,
            ),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def write_monitored_shadow_runner_outcome_json(
    outcome: MonitoredShadowRunnerOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            monitored_shadow_runner_outcome_to_record(outcome),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _error_record(exc: Exception) -> dict[str, Any]:
    return {
        "error_type": exc.__class__.__name__,
        "message": str(exc),
        "traceback": traceback.format_exc(),
    }


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("fqis_shadow_production_%Y%m%d_%H%M%S")


def _utc_now() -> str:
    return datetime.now(UTC).isoformat()