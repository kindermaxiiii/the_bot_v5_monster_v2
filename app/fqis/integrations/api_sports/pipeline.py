
from __future__ import annotations

import hashlib
import json
import os
import subprocess
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import Any, Callable, Mapping, Sequence


class ApiSportsPipelineStatus(str, Enum):
    DRY_RUN = "DRY_RUN"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


class ApiSportsPipelineStepName(str, Enum):
    QUALITY_GATE = "quality_gate"
    REPLAY = "replay"


class ApiSportsPipelineStepStatus(str, Enum):
    DRY_RUN = "DRY_RUN"
    COMPLETED = "COMPLETED"
    FAILED = "FAILED"


@dataclass(frozen=True)
class ApiSportsPipelineCommandResult:
    return_code: int
    stdout: str = ""
    stderr: str = ""


CommandRunner = Callable[[Sequence[str]], ApiSportsPipelineCommandResult]


@dataclass(frozen=True)
class ApiSportsPipelineConfig:
    normalized_input: Path | None = None
    output_dir: Path = Path(os.getenv("APISPORTS_PIPELINE_OUTPUT_DIR", "data/pipeline/api_sports"))
    python_executable: str = sys.executable
    run_id: str | None = None
    strict_quality: bool = False
    dry_run: bool = False

    def resolved_run_id(self) -> str:
        if self.run_id:
            return self.run_id
        return datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")

    def to_dict(self) -> dict[str, object]:
        return {
            "normalized_input": str(self.normalized_input) if self.normalized_input is not None else None,
            "output_dir": str(self.output_dir),
            "python_executable": self.python_executable,
            "run_id": self.run_id,
            "strict_quality": self.strict_quality,
            "dry_run": self.dry_run,
        }


@dataclass(frozen=True)
class ApiSportsPipelineStepPlan:
    name: ApiSportsPipelineStepName
    command: tuple[str, ...]
    output_path: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name.value,
            "command": list(self.command),
            "output_path": self.output_path,
        }


@dataclass(frozen=True)
class ApiSportsPipelineStepResult:
    name: ApiSportsPipelineStepName
    status: ApiSportsPipelineStepStatus
    command: tuple[str, ...]
    started_at_utc: str | None = None
    completed_at_utc: str | None = None
    return_code: int | None = None
    stdout_tail: str = ""
    stderr_tail: str = ""
    output_path: str | None = None
    reason: str | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name.value,
            "status": self.status.value,
            "command": list(self.command),
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "return_code": self.return_code,
            "stdout_tail": self.stdout_tail,
            "stderr_tail": self.stderr_tail,
            "output_path": self.output_path,
            "reason": self.reason,
        }


@dataclass(frozen=True)
class ApiSportsPipelineManifest:
    run_id: str
    status: ApiSportsPipelineStatus
    ready: bool
    run_dir: str
    normalized_input: str | None
    payload_sha256: str | None
    config: Mapping[str, object]
    started_at_utc: str
    completed_at_utc: str
    steps: tuple[ApiSportsPipelineStepResult, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "run_id": self.run_id,
            "status": self.status.value,
            "ready": self.ready,
            "run_dir": self.run_dir,
            "normalized_input": self.normalized_input,
            "payload_sha256": self.payload_sha256,
            "config": dict(self.config),
            "started_at_utc": self.started_at_utc,
            "completed_at_utc": self.completed_at_utc,
            "steps": [step.to_dict() for step in self.steps],
            "errors": list(self.errors),
        }


class ApiSportsPipelineError(RuntimeError):
    pass


class ApiSportsPipelineRunner:
    def __init__(
        self,
        config: ApiSportsPipelineConfig,
        *,
        command_runner: CommandRunner | None = None,
    ) -> None:
        self.config = config
        self.run_id = config.resolved_run_id()
        self.run_dir = config.output_dir / self.run_id
        self.command_runner = command_runner or _subprocess_runner

    def plan(self) -> tuple[ApiSportsPipelineStepPlan, ...]:
        normalized_input = str(self.config.normalized_input) if self.config.normalized_input is not None else ""
        quality_report_path = self.run_dir / "quality_report.json"

        quality_command = [
            self.config.python_executable,
            "scripts/fqis_api_sports_quality_gate.py",
            "--input",
            normalized_input,
            "--output",
            str(quality_report_path),
        ]
        if self.config.strict_quality:
            quality_command.append("--strict")

        replay_command = [
            self.config.python_executable,
            "scripts/fqis_api_sports_replay.py",
            "--input",
            normalized_input,
        ]

        return (
            ApiSportsPipelineStepPlan(
                name=ApiSportsPipelineStepName.QUALITY_GATE,
                command=tuple(quality_command),
                output_path=str(quality_report_path),
            ),
            ApiSportsPipelineStepPlan(
                name=ApiSportsPipelineStepName.REPLAY,
                command=tuple(replay_command),
                output_path=None,
            ),
        )

    def run(self) -> ApiSportsPipelineManifest:
        started_at = _utc_now()
        self.run_dir.mkdir(parents=True, exist_ok=True)

        steps: list[ApiSportsPipelineStepResult] = []
        payload_sha256 = _sha256_file(self.config.normalized_input) if _path_exists(self.config.normalized_input) else None

        for step_plan in self.plan():
            if self.config.dry_run:
                steps.append(
                    ApiSportsPipelineStepResult(
                        name=step_plan.name,
                        status=ApiSportsPipelineStepStatus.DRY_RUN,
                        command=step_plan.command,
                        output_path=step_plan.output_path,
                        reason="dry_run",
                    )
                )
                continue

            input_failure = self._input_failure()
            if input_failure is not None:
                steps.append(
                    ApiSportsPipelineStepResult(
                        name=step_plan.name,
                        status=ApiSportsPipelineStepStatus.FAILED,
                        command=step_plan.command,
                        output_path=step_plan.output_path,
                        reason=input_failure,
                    )
                )
                break

            result = self._execute_step(step_plan)
            steps.append(result)
            if result.status is ApiSportsPipelineStepStatus.FAILED:
                break

        status = _manifest_status(steps, dry_run=self.config.dry_run)
        errors = tuple(step.reason for step in steps if step.status is ApiSportsPipelineStepStatus.FAILED and step.reason)

        manifest = ApiSportsPipelineManifest(
            run_id=self.run_id,
            status=status,
            ready=status is ApiSportsPipelineStatus.COMPLETED,
            run_dir=str(self.run_dir),
            normalized_input=str(self.config.normalized_input) if self.config.normalized_input is not None else None,
            payload_sha256=payload_sha256,
            config=self.config.to_dict(),
            started_at_utc=started_at,
            completed_at_utc=_utc_now(),
            steps=tuple(steps),
            errors=errors,
        )

        _write_json(self.run_dir / "pipeline_manifest.json", manifest.to_dict())
        return manifest

    def _input_failure(self) -> str | None:
        if self.config.normalized_input is None:
            return "NO_NORMALIZED_INPUT"
        if not self.config.normalized_input.exists():
            return f"INPUT_NOT_FOUND: {self.config.normalized_input}"
        return None

    def _execute_step(self, step_plan: ApiSportsPipelineStepPlan) -> ApiSportsPipelineStepResult:
        started_at = _utc_now()
        try:
            result = self.command_runner(step_plan.command)
        except Exception as exc:
            return ApiSportsPipelineStepResult(
                name=step_plan.name,
                status=ApiSportsPipelineStepStatus.FAILED,
                command=step_plan.command,
                started_at_utc=started_at,
                completed_at_utc=_utc_now(),
                output_path=step_plan.output_path,
                reason=f"COMMAND_EXCEPTION: {type(exc).__name__}: {exc}",
            )

        status = (
            ApiSportsPipelineStepStatus.COMPLETED
            if result.return_code == 0
            else ApiSportsPipelineStepStatus.FAILED
        )

        return ApiSportsPipelineStepResult(
            name=step_plan.name,
            status=status,
            command=step_plan.command,
            started_at_utc=started_at,
            completed_at_utc=_utc_now(),
            return_code=result.return_code,
            stdout_tail=_tail(result.stdout),
            stderr_tail=_tail(result.stderr),
            output_path=step_plan.output_path,
            reason=None if result.return_code == 0 else f"COMMAND_FAILED: return_code={result.return_code}",
        )


def build_api_sports_pipeline_runner(
    *,
    normalized_input: str | Path | None,
    output_dir: str | Path | None = None,
    run_id: str | None = None,
    strict_quality: bool = False,
    dry_run: bool = False,
) -> ApiSportsPipelineRunner:
    return ApiSportsPipelineRunner(
        ApiSportsPipelineConfig(
            normalized_input=Path(normalized_input) if normalized_input is not None else None,
            output_dir=Path(output_dir) if output_dir is not None else Path(os.getenv("APISPORTS_PIPELINE_OUTPUT_DIR", "data/pipeline/api_sports")),
            run_id=run_id,
            strict_quality=strict_quality,
            dry_run=dry_run,
        )
    )


def _subprocess_runner(command: Sequence[str]) -> ApiSportsPipelineCommandResult:
    completed = subprocess.run(
        list(command),
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
        check=False,
    )
    return ApiSportsPipelineCommandResult(
        return_code=completed.returncode,
        stdout=completed.stdout or "",
        stderr=completed.stderr or "",
    )


def _manifest_status(
    steps: Sequence[ApiSportsPipelineStepResult],
    *,
    dry_run: bool,
) -> ApiSportsPipelineStatus:
    if dry_run:
        return ApiSportsPipelineStatus.DRY_RUN
    if any(step.status is ApiSportsPipelineStepStatus.FAILED for step in steps):
        return ApiSportsPipelineStatus.FAILED
    return ApiSportsPipelineStatus.COMPLETED


def _path_exists(path: Path | None) -> bool:
    return path is not None and path.exists() and path.is_file()


def _sha256_file(path: Path | None) -> str | None:
    if path is None or not path.exists():
        return None
    return hashlib.sha256(path.read_bytes()).hexdigest()


def _write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _tail(value: str, limit: int = 4_000) -> str:
    if len(value) <= limit:
        return value
    return value[-limit:]
