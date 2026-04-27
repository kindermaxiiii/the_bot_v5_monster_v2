
from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.operator_report import (
    ApiSportsOperatorReport,
    build_api_sports_operator_report,
)


@dataclass(frozen=True)
class ApiSportsReleaseGateConfig:
    min_ready_runs: int = 1
    min_ready_audit_bundles: int = 1
    allow_warnings: bool = False
    require_audit_bundle: bool = True

    @classmethod
    def from_env(cls) -> "ApiSportsReleaseGateConfig":
        return cls(
            min_ready_runs=_env_int("APISPORTS_RELEASE_MIN_READY_RUNS", 1),
            min_ready_audit_bundles=_env_int("APISPORTS_RELEASE_MIN_READY_AUDIT_BUNDLES", 1),
            allow_warnings=_env_bool("APISPORTS_RELEASE_ALLOW_WARNINGS", False),
            require_audit_bundle=_env_bool("APISPORTS_RELEASE_REQUIRE_AUDIT_BUNDLE", True),
        )

    def to_dict(self) -> dict[str, object]:
        return {
            "min_ready_runs": self.min_ready_runs,
            "min_ready_audit_bundles": self.min_ready_audit_bundles,
            "allow_warnings": self.allow_warnings,
            "require_audit_bundle": self.require_audit_bundle,
        }


@dataclass(frozen=True)
class ApiSportsReleaseGateCheck:
    name: str
    status: str
    message: str
    observed: Mapping[str, Any] | None = None

    def to_dict(self) -> dict[str, object]:
        return {
            "name": self.name,
            "status": self.status,
            "message": self.message,
            "observed": dict(self.observed) if self.observed is not None else None,
        }


@dataclass(frozen=True)
class ApiSportsReleaseGateDecision:
    status: str
    release_ready: bool
    generated_at_utc: str
    config: ApiSportsReleaseGateConfig
    ledger_path: str
    bundle_dir: str
    operator_report_status: str
    operator_report_ready: bool
    counts: Mapping[str, int]
    latest_ready_run_id: str | None
    latest_ready_audit_bundle_run_id: str | None
    checks: tuple[ApiSportsReleaseGateCheck, ...]
    errors: tuple[str, ...]
    operator_report: Mapping[str, Any]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "release_ready": self.release_ready,
            "generated_at_utc": self.generated_at_utc,
            "config": self.config.to_dict(),
            "ledger_path": self.ledger_path,
            "bundle_dir": self.bundle_dir,
            "operator_report_status": self.operator_report_status,
            "operator_report_ready": self.operator_report_ready,
            "counts": dict(self.counts),
            "latest_ready_run_id": self.latest_ready_run_id,
            "latest_ready_audit_bundle_run_id": self.latest_ready_audit_bundle_run_id,
            "checks": [check.to_dict() for check in self.checks],
            "errors": list(self.errors),
            "operator_report": dict(self.operator_report),
        }


class ApiSportsReleaseGateError(RuntimeError):
    pass


def evaluate_api_sports_release_gate(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
) -> ApiSportsReleaseGateDecision:
    gate_config = config or ApiSportsReleaseGateConfig.from_env()

    operator_report = build_api_sports_operator_report(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        require_audit_bundle=gate_config.require_audit_bundle,
    )

    checks = _release_checks(operator_report, gate_config)
    status = _aggregate_status(checks, allow_warnings=gate_config.allow_warnings)
    errors = tuple(
        f"{check.name}:{check.message}"
        for check in checks
        if check.status == "BLOCKED" or (check.status == "WARN" and not gate_config.allow_warnings)
    )

    latest_ready_run = operator_report.latest_ready_run or {}
    latest_ready_bundle = operator_report.latest_ready_audit_bundle or {}

    return ApiSportsReleaseGateDecision(
        status=status,
        release_ready=status in {"PASS", "WARN"},
        generated_at_utc=_utc_now(),
        config=gate_config,
        ledger_path=operator_report.ledger_path,
        bundle_dir=operator_report.bundle_dir,
        operator_report_status=operator_report.status,
        operator_report_ready=operator_report.ready,
        counts=dict(operator_report.counts),
        latest_ready_run_id=_optional_str(latest_ready_run.get("run_id")),
        latest_ready_audit_bundle_run_id=_optional_str(latest_ready_bundle.get("run_id")),
        checks=tuple(checks),
        errors=errors,
        operator_report=operator_report.to_dict(),
    )


def write_api_sports_release_gate(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    output_path: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
) -> ApiSportsReleaseGateDecision:
    decision = evaluate_api_sports_release_gate(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        config=config,
    )
    target = Path(output_path) if output_path is not None else default_release_gate_path()
    _write_json_atomic(target, decision.to_dict())
    return decision


def assert_api_sports_release_ready(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
) -> ApiSportsReleaseGateDecision:
    decision = evaluate_api_sports_release_gate(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        config=config,
    )
    if not decision.release_ready:
        raise ApiSportsReleaseGateError(f"API-Sports release gate blocked: {', '.join(decision.errors)}")
    return decision


def default_release_gate_path() -> Path:
    return Path(os.getenv("APISPORTS_RELEASE_GATE_PATH", "data/pipeline/api_sports/release_gate.json"))


def _release_checks(
    operator_report: ApiSportsOperatorReport,
    config: ApiSportsReleaseGateConfig,
) -> list[ApiSportsReleaseGateCheck]:
    counts = operator_report.counts
    checks: list[ApiSportsReleaseGateCheck] = []

    checks.append(
        _check(
            "operator_report_ready",
            "PASS" if operator_report.ready else "BLOCKED",
            "Operator readiness report must be ready before release.",
            {
                "operator_report_status": operator_report.status,
                "operator_report_ready": operator_report.ready,
            },
        )
    )

    checks.append(
        _check(
            "operator_report_status",
            "PASS"
            if operator_report.status == "PASS"
            else "WARN"
            if operator_report.status == "WARN"
            else "BLOCKED",
            "Operator report status must be acceptable for release.",
            {
                "operator_report_status": operator_report.status,
                "allow_warnings": config.allow_warnings,
            },
        )
    )

    runs_ready = int(counts.get("runs_ready", 0))
    checks.append(
        _check(
            "minimum_ready_runs",
            "PASS" if runs_ready >= config.min_ready_runs else "BLOCKED",
            "Minimum number of ready pipeline runs must be available.",
            {
                "runs_ready": runs_ready,
                "min_ready_runs": config.min_ready_runs,
            },
        )
    )

    audit_bundles_ready = int(counts.get("audit_bundles_ready", 0))
    if config.require_audit_bundle:
        audit_status = "PASS" if audit_bundles_ready >= config.min_ready_audit_bundles else "BLOCKED"
    else:
        audit_status = "PASS" if audit_bundles_ready >= config.min_ready_audit_bundles else "WARN"

    checks.append(
        _check(
            "minimum_ready_audit_bundles",
            audit_status,
            "Minimum number of ready audit evidence bundles must be available.",
            {
                "audit_bundles_ready": audit_bundles_ready,
                "min_ready_audit_bundles": config.min_ready_audit_bundles,
                "require_audit_bundle": config.require_audit_bundle,
            },
        )
    )

    latest_ready_run = operator_report.latest_ready_run or {}
    latest_ready_bundle = operator_report.latest_ready_audit_bundle or {}
    latest_ready_run_id = _optional_str(latest_ready_run.get("run_id"))
    latest_ready_bundle_id = _optional_str(latest_ready_bundle.get("run_id"))

    if latest_ready_run_id and latest_ready_bundle_id:
        match_status = "PASS" if latest_ready_run_id == latest_ready_bundle_id else "WARN"
    elif config.require_audit_bundle:
        match_status = "BLOCKED"
    else:
        match_status = "WARN"

    checks.append(
        _check(
            "latest_ready_run_bundle_alignment",
            match_status,
            "Latest ready run and latest ready audit bundle should align on run_id.",
            {
                "latest_ready_run_id": latest_ready_run_id,
                "latest_ready_audit_bundle_run_id": latest_ready_bundle_id,
                "require_audit_bundle": config.require_audit_bundle,
            },
        )
    )

    return checks


def _aggregate_status(
    checks: list[ApiSportsReleaseGateCheck],
    *,
    allow_warnings: bool,
) -> str:
    if any(check.status == "BLOCKED" for check in checks):
        return "BLOCKED"
    if any(check.status == "WARN" for check in checks):
        return "WARN" if allow_warnings else "BLOCKED"
    return "PASS"


def _check(
    name: str,
    status: str,
    message: str,
    observed: Mapping[str, Any] | None = None,
) -> ApiSportsReleaseGateCheck:
    return ApiSportsReleaseGateCheck(
        name=name,
        status=status,
        message=message,
        observed=observed,
    )


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _env_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    try:
        return int(raw)
    except ValueError as exc:
        raise ApiSportsReleaseGateError(f"{name} must be an integer.") from exc


def _env_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None or raw.strip() == "":
        return default
    return raw.strip().lower() in {"1", "true", "yes", "y", "on"}


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
