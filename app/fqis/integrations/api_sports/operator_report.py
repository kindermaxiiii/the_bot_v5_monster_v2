
from __future__ import annotations

import json
import os
from collections.abc import Mapping
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.audit_index import (
    build_api_sports_audit_index,
    default_audit_index_path,
    select_latest_audit_bundle,
)
from app.fqis.integrations.api_sports.run_registry import ApiSportsRunRegistry


@dataclass(frozen=True)
class ApiSportsOperatorCheck:
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
class ApiSportsOperatorReport:
    status: str
    ready: bool
    generated_at_utc: str
    ledger_path: str
    bundle_dir: str
    audit_index_path: str
    counts: Mapping[str, int]
    latest_run: Mapping[str, Any] | None
    latest_ready_run: Mapping[str, Any] | None
    latest_audit_bundle: Mapping[str, Any] | None
    latest_ready_audit_bundle: Mapping[str, Any] | None
    checks: tuple[ApiSportsOperatorCheck, ...]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "ready": self.ready,
            "generated_at_utc": self.generated_at_utc,
            "ledger_path": self.ledger_path,
            "bundle_dir": self.bundle_dir,
            "audit_index_path": self.audit_index_path,
            "counts": dict(self.counts),
            "latest_run": dict(self.latest_run) if self.latest_run is not None else None,
            "latest_ready_run": dict(self.latest_ready_run) if self.latest_ready_run is not None else None,
            "latest_audit_bundle": dict(self.latest_audit_bundle) if self.latest_audit_bundle is not None else None,
            "latest_ready_audit_bundle": dict(self.latest_ready_audit_bundle) if self.latest_ready_audit_bundle is not None else None,
            "checks": [check.to_dict() for check in self.checks],
            "errors": list(self.errors),
        }


class ApiSportsOperatorReportError(RuntimeError):
    pass


def build_api_sports_operator_report(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    require_audit_bundle: bool = True,
) -> ApiSportsOperatorReport:
    registry = ApiSportsRunRegistry(ledger_path)
    registry_snapshot = registry.snapshot()

    audit_index_path = _audit_index_path_for_bundle_dir(bundle_dir)
    audit_index = build_api_sports_audit_index(
        bundle_dir=bundle_dir,
        index_path=audit_index_path,
    )

    latest_run = registry.latest()
    latest_ready_run = registry.latest(ready=True)
    latest_audit_bundle = select_latest_audit_bundle(bundle_dir=bundle_dir)
    latest_ready_audit_bundle = select_latest_audit_bundle(bundle_dir=bundle_dir, ready=True)

    checks: list[ApiSportsOperatorCheck] = []

    checks.append(
        _check(
            "run_ledger_has_entries",
            "PASS" if registry_snapshot.entries_total > 0 else "BLOCKED",
            "Run ledger contains at least one recorded pipeline run.",
            {
                "entries_total": registry_snapshot.entries_total,
                "ledger_path": str(registry.ledger_path),
            },
        )
    )

    if latest_run is None:
        checks.append(
            _check(
                "latest_run_status",
                "BLOCKED",
                "No latest pipeline run is available.",
                {"latest_run_id": None},
            )
        )
    elif latest_run.status == "COMPLETED":
        checks.append(
            _check(
                "latest_run_status",
                "PASS",
                "Latest pipeline run completed successfully.",
                {"run_id": latest_run.run_id, "status": latest_run.status},
            )
        )
    elif latest_run.status == "DRY_RUN":
        checks.append(
            _check(
                "latest_run_status",
                "WARN",
                "Latest pipeline run is a dry run.",
                {"run_id": latest_run.run_id, "status": latest_run.status},
            )
        )
    else:
        checks.append(
            _check(
                "latest_run_status",
                "BLOCKED",
                "Latest pipeline run is not completed.",
                {"run_id": latest_run.run_id, "status": latest_run.status},
            )
        )

    checks.append(
        _check(
            "latest_ready_run_exists",
            "PASS" if latest_ready_run is not None else "BLOCKED",
            "At least one ready pipeline run is available.",
            {
                "latest_ready_run_id": latest_ready_run.run_id if latest_ready_run is not None else None,
                "ready_total": registry_snapshot.ready_total,
            },
        )
    )

    if audit_index.errors:
        checks.append(
            _check(
                "audit_index_errors",
                "BLOCKED" if require_audit_bundle else "WARN",
                "Audit index was built with errors.",
                {
                    "errors_total": len(audit_index.errors),
                    "errors": list(audit_index.errors),
                    "require_audit_bundle": require_audit_bundle,
                },
            )
        )
    else:
        checks.append(
            _check(
                "audit_index_errors",
                "PASS",
                "Audit index has no build errors.",
                {"errors_total": 0},
            )
        )

    checks.append(
        _check(
            "audit_bundle_inventory",
            "PASS"
            if audit_index.bundles_total > 0
            else "BLOCKED"
            if require_audit_bundle
            else "WARN",
            "Audit bundle inventory contains at least one bundle.",
            {
                "bundles_total": audit_index.bundles_total,
                "ready_total": audit_index.ready_total,
                "require_audit_bundle": require_audit_bundle,
            },
        )
    )

    checks.append(
        _check(
            "latest_ready_audit_bundle_exists",
            "PASS"
            if latest_ready_audit_bundle is not None
            else "BLOCKED"
            if require_audit_bundle
            else "WARN",
            "At least one ready audit evidence bundle is available.",
            {
                "latest_ready_audit_bundle_run_id": (
                    latest_ready_audit_bundle.run_id if latest_ready_audit_bundle is not None else None
                ),
                "ready_total": audit_index.ready_total,
                "require_audit_bundle": require_audit_bundle,
            },
        )
    )

    if latest_ready_run is not None and latest_ready_audit_bundle is not None:
        checks.append(
            _check(
                "latest_ready_run_has_matching_bundle",
                "PASS" if latest_ready_run.run_id == latest_ready_audit_bundle.run_id else "WARN",
                "Latest ready run and latest ready audit bundle refer to the same run_id.",
                {
                    "latest_ready_run_id": latest_ready_run.run_id,
                    "latest_ready_audit_bundle_run_id": latest_ready_audit_bundle.run_id,
                },
            )
        )

    status = _aggregate_status(checks)
    counts = {
        "runs_total": registry_snapshot.entries_total,
        "runs_ready": registry_snapshot.ready_total,
        "audit_bundles_total": audit_index.bundles_total,
        "audit_bundles_ready": audit_index.ready_total,
        "operator_checks_total": len(checks),
        "operator_warnings_total": sum(1 for check in checks if check.status == "WARN"),
        "operator_blockers_total": sum(1 for check in checks if check.status == "BLOCKED"),
    }

    errors = tuple(
        f"{check.name}:{check.message}"
        for check in checks
        if check.status == "BLOCKED"
    )

    return ApiSportsOperatorReport(
        status=status,
        ready=status != "BLOCKED",
        generated_at_utc=_utc_now(),
        ledger_path=str(registry.ledger_path),
        bundle_dir=str(audit_index.bundle_dir),
        audit_index_path=str(audit_index_path),
        counts=counts,
        latest_run=latest_run.to_dict() if latest_run is not None else None,
        latest_ready_run=latest_ready_run.to_dict() if latest_ready_run is not None else None,
        latest_audit_bundle=latest_audit_bundle.to_dict() if latest_audit_bundle is not None else None,
        latest_ready_audit_bundle=(
            latest_ready_audit_bundle.to_dict() if latest_ready_audit_bundle is not None else None
        ),
        checks=tuple(checks),
        errors=errors,
    )


def write_api_sports_operator_report(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    output_path: str | Path | None = None,
    require_audit_bundle: bool = True,
) -> ApiSportsOperatorReport:
    report = build_api_sports_operator_report(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        require_audit_bundle=require_audit_bundle,
    )

    target = Path(output_path) if output_path is not None else default_operator_report_path()
    _write_json_atomic(target, report.to_dict())
    return report


def default_operator_report_path() -> Path:
    return Path(os.getenv("APISPORTS_OPERATOR_REPORT_PATH", "data/pipeline/api_sports/operator_report.json"))


def _audit_index_path_for_bundle_dir(bundle_dir: str | Path | None) -> Path:
    if bundle_dir is None:
        return default_audit_index_path()
    return Path(bundle_dir) / "audit_bundle_index.json"


def _check(
    name: str,
    status: str,
    message: str,
    observed: Mapping[str, Any] | None = None,
) -> ApiSportsOperatorCheck:
    return ApiSportsOperatorCheck(
        name=name,
        status=status,
        message=message,
        observed=observed,
    )


def _aggregate_status(checks: list[ApiSportsOperatorCheck]) -> str:
    if any(check.status == "BLOCKED" for check in checks):
        return "BLOCKED"
    if any(check.status == "WARN" for check in checks):
        return "WARN"
    return "PASS"


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
