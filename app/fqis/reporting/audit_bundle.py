from __future__ import annotations

import hashlib
import json
import shutil
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.fqis.performance.clv import (
    build_clv_report_from_json,
    write_clv_report_json,
)
from app.fqis.performance.metrics import (
    build_performance_report_from_json,
    write_performance_report_json,
)
from app.fqis.reporting.hybrid_shadow_report import (
    build_hybrid_shadow_batch_report_from_jsonl,
    write_hybrid_shadow_batch_report_json,
)
from app.fqis.reporting.run_audit import (
    build_run_audit_report,
    write_run_audit_report_json,
)


@dataclass(slots=True, frozen=True)
class AuditBundleFile:
    role: str
    path: str
    relative_path: str
    size_bytes: int
    sha256: str


@dataclass(slots=True, frozen=True)
class AuditBundleManifest:
    status: str
    run_id: str
    generated_at_utc: str
    bundle_dir: str
    file_count: int
    total_size_bytes: int
    files: tuple[AuditBundleFile, ...]
    headline_metrics: dict[str, Any]
    health_status: str
    flag_count: int
    fail_count: int
    warn_count: int
    info_count: int


def build_audit_bundle(
    *,
    hybrid_batch_path: Path,
    settlement_path: Path,
    closing_path: Path,
    output_dir: Path,
    run_id: str | None = None,
    include_input_copies: bool = True,
) -> AuditBundleManifest:
    resolved_run_id = run_id or _default_run_id()
    bundle_dir = output_dir / resolved_run_id
    inputs_dir = bundle_dir / "inputs"
    reports_dir = bundle_dir / "reports"

    reports_dir.mkdir(parents=True, exist_ok=True)

    written_files: list[AuditBundleFile] = []

    if include_input_copies:
        inputs_dir.mkdir(parents=True, exist_ok=True)
        written_files.append(
            _copy_bundle_file(
                source_path=hybrid_batch_path,
                destination_path=inputs_dir / "hybrid_shadow_batch.jsonl",
                role="input_hybrid_shadow_batch",
                bundle_dir=bundle_dir,
            )
        )
        written_files.append(
            _copy_bundle_file(
                source_path=settlement_path,
                destination_path=inputs_dir / "settlement_report.json",
                role="input_settlement_report",
                bundle_dir=bundle_dir,
            )
        )
        written_files.append(
            _copy_bundle_file(
                source_path=closing_path,
                destination_path=inputs_dir / "closing_odds.jsonl",
                role="input_closing_odds",
                bundle_dir=bundle_dir,
            )
        )

    hybrid_report = build_hybrid_shadow_batch_report_from_jsonl(hybrid_batch_path)
    hybrid_report_path = reports_dir / "hybrid_shadow_batch_report.json"
    write_hybrid_shadow_batch_report_json(hybrid_report, hybrid_report_path)
    written_files.append(
        _bundle_file(
            path=hybrid_report_path,
            role="report_hybrid_shadow_batch",
            bundle_dir=bundle_dir,
        )
    )

    performance_report = build_performance_report_from_json(settlement_path)
    performance_report_path = reports_dir / "performance_report.json"
    write_performance_report_json(performance_report, performance_report_path)
    written_files.append(
        _bundle_file(
            path=performance_report_path,
            role="report_performance",
            bundle_dir=bundle_dir,
        )
    )

    clv_report = build_clv_report_from_json(
        settlement_path=settlement_path,
        closing_path=closing_path,
    )
    clv_report_path = reports_dir / "clv_report.json"
    write_clv_report_json(clv_report, clv_report_path)
    written_files.append(
        _bundle_file(
            path=clv_report_path,
            role="report_clv",
            bundle_dir=bundle_dir,
        )
    )

    run_audit_report = build_run_audit_report(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=closing_path,
        run_id=resolved_run_id,
    )
    run_audit_report_path = reports_dir / "run_audit_report.json"
    write_run_audit_report_json(run_audit_report, run_audit_report_path)
    written_files.append(
        _bundle_file(
            path=run_audit_report_path,
            role="report_run_audit",
            bundle_dir=bundle_dir,
        )
    )

    manifest_without_self = AuditBundleManifest(
        status="ok",
        run_id=resolved_run_id,
        generated_at_utc=datetime.now(UTC).isoformat(),
        bundle_dir=str(bundle_dir),
        file_count=len(written_files),
        total_size_bytes=sum(file.size_bytes for file in written_files),
        files=tuple(written_files),
        headline_metrics=dict(run_audit_report.headline_metrics),
        health_status=run_audit_report.health_status,
        flag_count=run_audit_report.flag_count,
        fail_count=run_audit_report.fail_count,
        warn_count=run_audit_report.warn_count,
        info_count=run_audit_report.info_count,
    )

    manifest_path = bundle_dir / "manifest.json"
    _write_manifest_json(manifest_without_self, manifest_path)

    manifest_file = _bundle_file(
        path=manifest_path,
        role="manifest",
        bundle_dir=bundle_dir,
    )

    final_files = tuple([*written_files, manifest_file])

    return AuditBundleManifest(
        status="ok",
        run_id=resolved_run_id,
        generated_at_utc=manifest_without_self.generated_at_utc,
        bundle_dir=str(bundle_dir),
        file_count=len(final_files),
        total_size_bytes=sum(file.size_bytes for file in final_files),
        files=final_files,
        headline_metrics=dict(run_audit_report.headline_metrics),
        health_status=run_audit_report.health_status,
        flag_count=run_audit_report.flag_count,
        fail_count=run_audit_report.fail_count,
        warn_count=run_audit_report.warn_count,
        info_count=run_audit_report.info_count,
    )


def audit_bundle_manifest_to_record(manifest: AuditBundleManifest) -> dict[str, Any]:
    return {
        "status": manifest.status,
        "source": "fqis_audit_bundle_manifest",
        "run_id": manifest.run_id,
        "generated_at_utc": manifest.generated_at_utc,
        "bundle_dir": manifest.bundle_dir,
        "file_count": manifest.file_count,
        "total_size_bytes": manifest.total_size_bytes,
        "health_status": manifest.health_status,
        "flag_count": manifest.flag_count,
        "fail_count": manifest.fail_count,
        "warn_count": manifest.warn_count,
        "info_count": manifest.info_count,
        "headline_metrics": dict(manifest.headline_metrics),
        "files": [
            _bundle_file_to_record(file)
            for file in manifest.files
        ],
    }


def write_audit_bundle_manifest_json(
    manifest: AuditBundleManifest,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            audit_bundle_manifest_to_record(manifest),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _write_manifest_json(manifest: AuditBundleManifest, path: Path) -> Path:
    return write_audit_bundle_manifest_json(manifest, path)


def _copy_bundle_file(
    *,
    source_path: Path,
    destination_path: Path,
    role: str,
    bundle_dir: Path,
) -> AuditBundleFile:
    if not source_path.exists():
        raise FileNotFoundError(f"bundle source file not found: {source_path}")

    destination_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copy2(source_path, destination_path)

    return _bundle_file(path=destination_path, role=role, bundle_dir=bundle_dir)


def _bundle_file(
    *,
    path: Path,
    role: str,
    bundle_dir: Path,
) -> AuditBundleFile:
    if not path.exists():
        raise FileNotFoundError(f"bundle file not found: {path}")

    return AuditBundleFile(
        role=role,
        path=str(path),
        relative_path=str(path.relative_to(bundle_dir)),
        size_bytes=path.stat().st_size,
        sha256=_sha256_file(path),
    )


def _bundle_file_to_record(file: AuditBundleFile) -> dict[str, Any]:
    return {
        "role": file.role,
        "path": file.path,
        "relative_path": file.relative_path,
        "size_bytes": file.size_bytes,
        "sha256": file.sha256,
    }


def _sha256_file(path: Path) -> str:
    digest = hashlib.sha256()

    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)

    return digest.hexdigest()


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("fqis_audit_bundle_%Y%m%d_%H%M%S")


    