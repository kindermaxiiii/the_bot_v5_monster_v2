
from __future__ import annotations

import hashlib
import json
import os
import platform
import subprocess
import sys
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.release_gate import (
    ApiSportsReleaseGateConfig,
    ApiSportsReleaseGateDecision,
    evaluate_api_sports_release_gate,
)


@dataclass(frozen=True)
class ApiSportsReleaseManifestArtifact:
    role: str
    path: str
    exists: bool
    sha256: str | None
    size_bytes: int | None

    def to_dict(self) -> dict[str, object]:
        return {
            "role": self.role,
            "path": self.path,
            "exists": self.exists,
            "sha256": self.sha256,
            "size_bytes": self.size_bytes,
        }


@dataclass(frozen=True)
class ApiSportsReleaseManifest:
    status: str
    release_ready: bool
    release_id: str
    generated_at_utc: str
    git_commit: str | None
    git_branch: str | None
    python_version: str
    platform: str
    manifest_path: str | None
    release_gate_path: str | None
    artifacts: tuple[ApiSportsReleaseManifestArtifact, ...]
    release_gate: Mapping[str, Any]
    config: Mapping[str, Any]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "release_ready": self.release_ready,
            "release_id": self.release_id,
            "generated_at_utc": self.generated_at_utc,
            "git_commit": self.git_commit,
            "git_branch": self.git_branch,
            "python_version": self.python_version,
            "platform": self.platform,
            "manifest_path": self.manifest_path,
            "release_gate_path": self.release_gate_path,
            "artifacts": [artifact.to_dict() for artifact in self.artifacts],
            "release_gate": dict(self.release_gate),
            "config": dict(self.config),
            "errors": list(self.errors),
        }


class ApiSportsReleaseManifestError(RuntimeError):
    pass


def build_api_sports_release_manifest(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    release_gate_path: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
    include_git: bool = True,
) -> ApiSportsReleaseManifest:
    decision = evaluate_api_sports_release_gate(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        config=config,
    )

    release_id = _release_id(decision)
    artifacts = _release_artifacts(decision, release_gate_path)

    errors = tuple(_manifest_errors(decision, artifacts))
    status = "READY" if decision.release_ready and not errors else "BLOCKED"

    return ApiSportsReleaseManifest(
        status=status,
        release_ready=status == "READY",
        release_id=release_id,
        generated_at_utc=_utc_now(),
        git_commit=_git(["rev-parse", "HEAD"]) if include_git else None,
        git_branch=_git(["rev-parse", "--abbrev-ref", "HEAD"]) if include_git else None,
        python_version=sys.version,
        platform=platform.platform(),
        manifest_path=None,
        release_gate_path=str(release_gate_path) if release_gate_path is not None else None,
        artifacts=tuple(artifacts),
        release_gate=decision.to_dict(),
        config={
            "ledger_path": str(ledger_path) if ledger_path is not None else None,
            "bundle_dir": str(bundle_dir) if bundle_dir is not None else None,
            "release_gate_path": str(release_gate_path) if release_gate_path is not None else None,
            "include_git": include_git,
        },
        errors=errors,
    )


def write_api_sports_release_manifest(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    release_gate_path: str | Path | None = None,
    output_path: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
    include_git: bool = True,
) -> ApiSportsReleaseManifest:
    manifest = build_api_sports_release_manifest(
        ledger_path=ledger_path,
        bundle_dir=bundle_dir,
        release_gate_path=release_gate_path,
        config=config,
        include_git=include_git,
    )

    target = Path(output_path) if output_path is not None else default_release_manifest_path()
    payload = manifest.to_dict()
    payload["manifest_path"] = str(target)

    _write_json_atomic(target, payload)

    return ApiSportsReleaseManifest(
        status=manifest.status,
        release_ready=manifest.release_ready,
        release_id=manifest.release_id,
        generated_at_utc=manifest.generated_at_utc,
        git_commit=manifest.git_commit,
        git_branch=manifest.git_branch,
        python_version=manifest.python_version,
        platform=manifest.platform,
        manifest_path=str(target),
        release_gate_path=manifest.release_gate_path,
        artifacts=manifest.artifacts,
        release_gate=manifest.release_gate,
        config=manifest.config,
        errors=manifest.errors,
    )


def load_api_sports_release_manifest(path: str | Path) -> ApiSportsReleaseManifest:
    payload = _load_json_object(Path(path))
    artifacts = tuple(
        ApiSportsReleaseManifestArtifact(
            role=str(item.get("role")),
            path=str(item.get("path")),
            exists=bool(item.get("exists")),
            sha256=_optional_str(item.get("sha256")),
            size_bytes=_optional_int(item.get("size_bytes")),
        )
        for item in _records(payload.get("artifacts"))
    )

    return ApiSportsReleaseManifest(
        status=_required_str(payload.get("status"), "status"),
        release_ready=bool(payload.get("release_ready")),
        release_id=_required_str(payload.get("release_id"), "release_id"),
        generated_at_utc=_required_str(payload.get("generated_at_utc"), "generated_at_utc"),
        git_commit=_optional_str(payload.get("git_commit")),
        git_branch=_optional_str(payload.get("git_branch")),
        python_version=_required_str(payload.get("python_version"), "python_version"),
        platform=_required_str(payload.get("platform"), "platform"),
        manifest_path=_optional_str(payload.get("manifest_path")),
        release_gate_path=_optional_str(payload.get("release_gate_path")),
        artifacts=artifacts,
        release_gate=dict(payload.get("release_gate") or {}),
        config=dict(payload.get("config") or {}),
        errors=tuple(str(item) for item in _sequence(payload.get("errors"))),
    )


def default_release_manifest_path() -> Path:
    return Path(
        os.getenv(
            "APISPORTS_RELEASE_MANIFEST_PATH",
            "data/pipeline/api_sports/release_manifest.json",
        )
    )


def _release_id(decision: ApiSportsReleaseGateDecision) -> str:
    base = {
        "status": decision.status,
        "release_ready": decision.release_ready,
        "latest_ready_run_id": decision.latest_ready_run_id,
        "latest_ready_audit_bundle_run_id": decision.latest_ready_audit_bundle_run_id,
        "counts": dict(decision.counts),
        "config": decision.config.to_dict(),
    }
    digest = hashlib.sha256(
        json.dumps(base, ensure_ascii=False, sort_keys=True).encode("utf-8")
    ).hexdigest()[:16]
    return f"api-sports-level2-{digest}"


def _release_artifacts(
    decision: ApiSportsReleaseGateDecision,
    release_gate_path: str | Path | None,
) -> list[ApiSportsReleaseManifestArtifact]:
    artifacts: list[ApiSportsReleaseManifestArtifact] = []

    if release_gate_path is not None:
        artifacts.append(_artifact("release_gate", Path(release_gate_path)))

    latest_ready_run = decision.operator_report.get("latest_ready_run")
    if isinstance(latest_ready_run, Mapping):
        manifest_path = _optional_str(latest_ready_run.get("manifest_path"))
        if manifest_path is not None:
            manifest_file = Path(manifest_path)
            if manifest_file.exists() and manifest_file.is_file():
                artifacts.append(_artifact("latest_ready_pipeline_manifest", manifest_file))

    latest_ready_bundle = decision.operator_report.get("latest_ready_audit_bundle")
    if isinstance(latest_ready_bundle, Mapping):
        bundle_path = _optional_str(latest_ready_bundle.get("bundle_path"))
        if bundle_path is not None:
            artifacts.append(_artifact("latest_ready_audit_bundle", Path(bundle_path)))

    return artifacts


def _manifest_errors(
    decision: ApiSportsReleaseGateDecision,
    artifacts: Sequence[ApiSportsReleaseManifestArtifact],
) -> list[str]:
    errors: list[str] = []

    if not decision.release_ready:
        errors.append("RELEASE_GATE_BLOCKED")

    for artifact in artifacts:
        if not artifact.exists:
            errors.append(f"MISSING_ARTIFACT:{artifact.role}:{artifact.path}")

    return errors


def _artifact(role: str, path: Path) -> ApiSportsReleaseManifestArtifact:
    exists = path.exists() and path.is_file()
    raw_bytes = path.read_bytes() if exists else None

    return ApiSportsReleaseManifestArtifact(
        role=role,
        path=str(path),
        exists=exists,
        sha256=hashlib.sha256(raw_bytes).hexdigest() if raw_bytes is not None else None,
        size_bytes=len(raw_bytes) if raw_bytes is not None else None,
    )


def _git(args: Sequence[str]) -> str | None:
    try:
        completed = subprocess.run(
            ["git", *args],
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            check=False,
        )
    except OSError:
        return None

    if completed.returncode != 0:
        return None

    return completed.stdout.strip() or None


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ApiSportsReleaseManifestError(f"Manifest path does not exist: {path}")
    if not path.is_file():
        raise ApiSportsReleaseManifestError(f"Manifest path is not a file: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ApiSportsReleaseManifestError(f"Manifest path is invalid JSON: {path}") from exc

    if not isinstance(payload, dict):
        raise ApiSportsReleaseManifestError(f"Manifest path must contain a JSON object: {path}")

    return payload


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsReleaseManifestError(f"Manifest field is required: {field}")
    return result


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    if value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _records(value: Any) -> list[Mapping[str, Any]]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return [item for item in value if isinstance(item, Mapping)]


def _sequence(value: Any) -> Sequence[Any]:
    if not isinstance(value, Sequence) or isinstance(value, (str, bytes, bytearray)):
        return []
    return value


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
