
from __future__ import annotations

import hashlib
import json
import os
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, replace
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.fqis.integrations.api_sports.release_gate import ApiSportsReleaseGateConfig
from app.fqis.integrations.api_sports.release_manifest import (
    ApiSportsReleaseManifestError,
    build_api_sports_release_manifest,
    default_release_manifest_path,
    load_api_sports_release_manifest,
    write_api_sports_release_manifest,
)


@dataclass(frozen=True)
class ApiSportsReleasePackFile:
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
class ApiSportsReleasePack:
    status: str
    release_ready: bool
    release_id: str
    generated_at_utc: str
    pack_path: str | None
    release_manifest_path: str | None
    files: tuple[ApiSportsReleasePackFile, ...]
    release_manifest: Mapping[str, Any]
    errors: tuple[str, ...]

    def to_dict(self) -> dict[str, object]:
        return {
            "status": self.status,
            "release_ready": self.release_ready,
            "release_id": self.release_id,
            "generated_at_utc": self.generated_at_utc,
            "pack_path": self.pack_path,
            "release_manifest_path": self.release_manifest_path,
            "files": [item.to_dict() for item in self.files],
            "release_manifest": dict(self.release_manifest),
            "errors": list(self.errors),
        }


class ApiSportsReleasePackError(RuntimeError):
    pass


def build_api_sports_release_pack(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    release_manifest_path: str | Path | None = None,
    release_gate_path: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
    include_git: bool = True,
) -> ApiSportsReleasePack:
    try:
        if release_manifest_path is not None:
            manifest = load_api_sports_release_manifest(release_manifest_path)
            manifest_payload = manifest.to_dict()
            manifest_path = str(release_manifest_path)
        else:
            manifest = build_api_sports_release_manifest(
                ledger_path=ledger_path,
                bundle_dir=bundle_dir,
                release_gate_path=release_gate_path,
                config=config,
                include_git=include_git,
            )
            manifest_payload = manifest.to_dict()
            manifest_path = _optional_str(manifest_payload.get("manifest_path"))

        return _build_pack_from_manifest_payload(
            manifest_payload,
            release_manifest_path=manifest_path,
        )

    except ApiSportsReleaseManifestError as exc:
        raise ApiSportsReleasePackError(str(exc)) from exc


def write_api_sports_release_pack(
    *,
    ledger_path: str | Path | None = None,
    bundle_dir: str | Path | None = None,
    release_manifest_path: str | Path | None = None,
    release_manifest_output_path: str | Path | None = None,
    release_gate_path: str | Path | None = None,
    output_path: str | Path | None = None,
    config: ApiSportsReleaseGateConfig | None = None,
    include_git: bool = True,
) -> ApiSportsReleasePack:
    try:
        if release_manifest_path is not None:
            manifest = load_api_sports_release_manifest(release_manifest_path)
            manifest_payload = manifest.to_dict()
            manifest_path = str(release_manifest_path)
        else:
            manifest_target = Path(release_manifest_output_path) if release_manifest_output_path else default_release_manifest_path()
            manifest = write_api_sports_release_manifest(
                ledger_path=ledger_path,
                bundle_dir=bundle_dir,
                release_gate_path=release_gate_path,
                output_path=manifest_target,
                config=config,
                include_git=include_git,
            )
            manifest_payload = manifest.to_dict()
            manifest_path = str(manifest_target)

        pack = _build_pack_from_manifest_payload(
            manifest_payload,
            release_manifest_path=manifest_path,
        )

        target = Path(output_path) if output_path is not None else default_release_pack_path()
        final_pack = replace(pack, pack_path=str(target))
        _write_json_atomic(target, final_pack.to_dict())
        return final_pack

    except ApiSportsReleaseManifestError as exc:
        raise ApiSportsReleasePackError(str(exc)) from exc


def load_api_sports_release_pack(path: str | Path) -> ApiSportsReleasePack:
    payload = _load_json_object(Path(path))

    files = tuple(
        ApiSportsReleasePackFile(
            role=_required_str(item.get("role"), "files.role"),
            path=_required_str(item.get("path"), "files.path"),
            exists=_bool(item.get("exists")),
            sha256=_optional_str(item.get("sha256")),
            size_bytes=_int_or_none(item.get("size_bytes")),
        )
        for item in _records(payload.get("files"))
    )

    release_manifest = payload.get("release_manifest")
    if not isinstance(release_manifest, Mapping):
        raise ApiSportsReleasePackError("Release pack field must be an object: release_manifest")

    return ApiSportsReleasePack(
        status=_required_str(payload.get("status"), "status"),
        release_ready=_bool(payload.get("release_ready")),
        release_id=_required_str(payload.get("release_id"), "release_id"),
        generated_at_utc=_required_str(payload.get("generated_at_utc"), "generated_at_utc"),
        pack_path=_optional_str(payload.get("pack_path")),
        release_manifest_path=_optional_str(payload.get("release_manifest_path")),
        files=files,
        release_manifest=dict(release_manifest),
        errors=tuple(str(item) for item in _sequence(payload.get("errors"))),
    )


def default_release_pack_path() -> Path:
    return Path(os.getenv("APISPORTS_RELEASE_PACK_PATH", "data/pipeline/api_sports/release_pack.json"))


def _build_pack_from_manifest_payload(
    manifest_payload: Mapping[str, Any],
    *,
    release_manifest_path: str | None,
) -> ApiSportsReleasePack:
    release_id = _required_str(manifest_payload.get("release_id"), "release_id")
    release_ready = _bool(manifest_payload.get("release_ready"))

    files = tuple(_collect_pack_files(manifest_payload, release_manifest_path))
    errors = tuple(_pack_errors(manifest_payload, release_ready, files))
    status = "READY" if release_ready and not errors else "BLOCKED"

    return ApiSportsReleasePack(
        status=status,
        release_ready=status == "READY",
        release_id=release_id,
        generated_at_utc=_utc_now(),
        pack_path=None,
        release_manifest_path=release_manifest_path,
        files=files,
        release_manifest=dict(manifest_payload),
        errors=errors,
    )


def _collect_pack_files(
    manifest_payload: Mapping[str, Any],
    release_manifest_path: str | None,
) -> list[ApiSportsReleasePackFile]:
    files: list[ApiSportsReleasePackFile] = []
    seen: set[tuple[str, str]] = set()

    def add(role: str, value: str | None) -> None:
        if value is None:
            return
        path = Path(value)
        key = (role, str(path))
        if key in seen:
            return
        seen.add(key)
        files.append(_file_record(role, path))

    add("release_manifest", release_manifest_path)
    add("release_gate", _optional_str(manifest_payload.get("release_gate_path")))

    for artifact in _records(manifest_payload.get("artifacts")):
        role = _optional_str(artifact.get("role")) or "artifact"
        add(f"artifact:{role}", _optional_str(artifact.get("path")))

    return files


def _pack_errors(
    manifest_payload: Mapping[str, Any],
    release_ready: bool,
    files: tuple[ApiSportsReleasePackFile, ...],
) -> list[str]:
    errors: list[str] = []

    if not release_ready:
        errors.append("RELEASE_MANIFEST_BLOCKED")

    manifest_errors = _sequence(manifest_payload.get("errors"))
    for item in manifest_errors:
        errors.append(f"RELEASE_MANIFEST_ERROR:{item}")

    for item in files:
        if not item.exists:
            errors.append(f"MISSING_FILE:{item.role}:{item.path}")

    return errors


def _file_record(role: str, path: Path) -> ApiSportsReleasePackFile:
    exists = path.exists() and path.is_file()
    raw_bytes = path.read_bytes() if exists else None

    return ApiSportsReleasePackFile(
        role=role,
        path=str(path),
        exists=exists,
        sha256=hashlib.sha256(raw_bytes).hexdigest() if raw_bytes is not None else None,
        size_bytes=len(raw_bytes) if raw_bytes is not None else None,
    )


def _load_json_object(path: Path) -> dict[str, Any]:
    if not path.exists():
        raise ApiSportsReleasePackError(f"JSON path does not exist: {path}")
    if not path.is_file():
        raise ApiSportsReleasePackError(f"JSON path is not a file: {path}")

    try:
        payload = json.loads(path.read_text(encoding="utf-8"))
    except json.JSONDecodeError as exc:
        raise ApiSportsReleasePackError(f"JSON path is invalid: {path}") from exc

    if not isinstance(payload, dict):
        raise ApiSportsReleasePackError(f"JSON path must contain an object: {path}")

    return payload


def _write_json_atomic(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(path.suffix + ".tmp")
    tmp.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    tmp.replace(path)


def _required_str(value: Any, field: str) -> str:
    result = _optional_str(value)
    if result is None:
        raise ApiSportsReleasePackError(f"Field is required: {field}")
    return result


def _optional_str(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


def _bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "y", "ready", "pass"}
    return bool(value)


def _int_or_none(value: Any) -> int | None:
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
