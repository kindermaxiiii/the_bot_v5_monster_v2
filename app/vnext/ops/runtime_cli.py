from __future__ import annotations

import json
import tempfile
from pathlib import Path


EXIT_SUCCESS = 0
EXIT_SUCCESS_DEGRADED = 2
EXIT_PREFLIGHT_FAILED = 3
EXIT_REPLAY_SOURCE_FAILED = 4
EXIT_LIVE_SOURCE_UNAVAILABLE = 5
EXIT_PATH_UNWRITABLE = 6
EXIT_PATH_UNREADABLE = 7
EXIT_INSPECT_SOURCE_FAILED = 8
EXIT_LATEST_RUN_MISSING = 9


def write_json_document(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=True), encoding="utf-8")


def derive_run_manifest_path(export_path: Path) -> Path:
    if export_path.suffix:
        return export_path.with_suffix(".manifest.json")
    return export_path.parent / f"{export_path.name}.manifest.json"


def probe_file_output_path(path: Path) -> None:
    parent = path.parent
    parent.mkdir(parents=True, exist_ok=True)
    if path.exists():
        with path.open("a", encoding="utf-8"):
            return

    with tempfile.TemporaryFile(mode="w+b", dir=parent):
        return
