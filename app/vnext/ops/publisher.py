from __future__ import annotations

import os
import tempfile
from pathlib import Path

from app.vnext.ops.jsonl_diagnose import diagnose_jsonl


class PublishError(RuntimeError):
    """Raised when publishing or validation fails."""


def publish_and_validate(export_path: Path, live_path: Path) -> dict[str, object]:
    """
    Atomically publish export_path to live_path and run diagnose_jsonl on the published file.

    Returns the diagnose_jsonl result dict on success.

    Raises PublishError on any failure (copy error, diagnose failure).
    """
    if not export_path.exists():
        raise PublishError(f"export_missing path={export_path}")

    live_dir = live_path.parent
    live_dir.mkdir(parents=True, exist_ok=True)

    tmp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=str(live_dir),
            suffix=".tmp",
        ) as handle:
            handle.write(export_path.read_text(encoding="utf-8"))
            tmp_path = Path(handle.name)

        os.replace(str(tmp_path), str(live_path))
        tmp_path = None

        result = diagnose_jsonl(live_path)
        if result.get("rows_with_missing_audits", 0) > 0:
            raise PublishError(
                f"diagnose_failed rows_with_missing_audits={result.get('rows_with_missing_audits')}"
            )
        return result
    except PublishError:
        raise
    except Exception as exc:
        raise PublishError(f"publish_error detail={exc}") from exc
    finally:
        if tmp_path is not None and tmp_path.exists():
            try:
                tmp_path.unlink(missing_ok=True)
            except Exception:
                pass