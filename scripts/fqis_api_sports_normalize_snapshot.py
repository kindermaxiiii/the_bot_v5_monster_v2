from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.normalization import (  # noqa: E402
    ApiSportsNormalizationError,
    ApiSportsNormalizer,
    FqisNormalizedWriter,
)


def _load_dotenv_if_available() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def main() -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports snapshot normalization.")
    parser.add_argument("--input", required=True, help="Snapshot JSON file or directory containing snapshot JSON files.")
    parser.add_argument(
        "--output-dir",
        default=os.getenv("APISPORTS_NORMALIZED_DIR", "data/normalized/api_sports"),
        help="Directory where normalized JSON batches are written.",
    )
    args = parser.parse_args()

    _load_dotenv_if_available()

    input_path = Path(args.input)
    if not input_path.exists():
        print(json.dumps({"status": "FAILED", "reason": f"Input path does not exist: {input_path}"}, indent=2))
        return 2

    files = _collect_snapshot_files(input_path)
    normalizer = ApiSportsNormalizer()
    writer = FqisNormalizedWriter(args.output_dir)

    report: dict[str, object] = {
        "status": "STARTED",
        "mode": "shadow_only_normalization",
        "provider": "api_sports_api_football",
        "input": str(input_path),
        "output_dir": str(args.output_dir),
        "files_total": len(files),
        "outputs": [],
        "summary": {
            "fixtures": 0,
            "odds_offers": 0,
            "odds_normalized": 0,
            "odds_review": 0,
            "odds_rejected": 0,
        },
    }

    try:
        for file_path in files:
            batch = normalizer.normalize_snapshot_file(file_path)
            output_path = writer.write(batch)
            report["outputs"].append(  # type: ignore[index, union-attr]
                {
                    "input": str(file_path),
                    "output": str(output_path),
                    "source": batch.source,
                    "run_id": batch.run_id,
                    "snapshot_id": batch.snapshot_id,
                    "summary": dict(batch.summary),
                }
            )
            for key in report["summary"]:  # type: ignore[index]
                report["summary"][key] += int(batch.summary.get(key, 0))  # type: ignore[index]

        report["status"] = "COMPLETED"
        print(json.dumps(report, indent=2, ensure_ascii=True))
        return 0
    except (ApiSportsNormalizationError, OSError, json.JSONDecodeError) as exc:
        report["status"] = "FAILED"
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        print(json.dumps(report, indent=2, ensure_ascii=True))
        return 1


def _collect_snapshot_files(input_path: Path) -> list[Path]:
    if input_path.is_file():
        return [input_path]
    return sorted(path for path in input_path.rglob("*.json") if path.is_file())


if __name__ == "__main__":
    raise SystemExit(main())
