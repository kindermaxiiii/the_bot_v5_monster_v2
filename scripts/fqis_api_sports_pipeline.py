
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.pipeline import (  # noqa: E402
    ApiSportsPipelineConfig,
    ApiSportsPipelineRunner,
    ApiSportsPipelineStatus,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports local normalized snapshot pipeline.")
    parser.add_argument("--normalized-input", help="Path to normalized API-Sports JSON snapshot.")
    parser.add_argument("--output-dir", default="data/pipeline/api_sports")
    parser.add_argument("--run-id")
    parser.add_argument("--strict-quality", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    runner = ApiSportsPipelineRunner(
        ApiSportsPipelineConfig(
            normalized_input=Path(args.normalized_input) if args.normalized_input else None,
            output_dir=Path(args.output_dir),
            run_id=args.run_id,
            strict_quality=args.strict_quality,
            dry_run=args.dry_run,
        )
    )
    manifest = runner.run()
    print(json.dumps(manifest.to_dict(), ensure_ascii=True, indent=2, sort_keys=True))

    return 1 if manifest.status is ApiSportsPipelineStatus.FAILED else 0


if __name__ == "__main__":
    raise SystemExit(main())
