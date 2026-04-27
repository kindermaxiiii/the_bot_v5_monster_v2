from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.replay import (  # noqa: E402
    DEFAULT_REPLAY_AUDIT_DIR,
    ApiSportsReplayError,
    replay_normalized_snapshot,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports normalized snapshot replay.")
    parser.add_argument("--input", required=True, help="Path to a normalized API-Sports snapshot JSON file.")
    parser.add_argument(
        "--output-dir",
        default=str(DEFAULT_REPLAY_AUDIT_DIR),
        help="Directory where the replay audit manifest will be written.",
    )
    parser.add_argument("--no-write", action="store_true", help="Do not write the audit manifest.")
    args = parser.parse_args()

    try:
        manifest = replay_normalized_snapshot(
            args.input,
            output_dir=args.output_dir,
            write_manifest=not args.no_write,
        )
    except ApiSportsReplayError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2
    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "error_type": type(exc).__name__,
                    "error": str(exc),
                },
                indent=2,
                ensure_ascii=False,
            )
        )
        return 1

    print(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
