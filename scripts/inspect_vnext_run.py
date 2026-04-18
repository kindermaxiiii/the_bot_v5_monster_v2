from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.vnext.ops.inspection import (
    InspectCliError,
    format_run_inspection,
    inspect_export_path,
    inspect_latest_run,
    inspect_manifest_path,
)


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--manifest", type=str, default="")
    group.add_argument("--export", type=str, default="")
    args = parser.parse_args()

    try:
        if args.manifest:
            summary = inspect_manifest_path(Path(args.manifest))
        elif args.export:
            summary = inspect_export_path(Path(args.export))
        else:
            summary = inspect_latest_run()
    except InspectCliError as exc:
        print(
            f"vnext_inspect_error reason={exc.reason} path={exc.path}",
            file=sys.stderr,
        )
        return exc.exit_code

    print(format_run_inspection(summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
