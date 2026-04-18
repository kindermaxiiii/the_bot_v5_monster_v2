from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.vnext.ops.diagnostics import (
    build_cohort_filter,
    format_recent_window,
    summarize_fixture_recent,
    summarize_recent_window,
)
from app.vnext.ops.inspection import InspectCliError


def _positive_int(value: str) -> int:
    parsed = int(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("last-cycles must be > 0")
    return parsed


def main() -> int:
    parser = argparse.ArgumentParser()
    group = parser.add_mutually_exclusive_group()
    group.add_argument("--manifest", type=str, default="")
    group.add_argument("--export", type=str, default="")
    parser.add_argument("--last-cycles", type=_positive_int, default=5)
    parser.add_argument("--fixture-id", type=int, default=0)
    parser.add_argument("--only-governed", action="store_true")
    parser.add_argument("--governed-status", type=str, default="")
    parser.add_argument("--contains-refusal", type=str, default="")
    args = parser.parse_args()

    try:
        cohort = build_cohort_filter(
            only_governed=args.only_governed,
            governed_status=args.governed_status,
            contains_refusal=args.contains_refusal,
        )
        summary, cycles = summarize_recent_window(
            manifest_path=Path(args.manifest) if args.manifest else None,
            export_path=Path(args.export) if args.export else None,
            last_cycles=args.last_cycles,
            cohort=cohort,
        )
        fixture_summary = None
        if args.fixture_id > 0:
            fixture_summary = summarize_fixture_recent(
                fixture_id=args.fixture_id,
                cycles=cycles,
            )
    except InspectCliError as exc:
        print(
            f"vnext_recent_error reason={exc.reason} path={exc.path}",
            file=sys.stderr,
        )
        return exc.exit_code

    print(format_recent_window(summary, fixture_summary))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
