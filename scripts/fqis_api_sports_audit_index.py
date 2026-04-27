
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.audit_index import (  # noqa: E402
    ApiSportsAuditIndexError,
    build_api_sports_audit_index,
    select_latest_audit_bundle,
    write_api_sports_audit_index,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports audit bundle index.")
    parser.add_argument("--bundle-dir", help="Directory containing API-Sports audit bundle JSON files.")
    parser.add_argument("--output", help="Path where audit_bundle_index.json will be written.")
    parser.add_argument("--latest", action="store_true", help="Resolve latest bundle instead of writing index.")
    parser.add_argument("--latest-ready", action="store_true", help="Resolve latest ready bundle instead of writing index.")
    parser.add_argument("--ready-only", action="store_true")
    parser.add_argument("--not-ready-only", action="store_true")
    parser.add_argument("--status", help="Filter latest selection by bundle status.")
    parser.add_argument("--quality-status", help="Filter latest selection by quality status.")
    parser.add_argument("--require", action="store_true", help="Return non-zero when selection is not found.")
    args = parser.parse_args(argv)

    if args.ready_only and args.not_ready_only:
        print(json.dumps({"status": "FAILED", "reason": "Use only one of --ready-only or --not-ready-only."}, indent=2))
        return 2

    try:
        should_select = (
            args.latest
            or args.latest_ready
            or args.ready_only
            or args.not_ready_only
            or args.status is not None
            or args.quality_status is not None
        )

        if should_select:
            ready_filter = True if args.latest_ready or args.ready_only else False if args.not_ready_only else None
            index = build_api_sports_audit_index(bundle_dir=args.bundle_dir)
            entry = select_latest_audit_bundle(
                bundle_dir=args.bundle_dir,
                ready=ready_filter,
                status=args.status,
                quality_status=args.quality_status,
            )

            payload = {
                "status": "FOUND" if entry is not None else "NOT_FOUND",
                "criteria": {
                    "ready": ready_filter,
                    "status": args.status,
                    "quality_status": args.quality_status,
                },
                "entry": entry.to_dict() if entry is not None else None,
                "index": index.to_dict(),
            }

            print(json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True))
            return 1 if args.require and entry is None else 0

        index = write_api_sports_audit_index(
            bundle_dir=args.bundle_dir,
            output_path=args.output,
        )
        print(json.dumps(index.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
        return 0

    except ApiSportsAuditIndexError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
