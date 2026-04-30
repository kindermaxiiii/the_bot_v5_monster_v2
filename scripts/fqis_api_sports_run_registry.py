
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.run_registry import (  # noqa: E402
    ApiSportsRunRegistry,
    ApiSportsRunRegistryError,
    default_run_registry_limit,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports run registry resolver.")
    parser.add_argument("--ledger", help="Path to run_ledger.jsonl.")
    parser.add_argument("--run-id", help="Resolve a specific run_id.")
    parser.add_argument("--list", action="store_true", help="List matching runs.")
    parser.add_argument("--limit", type=int, default=default_run_registry_limit())
    parser.add_argument("--status", help="Filter by run status, e.g. COMPLETED or FAILED.")
    parser.add_argument("--quality-status", help="Filter by quality status, e.g. PASS, WARN, BLOCKED.")
    parser.add_argument("--ready-only", action="store_true")
    parser.add_argument("--not-ready-only", action="store_true")
    parser.add_argument("--require", action="store_true", help="Return non-zero when no run is found.")
    args = parser.parse_args(argv)

    if args.ready_only and args.not_ready_only:
        print(json.dumps({"status": "FAILED", "reason": "Use only one of --ready-only or --not-ready-only."}, indent=2))
        return 2

    ready_filter = True if args.ready_only else False if args.not_ready_only else None
    registry = ApiSportsRunRegistry(args.ledger)

    try:
        if args.run_id:
            entry = registry.find_run_id(args.run_id)
            payload = {
                "status": "FOUND" if entry else "NOT_FOUND",
                "ledger_path": str(registry.ledger_path),
                "criteria": {"run_id": args.run_id},
                "entry": entry.to_dict() if entry else None,
                "snapshot": registry.snapshot().to_dict(),
            }
            print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
            return 1 if args.require and entry is None else 0

        if args.list:
            entries = registry.list_entries(
                status=args.status,
                ready=ready_filter,
                quality_status=args.quality_status,
                limit=args.limit,
            )
            payload = {
                "status": "LIST",
                "ledger_path": str(registry.ledger_path),
                "criteria": {
                    "status": args.status,
                    "ready": ready_filter,
                    "quality_status": args.quality_status,
                    "limit": args.limit,
                },
                "entries_total": len(entries),
                "entries": [entry.to_dict() for entry in entries],
                "snapshot": registry.snapshot().to_dict(),
            }
            print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
            return 0

        selection = registry.select_latest(
            status=args.status,
            ready=ready_filter,
            quality_status=args.quality_status,
        )
        payload = selection.to_dict()
        payload["snapshot"] = registry.snapshot().to_dict()

        print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
        return 1 if args.require and selection.entry is None else 0

    except (ApiSportsRunRegistryError, ApiSportsRunLedgerError) as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
