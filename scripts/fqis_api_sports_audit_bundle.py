
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.audit_bundle import (  # noqa: E402
    ApiSportsAuditBundleError,
    resolve_manifest_from_registry,
    write_api_sports_audit_bundle,
)
from app.fqis.integrations.api_sports.run_ledger import ApiSportsRunLedgerError  # noqa: E402
from app.fqis.integrations.api_sports.run_registry import ApiSportsRunRegistryError  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports audit evidence bundle builder.")
    parser.add_argument("--manifest", help="Path to pipeline_manifest.json.")
    parser.add_argument("--output", help="Exact output bundle path.")
    parser.add_argument("--output-dir", help="Directory where the audit bundle will be written.")
    parser.add_argument("--ledger", help="Optional run_ledger.jsonl path for registry resolution.")
    parser.add_argument("--run-id", help="Resolve manifest from a specific run_id.")
    parser.add_argument("--latest-ready", action="store_true", help="Resolve latest ready run from ledger.")
    parser.add_argument("--latest-completed", action="store_true", help="Resolve latest completed run from ledger.")
    parser.add_argument("--require-ready", action="store_true", help="Return non-zero when built bundle is not ready.")
    args = parser.parse_args(argv)

    try:
        if args.manifest:
            manifest_path = Path(args.manifest)
        else:
            manifest_path = resolve_manifest_from_registry(
                ledger_path=args.ledger,
                run_id=args.run_id,
                latest_ready=args.latest_ready,
                latest_completed=args.latest_completed,
            )

        bundle = write_api_sports_audit_bundle(
            manifest_path,
            output_path=args.output,
            output_dir=args.output_dir,
        )

        print(json.dumps(bundle.to_dict(), indent=2, ensure_ascii=True, sort_keys=True))
        return 1 if args.require_ready and not bundle.ready else 0

    except (ApiSportsAuditBundleError, ApiSportsRunLedgerError, ApiSportsRunRegistryError) as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
