
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.operator_report import (  # noqa: E402
    build_api_sports_operator_report,
    write_api_sports_operator_report,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports operator readiness report.")
    parser.add_argument("--ledger", help="Path to run_ledger.jsonl.")
    parser.add_argument("--bundle-dir", help="Directory containing audit bundle JSON files.")
    parser.add_argument("--output", help="Optional path where operator_report.json will be written.")
    parser.add_argument("--no-require-audit-bundle", action="store_true")
    parser.add_argument("--require-ready", action="store_true", help="Return non-zero when report is not ready.")
    args = parser.parse_args(argv)

    require_audit_bundle = not args.no_require_audit_bundle

    if args.output:
        report = write_api_sports_operator_report(
            ledger_path=args.ledger,
            bundle_dir=args.bundle_dir,
            output_path=args.output,
            require_audit_bundle=require_audit_bundle,
        )
    else:
        report = build_api_sports_operator_report(
            ledger_path=args.ledger,
            bundle_dir=args.bundle_dir,
            require_audit_bundle=require_audit_bundle,
        )

    print(json.dumps(report.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
    return 1 if args.require_ready and not report.ready else 0


if __name__ == "__main__":
    raise SystemExit(main())
