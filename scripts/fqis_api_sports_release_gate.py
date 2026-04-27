
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.release_gate import (  # noqa: E402
    ApiSportsReleaseGateConfig,
    ApiSportsReleaseGateError,
    evaluate_api_sports_release_gate,
    write_api_sports_release_gate,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports Level 2 release gate.")
    parser.add_argument("--ledger", help="Path to run_ledger.jsonl.")
    parser.add_argument("--bundle-dir", help="Directory containing audit bundle JSON files.")
    parser.add_argument("--output", help="Optional path where release_gate.json will be written.")
    parser.add_argument("--min-ready-runs", type=int)
    parser.add_argument("--min-ready-audit-bundles", type=int)
    parser.add_argument("--allow-warnings", action="store_true")
    parser.add_argument("--no-require-audit-bundle", action="store_true")
    parser.add_argument("--require-ready", action="store_true", help="Return non-zero when release is not ready.")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsReleaseGateConfig.from_env()
        config = ApiSportsReleaseGateConfig(
            min_ready_runs=args.min_ready_runs
            if args.min_ready_runs is not None
            else base_config.min_ready_runs,
            min_ready_audit_bundles=args.min_ready_audit_bundles
            if args.min_ready_audit_bundles is not None
            else base_config.min_ready_audit_bundles,
            allow_warnings=args.allow_warnings or base_config.allow_warnings,
            require_audit_bundle=False if args.no_require_audit_bundle else base_config.require_audit_bundle,
        )

        if args.output:
            decision = write_api_sports_release_gate(
                ledger_path=args.ledger,
                bundle_dir=args.bundle_dir,
                output_path=args.output,
                config=config,
            )
        else:
            decision = evaluate_api_sports_release_gate(
                ledger_path=args.ledger,
                bundle_dir=args.bundle_dir,
                config=config,
            )

        print(json.dumps(decision.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
        return 1 if args.require_ready and not decision.release_ready else 0

    except ApiSportsReleaseGateError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
