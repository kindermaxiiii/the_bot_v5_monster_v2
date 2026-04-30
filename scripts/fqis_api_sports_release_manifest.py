
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.release_gate import ApiSportsReleaseGateConfig  # noqa: E402
from app.fqis.integrations.api_sports.release_manifest import (  # noqa: E402
    ApiSportsReleaseManifestError,
    build_api_sports_release_manifest,
    load_api_sports_release_manifest,
    write_api_sports_release_manifest,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports Level 2 release manifest.")
    parser.add_argument("--ledger", help="Path to run_ledger.jsonl.")
    parser.add_argument("--bundle-dir", help="Directory containing audit bundle JSON files.")
    parser.add_argument("--release-gate", help="Optional release_gate.json artifact path.")
    parser.add_argument("--output", help="Optional release_manifest.json output path.")
    parser.add_argument("--load", help="Load and print an existing release manifest.")
    parser.add_argument("--no-git", action="store_true")
    parser.add_argument("--allow-warnings", action="store_true")
    parser.add_argument("--no-require-audit-bundle", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        if args.load:
            manifest = load_api_sports_release_manifest(args.load)
            print(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=True, sort_keys=True))
            return 1 if args.require_ready and not manifest.release_ready else 0

        base_config = ApiSportsReleaseGateConfig.from_env()
        config = ApiSportsReleaseGateConfig(
            min_ready_runs=base_config.min_ready_runs,
            min_ready_audit_bundles=base_config.min_ready_audit_bundles,
            allow_warnings=args.allow_warnings or base_config.allow_warnings,
            require_audit_bundle=False if args.no_require_audit_bundle else base_config.require_audit_bundle,
        )

        if args.output:
            manifest = write_api_sports_release_manifest(
                ledger_path=args.ledger,
                bundle_dir=args.bundle_dir,
                release_gate_path=args.release_gate,
                output_path=args.output,
                config=config,
                include_git=not args.no_git,
            )
        else:
            manifest = build_api_sports_release_manifest(
                ledger_path=args.ledger,
                bundle_dir=args.bundle_dir,
                release_gate_path=args.release_gate,
                config=config,
                include_git=not args.no_git,
            )

        print(json.dumps(manifest.to_dict(), indent=2, ensure_ascii=True, sort_keys=True))
        return 1 if args.require_ready and not manifest.release_ready else 0

    except ApiSportsReleaseManifestError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
