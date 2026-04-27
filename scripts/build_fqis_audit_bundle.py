from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.reporting.audit_bundle import (
    audit_bundle_manifest_to_record,
    build_audit_bundle,
    write_audit_bundle_manifest_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build complete FQIS audit bundle for one run.")
    parser.add_argument("--hybrid-batch-path", required=True)
    parser.add_argument("--settlement-path", required=True)
    parser.add_argument("--closing-path", required=True)
    parser.add_argument("--output-dir", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--manifest-output-path", default=None)
    parser.add_argument("--no-input-copies", action="store_true")
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    manifest = build_audit_bundle(
        hybrid_batch_path=Path(args.hybrid_batch_path),
        settlement_path=Path(args.settlement_path),
        closing_path=Path(args.closing_path),
        output_dir=Path(args.output_dir),
        run_id=args.run_id,
        include_input_copies=not args.no_input_copies,
    )
    record = audit_bundle_manifest_to_record(manifest)

    if args.manifest_output_path:
        write_audit_bundle_manifest_json(manifest, Path(args.manifest_output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        metrics = manifest.headline_metrics

        print(
            "fqis_audit_bundle_built "
            f"status={manifest.status} "
            f"run_id={manifest.run_id} "
            f"health={manifest.health_status} "
            f"files={manifest.file_count} "
            f"size_bytes={manifest.total_size_bytes} "
            f"flags={manifest.flag_count} "
            f"fails={manifest.fail_count} "
            f"warns={manifest.warn_count} "
            f"infos={manifest.info_count} "
            f"accepted_bets={_format_value(metrics.get('accepted_bet_count'))} "
            f"roi={_format_optional(metrics.get('roi'))} "
            f"clv_beat_rate={_format_optional(metrics.get('clv_beat_rate'))} "
            f"bundle_dir={manifest.bundle_dir}"
        )

    return 0


def _format_optional(value: object) -> str:
    if value is None:
        return "NA"

    return f"{float(value):.6f}"


def _format_value(value: object) -> str:
    if value is None:
        return "NA"

    return str(value)


if __name__ == "__main__":
    raise SystemExit(main())