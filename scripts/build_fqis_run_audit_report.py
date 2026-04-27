from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.reporting.run_audit import (
    build_run_audit_report,
    run_audit_report_to_record,
    write_run_audit_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Build global FQIS run audit report.")
    parser.add_argument("--hybrid-batch-path", required=True)
    parser.add_argument("--settlement-path", required=True)
    parser.add_argument("--closing-path", required=True)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = build_run_audit_report(
        hybrid_batch_path=Path(args.hybrid_batch_path),
        settlement_path=Path(args.settlement_path),
        closing_path=Path(args.closing_path),
        run_id=args.run_id,
    )
    record = run_audit_report_to_record(report)

    if args.output_path:
        write_run_audit_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        metrics = report.headline_metrics

        print(
            "fqis_run_audit_report "
            f"status={report.status} "
            f"health={report.health_status} "
            f"run_id={report.run_id} "
            f"flags={report.flag_count} "
            f"fails={report.fail_count} "
            f"warns={report.warn_count} "
            f"infos={report.info_count} "
            f"matches={_format_value(metrics.get('match_count'))} "
            f"accepted_bets={_format_value(metrics.get('accepted_bet_count'))} "
            f"roi={_format_optional(metrics.get('roi'))} "
            f"hit_rate={_format_optional(metrics.get('hit_rate'))} "
            f"brier={_format_optional(metrics.get('brier_score'))} "
            f"clv_beat_rate={_format_optional(metrics.get('clv_beat_rate'))} "
            f"avg_clv_pct={_format_optional(metrics.get('average_clv_percent'))}"
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