from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.reporting.hybrid_shadow_report import (
    build_hybrid_shadow_batch_report_from_jsonl,
    hybrid_shadow_batch_report_to_record,
    write_hybrid_shadow_batch_report_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize FQIS hybrid shadow batch diagnostics.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--output-path", default=None)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    report = build_hybrid_shadow_batch_report_from_jsonl(Path(args.input_path))
    record = hybrid_shadow_batch_report_to_record(report)

    if args.output_path:
        write_hybrid_shadow_batch_report_json(report, Path(args.output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        print(
            "fqis_hybrid_shadow_batch_report "
            f"status={report.status} "
            f"batches={report.batch_count} "
            f"matches={report.match_count} "
            f"accepted_matches={report.accepted_match_count} "
            f"rejected_matches={report.rejected_match_count} "
            f"accepted_bets={report.accepted_bet_count} "
            f"theses={report.thesis_count} "
            f"hybrid_probabilities={report.hybrid_probability_count} "
            f"hybrid_count={report.hybrid_count} "
            f"model_only_count={report.model_only_count} "
            f"acceptance_rate={report.acceptance_rate:.2%} "
            f"p_hybrid_mean={_format_optional(record['numeric_summaries']['p_hybrid']['mean'])} "
            f"delta_model_market_mean={_format_optional(record['numeric_summaries']['delta_model_market']['mean'])}"
        )

    return 0


def _format_optional(value: float | None) -> str:
    if value is None:
        return "NA"

    return f"{value:.6f}"


if __name__ == "__main__":
    raise SystemExit(main())

    