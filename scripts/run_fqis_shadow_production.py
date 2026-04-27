from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.orchestration.shadow_production import (
    ShadowProductionConfig,
    run_shadow_production,
    shadow_production_outcome_to_record,
    write_shadow_production_outcome_json,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="Run complete FQIS shadow-production orchestration.")
    parser.add_argument("--input-path", required=True)
    parser.add_argument("--results-path", required=True)
    parser.add_argument("--closing-path", required=True)
    parser.add_argument("--output-root", required=True)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--stake", type=float, default=1.0)
    parser.add_argument("--outcome-output-path", default=None)
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    outcome = run_shadow_production(
        ShadowProductionConfig(
            input_path=Path(args.input_path),
            results_path=Path(args.results_path),
            closing_path=Path(args.closing_path),
            output_root=Path(args.output_root),
            run_id=args.run_id,
            stake=args.stake,
        )
    )
    record = shadow_production_outcome_to_record(outcome)

    if args.outcome_output_path:
        write_shadow_production_outcome_json(outcome, Path(args.outcome_output_path))

    if args.json:
        print(json.dumps(record, ensure_ascii=False, sort_keys=True))
    else:
        headline = record["headline"]
        print(
            "fqis_shadow_production_complete "
            f"status={outcome.status} "
            f"run_id={outcome.run_id} "
            f"readiness={headline['readiness_status']} "
            f"level={headline['readiness_level']} "
            f"go={str(headline['is_go']).lower()} "
            f"matches={headline['matches']} "
            f"accepted_bets={headline['accepted_bets']} "
            f"settled_bets={headline['settled_bets']} "
            f"roi={_format_optional(headline['roi'])} "
            f"blockers={headline['blockers']} "
            f"warnings={headline['warnings']} "
            f"failures={headline['failures']} "
            f"bundle_files={headline['bundle_files']} "
            f"output_dir={outcome.output_dir}"
        )

    return 0


def _format_optional(value: object) -> str:
    if value is None:
        return "NA"

    return f"{float(value):.6f}"


if __name__ == "__main__":
    raise SystemExit(main())

    