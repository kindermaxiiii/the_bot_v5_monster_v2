from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.quality_gates import (  # noqa: E402
    ApiSportsQualityGateConfig,
    ApiSportsQualityGateError,
    ApiSportsQualityStatus,
    evaluate_snapshot_quality_file,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports normalized snapshot quality gate.")
    parser.add_argument("--input", required=True, help="Path to a normalized API-Sports JSON snapshot.")
    parser.add_argument("--output", help="Optional path where the quality report JSON will be written.")
    parser.add_argument("--strict", action="store_true", help="Return non-zero on WARN as well as BLOCKED.")
    parser.add_argument("--min-fixtures", type=int)
    parser.add_argument("--min-offers", type=int)
    args = parser.parse_args(argv)

    input_path = Path(args.input)
    if not input_path.exists():
        print(
            json.dumps(
                {"status": "FAILED", "reason": f"Input path does not exist: {input_path}"},
                indent=2,
                ensure_ascii=False,
            )
        )
        return 2

    try:
        config = ApiSportsQualityGateConfig.from_env()
        if args.min_fixtures is not None:
            config = ApiSportsQualityGateConfig(**{**config.to_dict(), "min_fixtures": args.min_fixtures})
        if args.min_offers is not None:
            config = ApiSportsQualityGateConfig(**{**config.to_dict(), "min_offers": args.min_offers})

        report = evaluate_snapshot_quality_file(input_path, config=config)
    except ApiSportsQualityGateError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2

    payload = report.to_dict()
    output = json.dumps(payload, indent=2, ensure_ascii=False, sort_keys=True)
    print(output)

    if args.output:
        output_path = Path(args.output)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(output + "\n", encoding="utf-8")

    if report.status is ApiSportsQualityStatus.BLOCKED:
        return 1
    if args.strict and report.status is ApiSportsQualityStatus.WARN:
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())