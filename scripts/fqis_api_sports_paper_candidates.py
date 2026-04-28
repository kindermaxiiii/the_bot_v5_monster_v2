
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.paper_candidates import (  # noqa: E402
    ApiSportsPaperCandidateConfig,
    ApiSportsPaperCandidatesError,
    build_api_sports_paper_candidates,
    write_api_sports_paper_candidates,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports paper candidate builder.")
    parser.add_argument("--input", help="JSON source file containing candidate-like records.")
    parser.add_argument("--output", help="Output paper_candidates.json path.")
    parser.add_argument("--sample", action="store_true", help="Use deterministic sample paper candidates.")
    parser.add_argument("--default-bookmaker")
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--min-odds", type=float)
    parser.add_argument("--max-odds", type=float)
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsPaperCandidateConfig.from_env()
        config = ApiSportsPaperCandidateConfig(
            default_bookmaker=args.default_bookmaker or base_config.default_bookmaker,
            max_candidates=args.max_candidates if args.max_candidates is not None else base_config.max_candidates,
            min_odds=args.min_odds if args.min_odds is not None else base_config.min_odds,
            max_odds=args.max_odds if args.max_odds is not None else base_config.max_odds,
        )

        if args.output:
            result = write_api_sports_paper_candidates(
                source_path=args.input,
                output_path=args.output,
                sample=args.sample,
                config=config,
            )
        else:
            result = build_api_sports_paper_candidates(
                source_path=args.input,
                sample=args.sample,
                config=config,
            )

        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
        return 1 if args.require_ready and result.status != "READY" else 0

    except ApiSportsPaperCandidatesError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
