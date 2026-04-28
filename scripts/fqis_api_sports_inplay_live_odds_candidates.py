from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.inplay_live_odds_candidates import (  # noqa: E402
    ApiSportsInplayLiveOddsCandidatesError,
    ApiSportsInplayLiveOddsConfig,
    write_api_sports_inplay_live_odds_candidates,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports in-play live odds candidate runner.")
    parser.add_argument("--output", required=True, help="Output paper_candidates.json path.")
    parser.add_argument("--api-key", help="API-Sports API key. Prefer env/.env in normal use.")
    parser.add_argument("--base-url", default="https://v3.football.api-sports.io")
    parser.add_argument("--fixture", type=int)
    parser.add_argument("--league", type=int)
    parser.add_argument("--bet", type=int)
    parser.add_argument("--max-candidates", type=int)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsInplayLiveOddsConfig.from_env()
        config = ApiSportsInplayLiveOddsConfig(
            max_candidates=args.max_candidates if args.max_candidates is not None else base_config.max_candidates,
            min_odds=base_config.min_odds,
            max_odds=base_config.max_odds,
            baseline_probability_discount=base_config.baseline_probability_discount,
            require_main_values=base_config.require_main_values,
            reject_suspended=base_config.reject_suspended,
        )

        result = write_api_sports_inplay_live_odds_candidates(
            output_path=args.output,
            api_key=args.api_key,
            base_url=args.base_url,
            fixture=args.fixture,
            league=args.league,
            bet=args.bet,
            sample=args.sample,
            config=config,
        )

        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=False, sort_keys=True))
        return 1 if args.require_ready and not result.ready else 0

    except ApiSportsInplayLiveOddsCandidatesError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
