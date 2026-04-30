from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.inplay_fixtures import (  # noqa: E402
    ApiSportsInplayFixturesConfig,
    ApiSportsInplayFixturesError,
    write_api_sports_inplay_fixtures,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports in-play fixture runner.")
    parser.add_argument("--output", required=True, help="Output inplay_fixtures.json path.")
    parser.add_argument("--api-key", help="API-Sports API key. Prefer env/.env in normal use.")
    parser.add_argument("--base-url", default="https://v3.football.api-sports.io")
    parser.add_argument("--league", type=int)
    parser.add_argument("--live")
    parser.add_argument("--timezone")
    parser.add_argument("--max-fixtures", type=int)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsInplayFixturesConfig.from_env()
        config = ApiSportsInplayFixturesConfig(
            live=args.live or base_config.live,
            timezone=args.timezone or base_config.timezone,
            max_fixtures=args.max_fixtures if args.max_fixtures is not None else base_config.max_fixtures,
            live_statuses=base_config.live_statuses,
        )

        result = write_api_sports_inplay_fixtures(
            output_path=args.output,
            api_key=args.api_key,
            base_url=args.base_url,
            league=args.league,
            sample=args.sample,
            config=config,
        )

        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=True, sort_keys=True))
        return 1 if args.require_ready and not result.ready else 0

    except ApiSportsInplayFixturesError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=True))
        return 2


if __name__ == "__main__":
    raise SystemExit(main())
