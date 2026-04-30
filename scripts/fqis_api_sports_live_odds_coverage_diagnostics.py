from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.live_odds_coverage_diagnostics import (  # noqa: E402
    ApiSportsLiveOddsCoverageDiagnosticsConfig,
    ApiSportsLiveOddsCoverageDiagnosticsError,
    build_api_sports_live_odds_coverage_diagnostics,
    fetch_api_sports_live_odds_payload,
    render_api_sports_live_odds_coverage_diagnostics_markdown,
    sample_live_odds_coverage_payloads,
)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports live odds coverage diagnostics.")
    parser.add_argument("--fixtures", help="Input inplay_fixtures.json path.")
    parser.add_argument("--raw-odds", help="Optional raw odds/live JSON input path.")
    parser.add_argument("--raw-output", help="Optional path to save fetched raw odds/live JSON.")
    parser.add_argument("--candidates", help="Optional candidates JSON path.")
    parser.add_argument("--output", required=True, help="Output diagnostics JSON path.")
    parser.add_argument("--markdown", help="Optional markdown output path.")
    parser.add_argument("--api-key", help="API-Sports API key. Prefer env/.env in normal use.")
    parser.add_argument("--base-url", default="https://v3.football.api-sports.io")
    parser.add_argument("--fixture", type=int, help="Optional fixture id for focused odds/live probe.")
    parser.add_argument("--min-odds", type=float)
    parser.add_argument("--max-odds", type=float)
    parser.add_argument("--sample", action="store_true")
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        base_config = ApiSportsLiveOddsCoverageDiagnosticsConfig.from_env()
        config = ApiSportsLiveOddsCoverageDiagnosticsConfig(
            min_odds=args.min_odds if args.min_odds is not None else base_config.min_odds,
            max_odds=args.max_odds if args.max_odds is not None else base_config.max_odds,
        )

        if args.sample:
            fixtures_payload, odds_payload, candidates_payload = sample_live_odds_coverage_payloads()
            result = build_api_sports_live_odds_coverage_diagnostics(
                fixtures_payload=fixtures_payload,
                odds_payload=odds_payload,
                candidates_payload=candidates_payload,
                config=config,
            )
        else:
            odds_payload = None
            odds_path = args.raw_odds

            if odds_path is None:
                api_key = args.api_key or _api_key_from_env_or_dotenv()
                odds_payload = fetch_api_sports_live_odds_payload(
                    api_key=api_key,
                    base_url=args.base_url,
                    fixture=args.fixture,
                )

                if args.raw_output:
                    raw_target = Path(args.raw_output)
                    raw_target.parent.mkdir(parents=True, exist_ok=True)
                    raw_target.write_text(
                        json.dumps(odds_payload, indent=2, ensure_ascii=False, sort_keys=True) + "\n",
                        encoding="utf-8",
                    )
                    odds_path = str(raw_target)

            result = build_api_sports_live_odds_coverage_diagnostics(
                fixtures_path=args.fixtures,
                odds_path=odds_path,
                odds_payload=odds_payload,
                candidates_path=args.candidates,
                config=config,
            )

        output = Path(args.output)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(result.to_dict(), indent=2, ensure_ascii=False, sort_keys=True) + "\n", encoding="utf-8")

        if args.markdown:
            markdown = Path(args.markdown)
            markdown.parent.mkdir(parents=True, exist_ok=True)
            markdown.write_text(render_api_sports_live_odds_coverage_diagnostics_markdown(result), encoding="utf-8")

        print(json.dumps(result.to_dict(), indent=2, ensure_ascii=True, sort_keys=True))

        if args.markdown:
            print(render_api_sports_live_odds_coverage_diagnostics_markdown(result))

        return 1 if args.require_ready and not result.ready else 0

    except ApiSportsLiveOddsCoverageDiagnosticsError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=True))
        return 2


def _api_key_from_env_or_dotenv() -> str:
    names = (
        "APISPORTS_API_KEY",
        "APISPORTS_KEY",
        "API_SPORTS_KEY",
        "API_FOOTBALL_KEY",
        "RAPIDAPI_KEY",
    )

    for name in names:
        value = os.getenv(name)
        if value:
            return value.strip()

    env_path = Path(".env")
    if env_path.exists():
        for raw_line in env_path.read_text(encoding="utf-8-sig").splitlines():
            line = raw_line.strip()
            if not line or line.startswith("#") or "=" not in line:
                continue

            key, value = line.split("=", 1)
            if key.strip() in names:
                return value.strip().strip('"').strip("'")

    return ""


if __name__ == "__main__":
    raise SystemExit(main())
