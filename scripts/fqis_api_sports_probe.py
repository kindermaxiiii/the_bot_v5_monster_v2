from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import date
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.client import ApiSportsClient
from app.fqis.integrations.api_sports.config import ApiSportsConfig, ApiSportsConfigError


def _load_dotenv_if_available() -> None:
    env_path = Path(".env")
    if not env_path.exists():
        return

    for raw_line in env_path.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        value = value.strip().strip('"').strip("'")
        if key and key not in os.environ:
            os.environ[key] = value


def main() -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports connectivity probe.")
    parser.add_argument("--date", default=date.today().isoformat())
    parser.add_argument("--timezone", default="Europe/Paris")
    parser.add_argument("--include-odds", action="store_true")
    args = parser.parse_args()

    _load_dotenv_if_available()

    try:
        config = ApiSportsConfig.from_env(require_key=True)
    except ApiSportsConfigError as exc:
        print(json.dumps({"status": "FAILED", "reason": str(exc)}, indent=2, ensure_ascii=False))
        return 2

    client = ApiSportsClient(config)
    report: dict[str, Any] = {
        "status": "STARTED",
        "mode": "shadow_only_probe",
        "provider": "api_sports_api_football",
        "config": config.redacted(),
        "probe_date": args.date,
        "timezone": args.timezone,
        "checks": {},
    }

    try:
        status = client.status()
        report["checks"]["status"] = {
            "endpoint": status.endpoint,
            "results": status.results,
            "rate_limit": _rate_limit_dict(client),
        }

        countries = client.countries()
        report["checks"]["countries"] = {
            "endpoint": countries.endpoint,
            "results": countries.results,
            "sample_size": len(countries.response or []),
            "rate_limit": _rate_limit_dict(client),
        }

        fixtures = client.fixtures_by_date(args.date, args.timezone)
        report["checks"]["fixtures_by_date"] = {
            "endpoint": fixtures.endpoint,
            "results": fixtures.results,
            "sample_size": len(fixtures.response or []),
            "rate_limit": _rate_limit_dict(client),
        }

        if args.include_odds:
            odds = client.odds_by_date(args.date, args.timezone)
            report["checks"]["odds_by_date"] = {
                "endpoint": odds.endpoint,
                "results": odds.results,
                "sample_size": len(odds.response or []),
                "paging": {
                    "current": odds.paging.current,
                    "total": odds.paging.total,
                },
                "rate_limit": _rate_limit_dict(client),
            }

        report["status"] = "COMPLETED"
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 0

    except Exception as exc:
        report["status"] = "FAILED"
        report["error_type"] = type(exc).__name__
        report["error"] = str(exc)
        print(json.dumps(report, indent=2, ensure_ascii=False))
        return 1


def _rate_limit_dict(client: ApiSportsClient) -> dict[str, int | None]:
    state = client.last_rate_limit_state
    if state is None:
        return {}

    return {
        "requests_limit": state.requests_limit,
        "requests_remaining": state.requests_remaining,
        "per_minute_limit": state.per_minute_limit,
        "per_minute_remaining": state.per_minute_remaining,
    }


if __name__ == "__main__":
    raise SystemExit(main())
