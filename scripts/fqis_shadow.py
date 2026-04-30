from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


from app.fqis.config.profiles import (
    list_shadow_production_profiles,
    load_shadow_production_profile,
    shadow_production_profile_to_record,
)
from app.fqis.monitoring.shadow_monitor import (
    MonitoredShadowRunnerConfig,
    monitored_shadow_runner_outcome_to_record,
    run_monitored_shadow_runner,
)


def main() -> int:
    parser = argparse.ArgumentParser(description="One-command FQIS shadow production runner.")
    parser.add_argument("--profile", default="demo")
    parser.add_argument("--profile-path", default=None)
    parser.add_argument("--run-id", default=None)
    parser.add_argument("--outcome-output-path", default=None)
    parser.add_argument("--latest-output-path", default=None)
    parser.add_argument("--event-log-path", default=None)
    parser.add_argument("--latest-status-path", default=None)
    parser.add_argument("--no-latest", action="store_true")
    parser.add_argument("--strict-exit-code", action="store_true")
    parser.add_argument("--print-profile", action="store_true")
    parser.add_argument("--list-profiles", action="store_true")
    parser.add_argument("--json", action="store_true")

    args = parser.parse_args()

    if args.list_profiles:
        print(json.dumps({"profiles": list_shadow_production_profiles()}, sort_keys=True))
        return 0

    profile_path = Path(args.profile_path) if args.profile_path else None

    if args.print_profile:
        profile = load_shadow_production_profile(
            profile_name=args.profile,
            profile_path=profile_path,
        )
        print(json.dumps(shadow_production_profile_to_record(profile), ensure_ascii=True, sort_keys=True))
        return 0

    outcome = run_monitored_shadow_runner(
        MonitoredShadowRunnerConfig(
            profile_name=args.profile,
            profile_path=profile_path,
            run_id=args.run_id,
            outcome_output_path=Path(args.outcome_output_path) if args.outcome_output_path else None,
            latest_output_path=Path(args.latest_output_path) if args.latest_output_path else None,
            event_log_path=Path(args.event_log_path) if args.event_log_path else None,
            latest_status_path=Path(args.latest_status_path) if args.latest_status_path else None,
            write_latest=not args.no_latest,
        )
    )
    record = monitored_shadow_runner_outcome_to_record(outcome)

    if args.json:
        print(json.dumps(record, ensure_ascii=True, sort_keys=True))
    elif outcome.runner_outcome is not None:
        headline = record["runner_outcome"]["headline"]
        print(
            "fqis_shadow "
            f"status={outcome.status} "
            f"profile={outcome.profile.name} "
            f"run_id={outcome.run_id} "
            f"readiness={outcome.readiness_status} "
            f"level={outcome.readiness_level} "
            f"go={str(outcome.is_go).lower()} "
            f"matches={headline['matches']} "
            f"accepted_bets={headline['accepted_bets']} "
            f"settled_bets={headline['settled_bets']} "
            f"roi={_format_optional(headline['roi'])} "
            f"blockers={headline['blockers']} "
            f"warnings={headline['warnings']} "
            f"failures={headline['failures']} "
            f"event_log={outcome.event_log_path} "
            f"latest_status={outcome.latest_status_path}"
        )
    else:
        error = outcome.error or {}
        print(
            "fqis_shadow "
            f"status=failed "
            f"profile={outcome.profile.name} "
            f"run_id={outcome.run_id} "
            f"error_type={error.get('error_type', 'UNKNOWN')} "
            f"message={_safe_message(error.get('message', ''))} "
            f"event_log={outcome.event_log_path} "
            f"latest_status={outcome.latest_status_path}"
        )

    if not outcome.is_success:
        return 1

    if args.strict_exit_code and not outcome.is_go:
        return 2

    return 0


def _format_optional(value: object) -> str:
    if value is None:
        return "NA"

    return f"{float(value):.6f}"


def _safe_message(message: object) -> str:
    return str(message).replace(" ", "_")


if __name__ == "__main__":
    raise SystemExit(main())
