from __future__ import annotations

import argparse
import json
import os
import sys
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from app.config import settings
from app.vnext.live.models import LiveSnapshot
from app.vnext.notifier import build_vnext_notifier
from app.vnext.ops.inspection import resolve_latest_run_index_path, write_latest_run_index
from app.vnext.ops.reporter import build_runtime_report, format_runtime_report
from app.vnext.ops.replay import replay_runtime_export
from app.vnext.ops.runtime_cli import (
    EXIT_INSPECT_SOURCE_FAILED,
    EXIT_LIVE_SOURCE_UNAVAILABLE,
    EXIT_PATH_UNWRITABLE,
    EXIT_PREFLIGHT_FAILED,
    EXIT_REPLAY_SOURCE_FAILED,
    EXIT_SUCCESS,
    EXIT_SUCCESS_DEGRADED,
    derive_run_manifest_path,
    probe_file_output_path,
    write_json_document,
)
from app.vnext.ops.store import VnextOpsStore
from app.vnext.runtime.deduper import Deduper
from app.vnext.runtime.demo_data import build_demo_prior_provider, build_demo_snapshot_source
from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.logger import format_cycle_log
from app.vnext.runtime.models import VnextRuntimeConfig
from app.vnext.runtime.prior_provider import LivePriorResultProvider
from app.vnext.runtime.runner import run_vnext_cycle
from app.vnext.runtime.source import LiveApiSource


@dataclass(slots=True, frozen=True)
class RunPreflight:
    status: str
    source_requested: str
    source_resolved: str
    notifier_requested: str
    notifier_resolved: str
    persist_state: bool
    export_path: str
    report_path: str
    manifest_path: str
    ops_store_path: str
    warnings: tuple[str, ...] = ()
    errors: tuple[str, ...] = ()


def _write_report(path: Path, report: dict[str, object]) -> None:
    write_json_document(path, report)


def _bool_text(value: bool) -> str:
    return str(value).lower()


def _target_text(path: str) -> str:
    return path or "-"


def _is_path_failure(preflight: RunPreflight) -> bool:
    return any(error.endswith("_unwritable") for error in preflight.errors)


def _build_preflight(
    *,
    source_requested: str,
    source_resolved: str,
    notifier_requested: str,
    notifier_resolved: str,
    notifier_warning: str | None,
    persist_state: bool,
    export_path: str,
    report_path: str,
) -> RunPreflight:
    warnings: list[str] = []
    errors: list[str] = []
    ops_store_path = ""

    if source_requested == "live" and not settings.api_football_key:
        errors.append("live_api_key_missing")

    if notifier_warning is not None:
        warnings.append(notifier_warning)

    try:
        probe_file_output_path(Path(export_path))
    except OSError:
        errors.append("export_path_unwritable")

    if report_path:
        try:
            probe_file_output_path(Path(report_path))
        except OSError:
            errors.append("report_path_unwritable")

    if persist_state:
        ops_store_path = str(Path("exports") / "vnext" / "ops" / source_resolved)
        try:
            VnextOpsStore(Path(ops_store_path)).probe_write_access()
        except OSError:
            errors.append("ops_store_unwritable")

    status = "ready"
    if errors:
        status = "refused"
    elif warnings:
        status = "degraded"

    return RunPreflight(
        status=status,
        source_requested=source_requested,
        source_resolved=source_resolved,
        notifier_requested=notifier_requested,
        notifier_resolved=notifier_resolved,
        persist_state=persist_state,
        export_path=export_path,
        report_path=report_path,
        manifest_path=str(derive_run_manifest_path(Path(export_path))),
        ops_store_path=ops_store_path,
        warnings=tuple(warnings),
        errors=tuple(errors),
    )


def _print_preflight(preflight: RunPreflight) -> None:
    print(
        "vnext_preflight "
        f"status={preflight.status} "
        f"source_requested={preflight.source_requested} "
        f"source_resolved={preflight.source_resolved} "
        f"notifier_requested={preflight.notifier_requested} "
        f"notifier_resolved={preflight.notifier_resolved} "
        f"persist_state={_bool_text(preflight.persist_state)} "
        f"export_path={preflight.export_path} "
        f"report_path={_target_text(preflight.report_path)} "
        f"ops_store={_target_text(preflight.ops_store_path)}"
    )
    for warning in preflight.warnings:
        print(f"vnext_preflight_warning reason={warning}", file=sys.stderr)
    for error in preflight.errors:
        if error == "export_path_unwritable":
            target = preflight.export_path
        elif error == "report_path_unwritable":
            target = preflight.report_path
        elif error == "ops_store_unwritable":
            target = preflight.ops_store_path
        else:
            target = ""
        suffix = f" path={target}" if target else ""
        print(f"vnext_preflight_error reason={error}{suffix}", file=sys.stderr)


def _print_start_summary(
    *,
    source_name: str,
    notifier_name: str,
    persist_state: bool,
    export_path: str,
    report_path: str,
) -> None:
    print(
        "vnext_run_start "
        f"source={source_name} "
        f"notifier={notifier_name} "
        f"persist_state={_bool_text(persist_state)} "
        f"export_path={export_path} "
        f"report_path={report_path or '-'}"
    )


def _build_run_manifest(
    *,
    preflight: RunPreflight,
    started_at: datetime,
    finished_at: datetime,
    final_status: str,
    cycles_requested: int,
    cycles_executed: int,
    ops_flags: tuple[str, ...],
) -> dict[str, object]:
    return {
        "started_at_utc": started_at.isoformat(),
        "finished_at_utc": finished_at.isoformat(),
        "status": final_status,
        "source_requested": preflight.source_requested,
        "source_resolved": preflight.source_resolved,
        "notifier_requested": preflight.notifier_requested,
        "notifier_resolved": preflight.notifier_resolved,
        "persist_state": preflight.persist_state,
        "export_path": preflight.export_path,
        "report_path": preflight.report_path or None,
        "manifest_path": preflight.manifest_path,
        "ops_store_path": preflight.ops_store_path or None,
        "preflight_status": preflight.status,
        "preflight_warnings": list(preflight.warnings),
        "preflight_errors": list(preflight.errors),
        "cycles_requested": cycles_requested,
        "cycles_executed": cycles_executed,
        "ops_flags": list(ops_flags),
    }


def _try_update_latest_run(manifest_path: Path, manifest: dict[str, object]) -> None:
    try:
        write_latest_run_index(manifest_path, manifest)
    except OSError:
        print(
            f"vnext_runtime_warning reason=latest_run_index_unavailable path={resolve_latest_run_index_path()}",
            file=sys.stderr,
        )


def _write_manifest(
    *,
    preflight: RunPreflight,
    started_at: datetime,
    finished_at: datetime,
    final_status: str,
    cycles_requested: int,
    cycles_executed: int,
    ops_flags: tuple[str, ...],
) -> dict[str, object]:
    manifest = _build_run_manifest(
        preflight=preflight,
        started_at=started_at,
        finished_at=finished_at,
        final_status=final_status,
        cycles_requested=cycles_requested,
        cycles_executed=cycles_executed,
        ops_flags=ops_flags,
    )
    manifest_path = Path(preflight.manifest_path)
    write_json_document(manifest_path, manifest)
    _try_update_latest_run(manifest_path, manifest)
    return manifest


def _try_write_manifest(
    *,
    preflight: RunPreflight,
    started_at: datetime,
    final_status: str,
    cycles_requested: int,
    cycles_executed: int,
    ops_flags: tuple[str, ...],
) -> None:
    try:
        _write_manifest(
            preflight=preflight,
            started_at=started_at,
            finished_at=datetime.now(timezone.utc),
            final_status=final_status,
            cycles_requested=cycles_requested,
            cycles_executed=cycles_executed,
            ops_flags=ops_flags,
        )
    except OSError:
        return


def _print_run_complete(
    *,
    final_status: str,
    cycles_requested: int,
    cycles_executed: int,
    ops_flags: tuple[str, ...],
) -> None:
    print(
        "vnext_run_complete "
        f"status={final_status} "
        f"cycles_requested={cycles_requested} "
        f"cycles_executed={cycles_executed} "
        f"ops_flags={list(ops_flags)}"
    )


def _run_replay(jsonl_path: str, report_path: str) -> int:
    if report_path:
        try:
            probe_file_output_path(Path(report_path))
        except OSError:
            print(
                f"vnext_replay_error reason=path_unwritable path={report_path}",
                file=sys.stderr,
            )
            return EXIT_PATH_UNWRITABLE

    try:
        cycles = replay_runtime_export(Path(jsonl_path))
        report = build_runtime_report(cycles)
        print(format_runtime_report(report))
        if report_path:
            _write_report(Path(report_path), report)
    except FileNotFoundError:
        print(
            f"vnext_replay_error reason=replay_source_missing path={jsonl_path}",
            file=sys.stderr,
        )
        return EXIT_REPLAY_SOURCE_FAILED
    except ValueError:
        print(
            f"vnext_replay_error reason=replay_source_invalid path={jsonl_path}",
            file=sys.stderr,
        )
        return EXIT_REPLAY_SOURCE_FAILED
    except OSError:
        target = report_path or jsonl_path
        print(
            f"vnext_replay_error reason=path_unwritable path={target}",
            file=sys.stderr,
        )
        return EXIT_PATH_UNWRITABLE

    return EXIT_SUCCESS


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--source", choices=("demo", "live"), default="demo")
    parser.add_argument("--notifier", choices=("none", "discord"), default="none")
    parser.add_argument("--discord-webhook-url", type=str, default="")
    parser.add_argument("--cycles", type=int, default=1)
    parser.add_argument("--max-active-matches", type=int, default=18)
    parser.add_argument("--cooldown-seconds", type=int, default=180)
    parser.add_argument("--team-history-limit", type=int, default=8)
    parser.add_argument("--export-path", type=str, default="")
    parser.add_argument("--persist-state", action="store_true")
    parser.add_argument("--replay-jsonl", type=str, default="")
    parser.add_argument("--report", type=str, default="")
    args = parser.parse_args()

    if args.replay_jsonl:
        return _run_replay(args.replay_jsonl, args.report)

    if args.source == "demo":
        source = build_demo_snapshot_source()
        prior_provider = build_demo_prior_provider()
        source_type = "snapshot"
        source_name = "demo"
    else:
        source = LiveApiSource(max_fixtures=args.max_active_matches)
        prior_provider = LivePriorResultProvider(team_history_limit=args.team_history_limit)
        source_type = "live"
        source_name = "api_football"

    notifier_binding = build_vnext_notifier(
        args.notifier,  # type: ignore[arg-type]
        discord_webhook_url=args.discord_webhook_url,
    )

    config = VnextRuntimeConfig(
        max_active_matches=args.max_active_matches,
        enable_publication_build=True,
        enable_notifier_send=notifier_binding.enable_send,
        dedupe_cooldown_seconds=args.cooldown_seconds,
        source_type=source_type,  # type: ignore[arg-type]
        source_name=source_name,
    )
    deduper = Deduper(cooldown_seconds=args.cooldown_seconds)
    previous_snapshots: dict[int, LiveSnapshot] = {}

    export_path = args.export_path
    if not export_path:
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S")
        export_path = f"exports/vnext/run_vnext_shadow_{timestamp}.jsonl"

    preflight = _build_preflight(
        source_requested=args.source,
        source_resolved=source_name,
        notifier_requested=args.notifier,
        notifier_resolved=notifier_binding.resolved_kind,
        notifier_warning=notifier_binding.warning,
        persist_state=args.persist_state,
        export_path=export_path,
        report_path=args.report,
    )
    started_at = datetime.now(timezone.utc)
    _print_preflight(preflight)
    if preflight.status == "refused":
        final_status = "path_unwritable" if _is_path_failure(preflight) else "preflight_failed"
        _try_write_manifest(
            preflight=preflight,
            started_at=started_at,
            final_status=final_status,
            cycles_requested=args.cycles,
            cycles_executed=0,
            ops_flags=(),
        )
        if final_status == "path_unwritable":
            return EXIT_PATH_UNWRITABLE
        return EXIT_PREFLIGHT_FAILED

    ops_store = None
    if args.persist_state:
        ops_store = VnextOpsStore(Path(preflight.ops_store_path))

    _print_start_summary(
        source_name=source_name,
        notifier_name=notifier_binding.resolved_kind,
        persist_state=args.persist_state,
        export_path=export_path,
        report_path=args.report,
    )

    cycles_executed = 0
    global_ops_flags: set[str] = set()
    try:
        for cycle_id in range(1, args.cycles + 1):
            cycle = run_vnext_cycle(
                cycle_id=cycle_id,
                config=config,
                source=source,
                prior_result_provider=prior_provider,
                deduper=deduper,
                previous_snapshots=previous_snapshots,
                notifier=notifier_binding.notifier,
                ops_store=ops_store,
            )
            cycles_executed += 1
            global_ops_flags.update(cycle.ops_flags)
            print(format_cycle_log(cycle))
            export_cycle_jsonl(Path(export_path), cycle)

        if args.report:
            report = build_runtime_report(replay_runtime_export(Path(export_path)))
            _write_report(Path(args.report), report)
            print(format_runtime_report(report))
    except OSError:
        _try_write_manifest(
            preflight=preflight,
            started_at=started_at,
            final_status="path_unwritable",
            cycles_requested=args.cycles,
            cycles_executed=cycles_executed,
            ops_flags=tuple(sorted(global_ops_flags)),
        )
        target = args.report or export_path
        print(
            f"vnext_runtime_error reason=path_unwritable path={target}",
            file=sys.stderr,
        )
        return EXIT_PATH_UNWRITABLE
    except RuntimeError as exc:
        if args.source == "live":
            _try_write_manifest(
                preflight=preflight,
                started_at=started_at,
                final_status="live_source_unavailable",
                cycles_requested=args.cycles,
                cycles_executed=cycles_executed,
                ops_flags=tuple(sorted(global_ops_flags)),
            )
            print(
                f"vnext_runtime_error reason=live_source_unavailable detail={str(exc) or 'unknown'}",
                file=sys.stderr,
            )
            return EXIT_LIVE_SOURCE_UNAVAILABLE
        raise

    final_status = "success"
    if preflight.status == "degraded" or global_ops_flags:
        final_status = "success_degraded"

    sorted_ops_flags = tuple(sorted(global_ops_flags))
    _write_manifest(
        preflight=preflight,
        started_at=started_at,
        finished_at=datetime.now(timezone.utc),
        final_status=final_status,
        cycles_requested=args.cycles,
        cycles_executed=cycles_executed,
        ops_flags=sorted_ops_flags,
    )
    _print_run_complete(
        final_status=final_status,
        cycles_requested=args.cycles,
        cycles_executed=cycles_executed,
        ops_flags=sorted_ops_flags,
    )

    # Publish latest successful export to the live path and validate it.
    if final_status.startswith("success"):
        publish_target = os.environ.get("VNEXT_LIVE_EXPORT_PATH", "")
        if publish_target:
            live_path = Path(publish_target)
        else:
            live_path = Path("exports") / "vnext" / "live_bot.jsonl"

        try:
            from app.vnext.ops.publisher import PublishError, publish_and_validate

            publish_result = publish_and_validate(Path(export_path), live_path)
            print(
                f"vnext_live_published path={live_path} "
                f"rows_with_missing_audits={publish_result.get('rows_with_missing_audits', 0)}"
            )
        except PublishError as exc:
            print(f"vnext_live_publish_error reason={exc}", file=sys.stderr)
            return EXIT_INSPECT_SOURCE_FAILED
        except Exception as exc:
            print(
                f"vnext_live_publish_error reason=unexpected detail={exc}",
                file=sys.stderr,
            )
            return EXIT_INSPECT_SOURCE_FAILED

    if final_status == "success_degraded":
        return EXIT_SUCCESS_DEGRADED
    return EXIT_SUCCESS


if __name__ == "__main__":
    raise SystemExit(main())