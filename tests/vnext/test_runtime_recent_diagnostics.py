from __future__ import annotations

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from app.vnext.ops.inspection import write_latest_run_index
from app.vnext.ops.runtime_cli import (
    EXIT_INSPECT_SOURCE_FAILED,
    EXIT_LATEST_RUN_MISSING,
    EXIT_PATH_UNREADABLE,
    EXIT_SUCCESS,
    write_json_document,
)
from app.vnext.publication.models import PublicMatchPayload
from app.vnext.runtime.exporter import export_cycle_jsonl
from app.vnext.runtime.models import RuntimeCounters, RuntimeCycleResult


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def _clean_env() -> dict[str, str]:
    env = os.environ.copy()
    env.pop("PYTHONPATH", None)
    env.pop("VNEXT_LATEST_RUN_PATH", None)
    return env


def _case_root(name: str) -> Path:
    root = Path("exports") / "pytest_runtime_recent_diag" / f"{name}_{uuid4().hex}"
    root.mkdir(parents=True, exist_ok=True)
    return root


def _latest_run_path(case_root: Path) -> Path:
    return case_root / "latest_run.json"


def _payload(*, fixture_id: int, label: str, odds: str) -> PublicMatchPayload:
    return PublicMatchPayload(
        fixture_id=fixture_id,
        public_status="WATCHLIST",
        publish_channel="WATCHLIST",
        match_label=label,
        competition_label="Premier Test",
        market_label="TEAM_TOTAL",
        line_label="Team Total Away Under Core",
        bookmaker_label="Book 1",
        odds_label=odds,
        confidence_band="HIGH",
        public_summary=f"TEAM_TOTAL Team Total Away Under Core @ Book 1 {odds}",
    )


def _build_recent_export(path: Path) -> None:
    export_cycle_jsonl(
        path,
        RuntimeCycleResult(
            cycle_id=1,
            timestamp_utc=datetime(2026, 4, 18, 10, 0, tzinfo=timezone.utc),
            counters=RuntimeCounters(
                fixture_count_seen=2,
                computed_publish_count=0,
                deduped_count=0,
                notified_count=0,
                silent_count=0,
                unsent_shadow_count=0,
                notifier_attempt_count=0,
            ),
            payloads=(),
            refusal_summaries=(
                "posterior_too_weak",
                "candidate_not_selectable",
                "elite_thresholds_not_met",
            ),
            fixture_audits=(
                {
                    "fixture_id": 101,
                    "match_label": "Alpha vs Beta",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 1,
                    "line": 1.5,
                    "odds_decimal": 1.87,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": ["candidate_not_selectable"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 202,
                    "match_label": "Gamma vs Delta",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
            publication_records=(),
            ops_flags=(),
            notifier_mode="none",
        ),
    )
    publish_payload = _payload(fixture_id=101, label="Alpha vs Beta", odds="1.91")
    export_cycle_jsonl(
        path,
        RuntimeCycleResult(
            cycle_id=2,
            timestamp_utc=datetime(2026, 4, 18, 10, 5, tzinfo=timezone.utc),
            counters=RuntimeCounters(
                fixture_count_seen=1,
                computed_publish_count=1,
                deduped_count=0,
                notified_count=1,
                silent_count=0,
                unsent_shadow_count=0,
                notifier_attempt_count=1,
            ),
            payloads=(publish_payload,),
            refusal_summaries=(),
            fixture_audits=(
                {
                    "fixture_id": 101,
                    "match_label": "Alpha vs Beta",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "PUBLISH",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 1,
                    "line": 1.5,
                    "odds_decimal": 1.91,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
            publication_records=(
                {
                    "cycle_id": 2,
                    "timestamp_utc": datetime(2026, 4, 18, 10, 5, tzinfo=timezone.utc).isoformat(),
                    "fixture_id": 101,
                    "public_status": "WATCHLIST",
                    "publish_channel": "WATCHLIST",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 1,
                    "bookmaker_name": "Book 1",
                    "line": 1.5,
                    "odds_decimal": 1.91,
                    "public_summary": publish_payload.public_summary,
                    "disposition": "retained",
                    "notified": True,
                    "dedupe_origin": None,
                    "source": "published_artifact.v1",
                },
            ),
            ops_flags=(),
            notifier_mode="explicit_ack",
        ),
    )
    export_cycle_jsonl(
        path,
        RuntimeCycleResult(
            cycle_id=3,
            timestamp_utc=datetime(2026, 4, 18, 10, 10, tzinfo=timezone.utc),
            counters=RuntimeCounters(
                fixture_count_seen=1,
                computed_publish_count=1,
                deduped_count=1,
                notified_count=0,
                silent_count=1,
                unsent_shadow_count=0,
                notifier_attempt_count=0,
            ),
            payloads=(),
            refusal_summaries=("posterior_too_weak", "publishability_low"),
            fixture_audits=(
                {
                    "fixture_id": 303,
                    "match_label": "Epsilon vs Zeta",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "PUBLISH",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 2,
                    "line": 2.5,
                    "odds_decimal": 1.95,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
            publication_records=(
                {
                    "cycle_id": 3,
                    "timestamp_utc": datetime(2026, 4, 18, 10, 10, tzinfo=timezone.utc).isoformat(),
                    "fixture_id": 303,
                    "public_status": "WATCHLIST",
                    "publish_channel": "WATCHLIST",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 2,
                    "bookmaker_name": "Book 2",
                    "line": 2.5,
                    "odds_decimal": 1.95,
                    "public_summary": "TEAM_TOTAL Team Total Away Under Core @ Book 2 1.95",
                    "disposition": "deduped",
                    "notified": False,
                    "dedupe_origin": "deduped_in_memory",
                    "source": "published_artifact.v1",
                },
            ),
            ops_flags=("state_store_unavailable",),
            notifier_mode="none",
        ),
    )


def _build_observation_export(path: Path) -> None:
    export_cycle_jsonl(
        path,
        RuntimeCycleResult(
            cycle_id=1,
            timestamp_utc=datetime(2026, 4, 18, 14, 0, tzinfo=timezone.utc),
            counters=RuntimeCounters(
                fixture_count_seen=3,
                computed_publish_count=0,
                deduped_count=0,
                notified_count=0,
                silent_count=0,
                unsent_shadow_count=0,
                notifier_attempt_count=0,
            ),
            payloads=(),
            refusal_summaries=("candidate_not_selectable", "publishability_low", "no_offer_found"),
            fixture_audits=(
                {
                    "fixture_id": 901,
                    "match_label": "Blocked vs Selectable",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["candidate_not_selectable"],
                    "execution_refusal_summary": [],
                    "candidate_not_selectable_reason": "all_candidates_blocked",
                    "translated_candidate_count": 2,
                    "selectable_candidate_count": 0,
                    "best_candidate_family": None,
                    "best_candidate_exists": None,
                    "best_candidate_selectable": None,
                    "best_candidate_blockers": [],
                    "distinct_candidate_blockers_summary": ["low_live_snapshot_quality", "state_conflict"],
                    "execution_candidate_count": None,
                    "execution_selectable_count": None,
                    "attempted_template_keys": [],
                    "offer_present_template_keys": [],
                    "missing_offer_template_keys": [],
                    "blocked_execution_reasons_summary": [],
                    "final_execution_refusal_reason": None,
                    "publishability_score": None,
                    "template_binding_score": None,
                    "bookmaker_diversity_score": None,
                    "price_integrity_score": None,
                    "retrievability_score": None,
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 902,
                    "match_label": "Plateau vs Market",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "candidate_not_selectable_reason": None,
                    "translated_candidate_count": 4,
                    "selectable_candidate_count": 1,
                    "best_candidate_family": "OU_FT",
                    "best_candidate_exists": True,
                    "best_candidate_selectable": True,
                    "best_candidate_blockers": [],
                    "distinct_candidate_blockers_summary": ["weak_directionality"],
                    "execution_candidate_count": 2,
                    "execution_selectable_count": 0,
                    "attempted_template_keys": ["OU_FT_OVER_CORE", "BTTS_YES_CORE"],
                    "offer_present_template_keys": ["OU_FT_OVER_CORE"],
                    "missing_offer_template_keys": ["BTTS_YES_CORE"],
                    "blocked_execution_reasons_summary": ["publishability_low", "retrievability_low", "template_bind_failed"],
                    "final_execution_refusal_reason": "publishability_low",
                    "publishability_score": 0.57,
                    "template_binding_score": 1.0,
                    "bookmaker_diversity_score": 0.3333,
                    "price_integrity_score": 1.0,
                    "retrievability_score": 0.58,
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 903,
                    "match_label": "Missing vs Offers",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "candidate_not_selectable_reason": None,
                    "translated_candidate_count": 3,
                    "selectable_candidate_count": 1,
                    "best_candidate_family": "OU_FT",
                    "best_candidate_exists": True,
                    "best_candidate_selectable": True,
                    "best_candidate_blockers": [],
                    "distinct_candidate_blockers_summary": [],
                    "execution_candidate_count": 1,
                    "execution_selectable_count": 0,
                    "attempted_template_keys": ["OU_FT_OVER_CORE"],
                    "offer_present_template_keys": [],
                    "missing_offer_template_keys": ["OU_FT_OVER_CORE"],
                    "blocked_execution_reasons_summary": ["market_unavailable", "no_offer_found"],
                    "final_execution_refusal_reason": "no_offer_found",
                    "publishability_score": None,
                    "template_binding_score": None,
                    "bookmaker_diversity_score": None,
                    "price_integrity_score": None,
                    "retrievability_score": None,
                    "source": "runtime_fixture_audit.v1",
                },
            ),
            publication_records=(),
            ops_flags=(),
            notifier_mode="none",
        ),
    )


def _build_temporal_export(path: Path) -> None:
    cycles = (
        (
            1,
            "2026-04-18T11:00:00+00:00",
            (
                {
                    "fixture_id": 501,
                    "match_label": "Oscillators vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 602,
                    "match_label": "Anchors vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 703,
                    "match_label": "Steady vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            2,
            "2026-04-18T11:05:00+00:00",
            (
                {
                    "fixture_id": 501,
                    "match_label": "Oscillators vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 602,
                    "match_label": "Anchors vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 703,
                    "match_label": "Steady vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            3,
            "2026-04-18T11:10:00+00:00",
            (
                {
                    "fixture_id": 501,
                    "match_label": "Oscillators vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": ["candidate_not_selectable"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 602,
                    "match_label": "Anchors vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            4,
            "2026-04-18T11:15:00+00:00",
            (
                {
                    "fixture_id": 501,
                    "match_label": "Oscillators vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 703,
                    "match_label": "Steady vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
    )
    for cycle_id, timestamp_utc, fixture_audits in cycles:
        export_cycle_jsonl(
            path,
            RuntimeCycleResult(
                cycle_id=cycle_id,
                timestamp_utc=datetime.fromisoformat(timestamp_utc),
                counters=RuntimeCounters(
                    fixture_count_seen=len(fixture_audits),
                    computed_publish_count=0,
                    deduped_count=0,
                    notified_count=0,
                    silent_count=0,
                    unsent_shadow_count=0,
                    notifier_attempt_count=0,
                ),
                payloads=(),
                refusal_summaries=tuple(
                    sorted(
                        {
                            reason
                            for audit in fixture_audits
                            for reason in (
                                *audit["governance_refusal_summary"],
                                *audit["execution_refusal_summary"],
                            )
                        }
                    )
                ),
                fixture_audits=fixture_audits,
                publication_records=(),
                ops_flags=(),
                notifier_mode="none",
            ),
        )


def _build_near_case_export(path: Path) -> None:
    cycles = (
        (
            1,
            "2026-04-18T12:00:00+00:00",
            (
                {
                    "fixture_id": 801,
                    "match_label": "Near Publish FC vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 802,
                    "match_label": "Osc Edge vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 803,
                    "match_label": "Watch Hold vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 804,
                    "match_label": "Cold Start vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            2,
            "2026-04-18T12:05:00+00:00",
            (
                {
                    "fixture_id": 801,
                    "match_label": "Near Publish FC vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 7,
                    "line": 1.5,
                    "odds_decimal": 1.93,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 802,
                    "match_label": "Osc Edge vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 803,
                    "match_label": "Watch Hold vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 804,
                    "match_label": "Cold Start vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            3,
            "2026-04-18T12:10:00+00:00",
            (
                {
                    "fixture_id": 801,
                    "match_label": "Near Publish FC vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "PUBLISH",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 7,
                    "line": 1.5,
                    "odds_decimal": 1.95,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 802,
                    "match_label": "Osc Edge vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": ["candidate_not_selectable"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 803,
                    "match_label": "Watch Hold vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            4,
            "2026-04-18T12:15:00+00:00",
            (
                {
                    "fixture_id": 801,
                    "match_label": "Near Publish FC vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": "TEAM_TOTAL_AWAY_UNDER_CORE",
                    "bookmaker_id": 7,
                    "line": 1.5,
                    "odds_decimal": 1.91,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 802,
                    "match_label": "Osc Edge vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 803,
                    "match_label": "Watch Hold vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
    )
    for cycle_id, timestamp_utc, fixture_audits in cycles:
        export_cycle_jsonl(
            path,
            RuntimeCycleResult(
                cycle_id=cycle_id,
                timestamp_utc=datetime.fromisoformat(timestamp_utc),
                counters=RuntimeCounters(
                    fixture_count_seen=len(fixture_audits),
                    computed_publish_count=sum(
                        1 for audit in fixture_audits if audit["publish_status"] == "PUBLISH"
                    ),
                    deduped_count=0,
                    notified_count=0,
                    silent_count=0,
                    unsent_shadow_count=0,
                    notifier_attempt_count=0,
                ),
                payloads=(),
                refusal_summaries=tuple(
                    sorted(
                        {
                            reason
                            for audit in fixture_audits
                            for reason in (
                                *audit["governance_refusal_summary"],
                                *audit["execution_refusal_summary"],
                            )
                        }
                    )
                ),
                fixture_audits=fixture_audits,
                publication_records=(),
                ops_flags=(),
                notifier_mode="none",
            ),
        )


def _build_current_stale_export(path: Path) -> None:
    cycles = (
        (
            1,
            "2026-04-18T13:00:00+00:00",
            (
                {
                    "fixture_id": 901,
                    "match_label": "Active Plateau vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 902,
                    "match_label": "Historical Plateau vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 903,
                    "match_label": "Decayed Case vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 904,
                    "match_label": "Cold Stable vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            2,
            "2026-04-18T13:05:00+00:00",
            (
                {
                    "fixture_id": 901,
                    "match_label": "Active Plateau vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 902,
                    "match_label": "Historical Plateau vs United",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["no_offer_found"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 903,
                    "match_label": "Decayed Case vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 904,
                    "match_label": "Cold Stable vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            3,
            "2026-04-18T13:10:00+00:00",
            (
                {
                    "fixture_id": 901,
                    "match_label": "Active Plateau vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 903,
                    "match_label": "Decayed Case vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": ["candidate_not_selectable"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 904,
                    "match_label": "Cold Stable vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
        (
            4,
            "2026-04-18T13:15:00+00:00",
            (
                {
                    "fixture_id": 901,
                    "match_label": "Active Plateau vs City",
                    "competition_label": "Premier Test",
                    "governed_public_status": "WATCHLIST",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["elite_thresholds_not_met"],
                    "execution_refusal_summary": ["publishability_low"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 903,
                    "match_label": "Decayed Case vs Borough",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": [],
                    "execution_refusal_summary": ["candidate_not_selectable"],
                    "source": "runtime_fixture_audit.v1",
                },
                {
                    "fixture_id": 904,
                    "match_label": "Cold Stable vs Town",
                    "competition_label": "Premier Test",
                    "governed_public_status": "NO_BET",
                    "publish_status": "DO_NOT_PUBLISH",
                    "template_key": None,
                    "bookmaker_id": None,
                    "line": None,
                    "odds_decimal": None,
                    "governance_refusal_summary": ["posterior_too_weak"],
                    "execution_refusal_summary": [],
                    "source": "runtime_fixture_audit.v1",
                },
            ),
        ),
    )
    for cycle_id, timestamp_utc, fixture_audits in cycles:
        export_cycle_jsonl(
            path,
            RuntimeCycleResult(
                cycle_id=cycle_id,
                timestamp_utc=datetime.fromisoformat(timestamp_utc),
                counters=RuntimeCounters(
                    fixture_count_seen=len(fixture_audits),
                    computed_publish_count=0,
                    deduped_count=0,
                    notified_count=0,
                    silent_count=0,
                    unsent_shadow_count=0,
                    notifier_attempt_count=0,
                ),
                payloads=(),
                refusal_summaries=tuple(
                    sorted(
                        {
                            reason
                            for audit in fixture_audits
                            for reason in (
                                *audit["governance_refusal_summary"],
                                *audit["execution_refusal_summary"],
                            )
                        }
                    )
                ),
                fixture_audits=fixture_audits,
                publication_records=(),
                ops_flags=(),
                notifier_mode="none",
            ),
        )


def _write_manifest_and_latest(case_root: Path, export_path: Path, latest_path: Path) -> Path:
    manifest_path = export_path.with_suffix(".manifest.json")
    manifest = {
        "started_at_utc": "2026-04-18T10:00:00+00:00",
        "finished_at_utc": "2026-04-18T10:10:00+00:00",
        "status": "success",
        "source_requested": "demo",
        "source_resolved": "demo",
        "notifier_requested": "none",
        "notifier_resolved": "none",
        "persist_state": False,
        "export_path": str(export_path),
        "report_path": "",
        "cycles_requested": 3,
        "cycles_executed": 3,
        "preflight_status": "ready",
        "preflight_warnings": [],
        "preflight_errors": [],
        "ops_flags": [],
    }
    write_json_document(manifest_path, manifest)
    write_latest_run_index(manifest_path, manifest, path=latest_path)
    return manifest_path


def test_diagnose_script_defaults_to_latest_run() -> None:
    case_root = _case_root("recent_latest")
    export_path = case_root / "runtime.jsonl"
    latest_path = _latest_run_path(case_root)
    _build_recent_export(export_path)
    _write_manifest_and_latest(case_root, export_path, latest_path)
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py"],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_diag input=latest cohort=all window_cycles=3 "
        "first_cycle_utc=2026-04-18T10:00:00+00:00 "
        "last_cycle_utc=2026-04-18T10:10:00+00:00 "
        "status=success source=demo notifier=none"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_scope cycles_read_source=3 cycles_with_matches_filtered=3 "
        "fixture_audits_source=4 fixture_audits_filtered=4 "
        "fixtures_seen_source=3 fixtures_seen_filtered=3"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_counts publishable=2 retained=1 deduped=1 "
        "shadow_unsent=0 notify_attempts=1 notified=1 acked_records=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_chain fixture_audits=4 fixtures_seen=3 governed_non_no_bet=3 "
        "publish_candidates=2 template_attached=3 bookmaker_attached=3 offer_attached=3 "
        "retained_records=1 deduped_records=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_cutoffs no_bet_like=1 governed_but_not_publish=1 "
        "publish_deduped=1 publish_retained=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_funnel_fixture fixtures_seen=3 no_bet=1 governed_non_no_bet=2 "
        "publish=2 do_not_publish=2 template_attached=2 offer_attached=2 retained=1 deduped=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_transitions audit_transitions=[('WATCHLIST->PUBLISH', 2), "
        "('WATCHLIST->DO_NOT_PUBLISH', 1), ('NO_BET->DO_NOT_PUBLISH', 1)]"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_refusal_zones no_bet=[('elite_thresholds_not_met', 1)] "
        "governed=[('posterior_too_weak', 1), ('candidate_not_selectable', 1)] "
        "publish=[] offer_attached=[('posterior_too_weak', 1), ('candidate_not_selectable', 1)]"
    ) in completed.stdout
    assert "('posterior_too_weak', 2)" in completed.stdout
    assert "near_publish_refusals=[('posterior_too_weak', 1), ('candidate_not_selectable', 1)]" in completed.stdout
    assert "('state_store_unavailable', 1)" in completed.stdout
    assert f"latest={latest_path}" in completed.stdout
    assert (
        "vnext_recent_fixture_top fixture_id=101 match_label=Alpha vs Beta occurrences=2 "
        "last_seen_utc=2026-04-18T10:05:00+00:00 governed_public_status=WATCHLIST "
        "publish_status=PUBLISH template_key=TEAM_TOTAL_AWAY_UNDER_CORE bookmaker_id=1 "
        "line=1.5 odds_decimal=1.91 retained=1 deduped=0 watchlist_hits=2 publish_hits=1 "
        "offer_hits=2 candidate_not_selectable=1 no_best_candidate=0 publishability_low=0 "
        "no_offer_found=0 transitions=[('WATCHLIST->DO_NOT_PUBLISH', 1), ('WATCHLIST->PUBLISH', 1)]"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_top fixture_id=303 match_label=Epsilon vs Zeta occurrences=1 "
        "last_seen_utc=2026-04-18T10:10:00+00:00 governed_public_status=WATCHLIST "
        "publish_status=PUBLISH template_key=TEAM_TOTAL_AWAY_UNDER_CORE bookmaker_id=2 "
        "line=2.5 odds_decimal=1.95 retained=0 deduped=1 watchlist_hits=1 publish_hits=1 "
        "offer_hits=1 candidate_not_selectable=0 no_best_candidate=0 publishability_low=0 "
        "no_offer_found=0 transitions=[('WATCHLIST->PUBLISH', 1)]"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_top fixture_id=202 match_label=Gamma vs Delta occurrences=1 "
        "last_seen_utc=2026-04-18T10:00:00+00:00 governed_public_status=NO_BET "
        "publish_status=DO_NOT_PUBLISH template_key=- bookmaker_id=- line=- odds_decimal=- "
        "retained=0 deduped=0 watchlist_hits=0 publish_hits=0 offer_hits=0 "
        "candidate_not_selectable=0 no_best_candidate=0 publishability_low=0 "
        "no_offer_found=0 transitions=[('NO_BET->DO_NOT_PUBLISH', 1)]"
    ) in completed.stdout


def test_diagnose_script_reads_explicit_manifest_with_last_cycles() -> None:
    case_root = _case_root("recent_manifest")
    export_path = case_root / "runtime.jsonl"
    latest_path = _latest_run_path(case_root)
    _build_recent_export(export_path)
    manifest_path = _write_manifest_and_latest(case_root, export_path, latest_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--manifest",
            str(manifest_path),
            "--last-cycles",
            "2",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_diag input=manifest cohort=all window_cycles=2 "
        "first_cycle_utc=2026-04-18T10:05:00+00:00 "
        "last_cycle_utc=2026-04-18T10:10:00+00:00 "
        "status=success source=demo notifier=none"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_scope cycles_read_source=2 cycles_with_matches_filtered=2 "
        "fixture_audits_source=2 fixture_audits_filtered=2 "
        "fixtures_seen_source=2 fixtures_seen_filtered=2"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_counts publishable=2 retained=1 deduped=1 "
        "shadow_unsent=0 notify_attempts=1 notified=1 acked_records=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_chain fixture_audits=2 fixtures_seen=2 governed_non_no_bet=2 "
        "publish_candidates=2 template_attached=2 bookmaker_attached=2 offer_attached=2 "
        "retained_records=1 deduped_records=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_cutoffs no_bet_like=0 governed_but_not_publish=0 "
        "publish_deduped=1 publish_retained=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_funnel_fixture fixtures_seen=2 no_bet=0 governed_non_no_bet=2 "
        "publish=2 do_not_publish=0 template_attached=2 offer_attached=2 retained=1 deduped=1"
    ) in completed.stdout
    assert "vnext_recent_diag_transitions audit_transitions=[('WATCHLIST->PUBLISH', 2)]" in completed.stdout
    assert "vnext_recent_diag_refusal_zones no_bet=[] governed=[] publish=[] offer_attached=[]" in completed.stdout
    assert "manifest=" + str(manifest_path) in completed.stdout
    assert "latest=-" in completed.stdout


def test_diagnose_script_reads_explicit_export_with_fixture_details() -> None:
    case_root = _case_root("recent_export_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_recent_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--last-cycles",
            "3",
            "--fixture-id",
            "101",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_diag input=export cohort=all window_cycles=3 "
        "first_cycle_utc=2026-04-18T10:00:00+00:00 "
        "last_cycle_utc=2026-04-18T10:10:00+00:00 status=- source=- notifier=-"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_scope cycles_read_source=3 cycles_with_matches_filtered=3 "
        "fixture_audits_source=4 fixture_audits_filtered=4 "
        "fixtures_seen_source=3 fixtures_seen_filtered=3"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture fixture_id=101 window_cycles=3 matches=2 "
        "match_label=Alpha vs Beta first_seen_utc=2026-04-18T10:00:00+00:00 "
        "last_seen_utc=2026-04-18T10:05:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_latest governed_public_status=WATCHLIST "
        "publish_status=PUBLISH template_key=TEAM_TOTAL_AWAY_UNDER_CORE "
        "bookmaker_id=1 line=1.5 odds_decimal=1.91"
    ) in completed.stdout
    assert "recent_timestamps=['2026-04-18T10:05:00+00:00', '2026-04-18T10:00:00+00:00']" in completed.stdout
    assert "governed_public_statuses=['WATCHLIST'] publish_statuses=['PUBLISH', 'DO_NOT_PUBLISH']" in completed.stdout
    assert (
        "vnext_recent_fixture_funnel watchlist=2 governed_hits=2 no_bet=0 publish=1 "
        "do_not_publish=1 template_hits=2 offer_hits=2"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_transitions transitions=[('WATCHLIST->DO_NOT_PUBLISH', 1), "
        "('WATCHLIST->PUBLISH', 1)] governance_counts=[('posterior_too_weak', 1)] "
        "execution_counts=[('candidate_not_selectable', 1)]"
    ) in completed.stdout
    assert "governance=['posterior_too_weak'] execution=['candidate_not_selectable']" in completed.stdout
    assert "template_keys=['TEAM_TOTAL_AWAY_UNDER_CORE'] bookmaker_ids=[1] lines=[1.5] odds=[1.91, 1.87]" in completed.stdout


def test_diagnose_script_reports_zero_matches_for_missing_fixture() -> None:
    case_root = _case_root("recent_missing_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_recent_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "999",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture fixture_id=999 window_cycles=3 matches=0 "
        "match_label=- first_seen_utc=- last_seen_utc=-"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_latest governed_public_status=- publish_status=- "
        "template_key=- bookmaker_id=- line=- odds_decimal=-"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_funnel watchlist=0 governed_hits=0 no_bet=0 publish=0 "
        "do_not_publish=0 template_hits=0 offer_hits=0"
    ) in completed.stdout
    assert "vnext_recent_fixture_transitions transitions=[] governance_counts=[] execution_counts=[]" in completed.stdout
    assert "recent_timestamps=[]" in completed.stdout
    assert "governed_public_statuses=[] publish_statuses=[]" in completed.stdout
    assert "governance=[] execution=[]" in completed.stdout


def test_diagnose_script_filters_watchlist_cohort_with_source_comparison() -> None:
    case_root = _case_root("recent_watchlist_slice")
    export_path = case_root / "runtime.jsonl"
    _build_recent_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--governed-status",
            "WATCHLIST",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "cohort=governed_status:WATCHLIST" in completed.stdout
    assert (
        "vnext_recent_diag_scope cycles_read_source=3 cycles_with_matches_filtered=3 "
        "fixture_audits_source=4 fixture_audits_filtered=3 "
        "fixtures_seen_source=3 fixtures_seen_filtered=2"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_funnel_fixture fixtures_seen=2 no_bet=0 governed_non_no_bet=2 "
        "publish=2 do_not_publish=1 template_attached=2 offer_attached=2 retained=1 deduped=1"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_transitions audit_transitions=[('WATCHLIST->PUBLISH', 2), "
        "('WATCHLIST->DO_NOT_PUBLISH', 1)]"
    ) in completed.stdout
    assert "vnext_recent_diag_refusal_zones no_bet=[]" in completed.stdout
    assert "fixture_id=202" not in completed.stdout


def test_diagnose_script_filters_candidate_not_selectable_cohort_and_drilldown() -> None:
    case_root = _case_root("recent_candidate_slice")
    export_path = case_root / "runtime.jsonl"
    _build_recent_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--contains-refusal",
            "candidate_not_selectable",
            "--fixture-id",
            "101",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "cohort=contains_refusal:candidate_not_selectable" in completed.stdout
    assert (
        "vnext_recent_diag_scope cycles_read_source=3 cycles_with_matches_filtered=1 "
        "fixture_audits_source=4 fixture_audits_filtered=1 "
        "fixtures_seen_source=3 fixtures_seen_filtered=1"
    ) in completed.stdout
    assert "audit_transitions=[('WATCHLIST->DO_NOT_PUBLISH', 1)]" in completed.stdout
    assert "refusals=[('candidate_not_selectable', 1), ('posterior_too_weak', 1)]" in completed.stdout
    assert (
        "vnext_recent_fixture fixture_id=101 window_cycles=3 matches=1 "
        "match_label=Alpha vs Beta first_seen_utc=2026-04-18T10:00:00+00:00 "
        "last_seen_utc=2026-04-18T10:00:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_funnel watchlist=1 governed_hits=1 no_bet=0 publish=0 "
        "do_not_publish=1 template_hits=1 offer_hits=1"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_transitions transitions=[('WATCHLIST->DO_NOT_PUBLISH', 1)] "
        "governance_counts=[('posterior_too_weak', 1)] execution_counts=[('candidate_not_selectable', 1)]"
    ) in completed.stdout


def test_diagnose_script_reports_zero_matches_for_fixture_absent_in_cohort() -> None:
    case_root = _case_root("recent_absent_in_slice")
    export_path = case_root / "runtime.jsonl"
    _build_recent_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--contains-refusal",
            "candidate_not_selectable",
            "--fixture-id",
            "303",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "cohort=contains_refusal:candidate_not_selectable" in completed.stdout
    assert (
        "vnext_recent_fixture fixture_id=303 window_cycles=3 matches=0 "
        "match_label=- first_seen_utc=- last_seen_utc=-"
    ) in completed.stdout


def test_diagnose_script_fails_cleanly_when_filter_is_invalid() -> None:
    case_root = _case_root("recent_invalid_filter")
    export_path = case_root / "runtime.jsonl"
    _build_recent_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--governed-status",
            "BROKEN",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert "vnext_recent_error reason=filter_invalid path=governed_status:BROKEN" in completed.stderr


def test_diagnose_script_reports_temporal_behaviors_and_top_fixtures() -> None:
    case_root = _case_root("recent_temporal_global")
    export_path = case_root / "runtime.jsonl"
    _build_temporal_export(export_path)

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py", "--export", str(export_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "vnext_recent_diag_temporal behaviors=[('oscillating', 1), ('stable_no_bet', 1), ('stable_watchlist', 1)]" in completed.stdout
    assert (
        "vnext_recent_temporal_top kind=oscillating fixture_id=501 "
        "match_label=Oscillators vs United matches=4 behavior=oscillating "
        "oscillations=3 temporal_transitions=[('NO_BET->WATCHLIST', 2), ('WATCHLIST->NO_BET', 1)]"
    ) in completed.stdout
    assert (
        "vnext_recent_temporal_top kind=stable fixture_id=703 "
        "match_label=Steady vs Borough matches=3 behavior=stable_watchlist "
        "oscillations=0 temporal_transitions=[('WATCHLIST->WATCHLIST', 2)]"
    ) in completed.stdout
    assert (
        "vnext_recent_temporal_top kind=stable fixture_id=602 "
        "match_label=Anchors vs City matches=3 behavior=stable_no_bet "
        "oscillations=0 temporal_transitions=[('NO_BET->NO_BET', 2)]"
    ) in completed.stdout


def test_diagnose_script_reports_temporal_drilldown_for_fixture() -> None:
    case_root = _case_root("recent_temporal_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_temporal_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "501",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture fixture_id=501 window_cycles=4 matches=4 "
        "match_label=Oscillators vs United first_seen_utc=2026-04-18T11:00:00+00:00 "
        "last_seen_utc=2026-04-18T11:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_temporal behavior=oscillating oscillations=3 "
        "temporal_transitions=[('NO_BET->WATCHLIST', 2), ('WATCHLIST->NO_BET', 1)] "
        "recent_steps=['2026-04-18T11:05:00+00:00:NO_BET->WATCHLIST', "
        "'2026-04-18T11:10:00+00:00:WATCHLIST->NO_BET', "
        "'2026-04-18T11:15:00+00:00:NO_BET->WATCHLIST']"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_transitions transitions=[('NO_BET->DO_NOT_PUBLISH', 2), "
        "('WATCHLIST->DO_NOT_PUBLISH', 2)] governance_counts=[('elite_thresholds_not_met', 2), "
        "('posterior_too_weak', 1)] execution_counts=[('publishability_low', 1), "
        "('candidate_not_selectable', 1), ('no_offer_found', 1)]"
    ) in completed.stdout


def test_diagnose_script_reports_temporal_drilldown_inside_cohort() -> None:
    case_root = _case_root("recent_temporal_cohort_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_temporal_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--governed-status",
            "WATCHLIST",
            "--fixture-id",
            "501",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "cohort=governed_status:WATCHLIST" in completed.stdout
    assert (
        "vnext_recent_fixture fixture_id=501 window_cycles=4 matches=2 "
        "match_label=Oscillators vs United first_seen_utc=2026-04-18T11:05:00+00:00 "
        "last_seen_utc=2026-04-18T11:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_temporal behavior=stable_watchlist oscillations=0 "
        "temporal_transitions=[('WATCHLIST->WATCHLIST', 1)] "
        "recent_steps=['2026-04-18T11:15:00+00:00:WATCHLIST->WATCHLIST']"
    ) in completed.stdout


def test_diagnose_script_reports_best_near_cases_with_explicit_sort_keys() -> None:
    case_root = _case_root("recent_near_cases")
    export_path = case_root / "runtime.jsonl"
    _build_near_case_export(export_path)

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py", "--export", str(export_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_diag_near sort_keys=['publish_hits', 'template_hits', 'offer_hits', "
        "'current_governed_streak', 'longest_governed_streak', 'watchlist_hits', "
        "'governed_hits', 'oscillations', 'last_seen_utc']"
    ) in completed.stdout
    assert (
        "vnext_recent_diag_episodes sort_keys_watchlist=['longest_watchlist_episode', "
        "'watchlist_episodes', 'current_watchlist_streak', 'last_seen_utc'] "
        "sort_keys_alternating=['episodes_total', 'oscillations', "
        "'watchlist_episodes', 'last_seen_utc']"
    ) in completed.stdout
    assert (
        "vnext_recent_near_top fixture_id=801 match_label=Near Publish FC vs City "
        "behavior=mixed_publish_path watchlist_hits=4 governed_hits=4 "
        "current_watchlist_streak=4 longest_watchlist_streak=4 "
        "current_governed_streak=4 longest_governed_streak=4 "
        "template_hits=3 offer_hits=3 publish_hits=1 oscillations=0 "
        "recent_refusals=['elite_thresholds_not_met', 'publishability_low', 'no_offer_found'] "
        "last_seen_utc=2026-04-18T12:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_near_top fixture_id=803 match_label=Watch Hold vs United "
        "behavior=stable_watchlist watchlist_hits=4 governed_hits=4 "
        "current_watchlist_streak=4 longest_watchlist_streak=4 "
        "current_governed_streak=4 longest_governed_streak=4 "
        "template_hits=0 offer_hits=0 publish_hits=0 oscillations=0 "
        "recent_refusals=['elite_thresholds_not_met', 'no_offer_found', 'publishability_low'] "
        "last_seen_utc=2026-04-18T12:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_near_top fixture_id=802 match_label=Osc Edge vs Town "
        "behavior=oscillating watchlist_hits=2 governed_hits=2 "
        "current_watchlist_streak=1 longest_watchlist_streak=1 "
        "current_governed_streak=1 longest_governed_streak=1 "
        "template_hits=0 offer_hits=0 publish_hits=0 oscillations=3 "
        "recent_refusals=['elite_thresholds_not_met', 'no_offer_found', "
        "'candidate_not_selectable', 'publishability_low', 'posterior_too_weak'] "
        "last_seen_utc=2026-04-18T12:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_episode_top kind=watchlist_plateau fixture_id=801 "
        "match_label=Near Publish FC vs City longest_watchlist_episode=4 "
        "watchlist_episodes=1 tail_episode=WATCHLISTx4 "
        "tail_episode_refusals=['elite_thresholds_not_met', 'publishability_low', 'no_offer_found'] "
        "last_seen_utc=2026-04-18T12:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_episode_top kind=alternating fixture_id=802 "
        "match_label=Osc Edge vs Town episodes_total=4 watchlist_episodes=2 "
        "no_bet_episodes=2 oscillations=3 tail_episode=WATCHLISTx1 "
        "last_seen_utc=2026-04-18T12:15:00+00:00"
    ) in completed.stdout


def test_diagnose_script_reports_streaks_and_recent_useful_steps_for_fixture() -> None:
    case_root = _case_root("recent_near_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_near_case_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "802",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture_funnel watchlist=2 governed_hits=2 no_bet=2 publish=0 "
        "do_not_publish=4 template_hits=0 offer_hits=0"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_streaks current_watchlist_streak=1 longest_watchlist_streak=1 "
        "current_governed_streak=1 longest_governed_streak=1 tail_status=WATCHLIST tail_streak=1"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_episodes episodes_total=4 watchlist_episodes=2 "
        "no_bet_episodes=2 longest_watchlist_episode=1 longest_no_bet_episode=1 "
        "tail_episode_status=WATCHLIST tail_episode_length=1"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_recent governed_steps=['2026-04-18T12:00:00+00:00:NO_BET', "
        "'2026-04-18T12:05:00+00:00:WATCHLIST', '2026-04-18T12:10:00+00:00:NO_BET', "
        "'2026-04-18T12:15:00+00:00:WATCHLIST'] "
        "refusal_steps=['2026-04-18T12:00:00+00:00:posterior_too_weak', "
        "'2026-04-18T12:05:00+00:00:elite_thresholds_not_met|publishability_low', "
        "'2026-04-18T12:10:00+00:00:candidate_not_selectable', "
        "'2026-04-18T12:15:00+00:00:elite_thresholds_not_met|no_offer_found'] "
        "recent_refusals=['elite_thresholds_not_met', 'no_offer_found', "
        "'candidate_not_selectable', 'publishability_low', 'posterior_too_weak']"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_episode_recent segments=['NO_BETx1', 'WATCHLISTx1', "
        "'NO_BETx1', 'WATCHLISTx1'] tail_episode_refusals=['elite_thresholds_not_met', "
        "'no_offer_found'] no_bet_episode_refusals=[('posterior_too_weak', 1), "
        "('candidate_not_selectable', 1)] watchlist_episode_refusals=[('elite_thresholds_not_met', 2), "
        "('publishability_low', 1), ('no_offer_found', 1)] recent_episode_refusals=[('elite_thresholds_not_met', 2), "
        "('publishability_low', 1), ('candidate_not_selectable', 1), ('no_offer_found', 1)]"
    ) in completed.stdout


def test_diagnose_script_reports_near_cases_inside_publishability_low_cohort() -> None:
    case_root = _case_root("recent_near_cohort")
    export_path = case_root / "runtime.jsonl"
    _build_near_case_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--contains-refusal",
            "publishability_low",
            "--fixture-id",
            "802",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert "cohort=contains_refusal:publishability_low" in completed.stdout
    assert (
        "vnext_recent_diag_scope cycles_read_source=4 cycles_with_matches_filtered=4 "
        "fixture_audits_source=14 fixture_audits_filtered=6 "
        "fixtures_seen_source=4 fixtures_seen_filtered=3"
    ) in completed.stdout
    assert (
        "vnext_recent_near_top fixture_id=801 match_label=Near Publish FC vs City "
        "behavior=stable_watchlist watchlist_hits=2 governed_hits=2 "
        "current_watchlist_streak=2 longest_watchlist_streak=2 "
        "current_governed_streak=2 longest_governed_streak=2 "
        "template_hits=1 offer_hits=1 publish_hits=0 oscillations=0 "
        "recent_refusals=['elite_thresholds_not_met', 'publishability_low'] "
        "last_seen_utc=2026-04-18T12:15:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture fixture_id=802 window_cycles=4 matches=1 "
        "match_label=Osc Edge vs Town first_seen_utc=2026-04-18T12:05:00+00:00 "
        "last_seen_utc=2026-04-18T12:05:00+00:00"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_streaks current_watchlist_streak=1 longest_watchlist_streak=1 "
        "current_governed_streak=1 longest_governed_streak=1 tail_status=WATCHLIST tail_streak=1"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_episodes episodes_total=1 watchlist_episodes=1 "
        "no_bet_episodes=0 longest_watchlist_episode=1 longest_no_bet_episode=0 "
        "tail_episode_status=WATCHLIST tail_episode_length=1"
    ) in completed.stdout


def test_diagnose_script_reports_compact_episode_summary_for_fixture_in_watchlist_cohort() -> None:
    case_root = _case_root("recent_episode_cohort_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_temporal_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--governed-status",
            "WATCHLIST",
            "--fixture-id",
            "501",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture_episodes episodes_total=1 watchlist_episodes=1 "
        "no_bet_episodes=0 longest_watchlist_episode=2 longest_no_bet_episode=0 "
        "tail_episode_status=WATCHLIST tail_episode_length=2"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_episode_recent segments=['WATCHLISTx2'] "
        "tail_episode_refusals=['elite_thresholds_not_met', 'publishability_low', 'no_offer_found'] "
        "no_bet_episode_refusals=[] watchlist_episode_refusals=[('elite_thresholds_not_met', 1), "
        "('publishability_low', 1), ('no_offer_found', 1)] recent_episode_refusals=[('elite_thresholds_not_met', 1), "
        "('publishability_low', 1), ('no_offer_found', 1)]"
    ) in completed.stdout


def test_diagnose_script_reports_current_vs_stale_tops() -> None:
    case_root = _case_root("recent_current_tops")
    export_path = case_root / "runtime.jsonl"
    _build_current_stale_export(export_path)

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py", "--export", str(export_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_diag_current sort_keys_current=['current_governed_streak', "
        "'longest_watchlist_episode', 'governed_hits', 'last_seen_utc'] "
        "sort_keys_historical=['longest_watchlist_episode', 'cycles_since_last_governed', "
        "'governed_hits', 'last_governed_utc'] "
        "sort_keys_recently_decayed=['cycles_since_last_governed', 'currently_seen', "
        "'longest_watchlist_episode', 'governed_hits', 'last_seen_utc']"
    ) in completed.stdout
    assert (
        "vnext_recent_current_top kind=current_blocked_plateau fixture_id=901 "
        "match_label=Active Plateau vs City currently_seen=true currently_governed=true "
        "currently_watchlist=true current_plateau_active=true current_governed_streak=4 "
        "longest_watchlist_episode=4 last_governed_utc=2026-04-18T13:15:00+00:00 "
        "cycles_since_last_governed=0"
    ) in completed.stdout
    assert (
        "vnext_recent_current_top kind=historical_blocked_plateau fixture_id=902 "
        "match_label=Historical Plateau vs United currently_seen=false currently_governed=false "
        "currently_watchlist=false current_plateau_active=false longest_watchlist_episode=2 "
        "last_governed_utc=2026-04-18T13:05:00+00:00 cycles_since_last_governed=2"
    ) in completed.stdout
    assert (
        "vnext_recent_current_top kind=recently_decayed fixture_id=903 "
        "match_label=Decayed Case vs Borough currently_seen=true currently_governed=false "
        "currently_watchlist=false current_plateau_active=false "
        "last_seen_utc=2026-04-18T13:15:00+00:00 last_governed_utc=2026-04-18T13:05:00+00:00 "
        "cycles_since_last_seen=0 cycles_since_last_governed=2"
    ) in completed.stdout


def test_diagnose_script_reports_liveness_for_active_fixture() -> None:
    case_root = _case_root("recent_current_active_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_current_stale_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "901",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture_liveness currently_seen=true currently_governed=true "
        "currently_watchlist=true last_seen_utc=2026-04-18T13:15:00+00:00 "
        "last_governed_utc=2026-04-18T13:15:00+00:00 "
        "last_watchlist_utc=2026-04-18T13:15:00+00:00 cycles_since_last_seen=0 "
        "cycles_since_last_governed=0 cycles_since_last_watchlist=0 "
        "current_plateau_active=true"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_episodes episodes_total=1 watchlist_episodes=1 "
        "no_bet_episodes=0 longest_watchlist_episode=4 longest_no_bet_episode=0 "
        "tail_episode_status=WATCHLIST tail_episode_length=4"
    ) in completed.stdout


def test_diagnose_script_reports_liveness_for_stale_fixture() -> None:
    case_root = _case_root("recent_current_stale_fixture")
    export_path = case_root / "runtime.jsonl"
    _build_current_stale_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "902",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture_liveness currently_seen=false currently_governed=false "
        "currently_watchlist=false last_seen_utc=2026-04-18T13:05:00+00:00 "
        "last_governed_utc=2026-04-18T13:05:00+00:00 "
        "last_watchlist_utc=2026-04-18T13:05:00+00:00 cycles_since_last_seen=2 "
        "cycles_since_last_governed=2 cycles_since_last_watchlist=2 "
        "current_plateau_active=false"
    ) in completed.stdout
    assert (
        "vnext_recent_fixture_episodes episodes_total=1 watchlist_episodes=1 "
        "no_bet_episodes=0 longest_watchlist_episode=2 longest_no_bet_episode=0 "
        "tail_episode_status=WATCHLIST tail_episode_length=2"
    ) in completed.stdout


def test_diagnose_script_reports_liveness_inside_publishability_low_cohort_for_active_and_stale() -> None:
    case_root = _case_root("recent_current_cohort")
    export_path = case_root / "runtime.jsonl"
    _build_current_stale_export(export_path)

    active_completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--contains-refusal",
            "publishability_low",
            "--fixture-id",
            "901",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    stale_completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--contains-refusal",
            "publishability_low",
            "--fixture-id",
            "902",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert active_completed.returncode == EXIT_SUCCESS
    assert stale_completed.returncode == EXIT_SUCCESS
    assert "cohort=contains_refusal:publishability_low" in active_completed.stdout
    assert (
        "vnext_recent_fixture_liveness currently_seen=true currently_governed=true "
        "currently_watchlist=true last_seen_utc=2026-04-18T13:15:00+00:00 "
        "last_governed_utc=2026-04-18T13:15:00+00:00 "
        "last_watchlist_utc=2026-04-18T13:15:00+00:00 cycles_since_last_seen=0 "
        "cycles_since_last_governed=0 cycles_since_last_watchlist=0 "
        "current_plateau_active=true"
    ) in active_completed.stdout
    assert (
        "vnext_recent_fixture_liveness currently_seen=false currently_governed=false "
        "currently_watchlist=false last_seen_utc=2026-04-18T13:00:00+00:00 "
        "last_governed_utc=2026-04-18T13:00:00+00:00 "
        "last_watchlist_utc=2026-04-18T13:00:00+00:00 cycles_since_last_seen=3 "
        "cycles_since_last_governed=3 cycles_since_last_watchlist=3 "
        "current_plateau_active=false"
    ) in stale_completed.stdout


def test_diagnose_script_displays_selection_and_execution_observations() -> None:
    case_root = _case_root("recent_observation_fields")
    export_path = case_root / "runtime.jsonl"
    _build_observation_export(export_path)

    selection_completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "901",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )
    execution_completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "902",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert selection_completed.returncode == EXIT_SUCCESS
    assert execution_completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture_selection_obs candidate_not_selectable_reason=all_candidates_blocked "
        "translated_candidates=2 selectable_candidates=0 best_candidate_family=- "
        "best_candidate_exists=- best_candidate_selectable=- best_candidate_blockers=[] "
        "distinct_blockers=['low_live_snapshot_quality', 'state_conflict']"
    ) in selection_completed.stdout
    assert (
        "vnext_recent_fixture_execution_obs final_execution_refusal_reason=publishability_low "
        "execution_candidates=2 execution_selectable=0 "
        "attempted_templates=['OU_FT_OVER_CORE', 'BTTS_YES_CORE'] "
        "offer_present_templates=['OU_FT_OVER_CORE'] "
        "missing_offer_templates=['BTTS_YES_CORE'] "
        "blocked_execution_reasons=['publishability_low', 'retrievability_low', 'template_bind_failed'] "
        "publishability_score=0.57 template_binding_score=1.0 bookmaker_diversity_score=0.3333 "
        "price_integrity_score=1.0 retrievability_score=0.58"
    ) in execution_completed.stdout


def test_diagnose_script_keeps_absent_observation_fields_sober() -> None:
    case_root = _case_root("recent_observation_absent")
    export_path = case_root / "runtime.jsonl"
    _build_observation_export(export_path)

    completed = subprocess.run(
        [
            sys.executable,
            "scripts/diagnose_vnext_recent.py",
            "--export",
            str(export_path),
            "--fixture-id",
            "903",
        ],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_SUCCESS
    assert (
        "vnext_recent_fixture_execution_obs final_execution_refusal_reason=no_offer_found "
        "execution_candidates=1 execution_selectable=0 attempted_templates=['OU_FT_OVER_CORE'] "
        "offer_present_templates=[] missing_offer_templates=['OU_FT_OVER_CORE'] "
        "blocked_execution_reasons=['market_unavailable', 'no_offer_found'] "
        "publishability_score=- template_binding_score=- bookmaker_diversity_score=- "
        "price_integrity_score=- retrievability_score=-"
    ) in completed.stdout


def test_diagnose_script_fails_cleanly_when_latest_run_is_missing() -> None:
    case_root = _case_root("recent_latest_missing")
    latest_path = _latest_run_path(case_root)
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py"],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_LATEST_RUN_MISSING
    assert f"vnext_recent_error reason=latest_run_missing path={latest_path}" in completed.stderr


def test_diagnose_script_fails_cleanly_when_latest_run_is_invalid() -> None:
    case_root = _case_root("recent_latest_invalid")
    latest_path = _latest_run_path(case_root)
    latest_path.write_text("{invalid-json\n", encoding="utf-8")
    env = _clean_env()
    env["VNEXT_LATEST_RUN_PATH"] = str(latest_path)

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py"],
        cwd=_repo_root(),
        env=env,
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_recent_error reason=latest_run_invalid path={latest_path}" in completed.stderr


def test_diagnose_script_fails_cleanly_when_export_is_missing() -> None:
    missing_export = _case_root("recent_export_missing") / "missing.jsonl"

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py", "--export", str(missing_export)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_recent_error reason=export_missing path={missing_export}" in completed.stderr


def test_diagnose_script_fails_cleanly_when_export_is_invalid() -> None:
    export_path = _case_root("recent_export_invalid") / "invalid.jsonl"
    export_path.write_text("{invalid-json\n", encoding="utf-8")

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py", "--export", str(export_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_INSPECT_SOURCE_FAILED
    assert f"vnext_recent_error reason=export_invalid path={export_path}" in completed.stderr


def test_diagnose_script_fails_cleanly_when_path_is_unreadable() -> None:
    unreadable_path = _case_root("recent_unreadable")

    completed = subprocess.run(
        [sys.executable, "scripts/diagnose_vnext_recent.py", "--manifest", str(unreadable_path)],
        cwd=_repo_root(),
        env=_clean_env(),
        capture_output=True,
        text=True,
        check=False,
    )

    assert completed.returncode == EXIT_PATH_UNREADABLE
    assert f"vnext_recent_error reason=path_unreadable path={unreadable_path}" in completed.stderr
