import json
import py_compile
import subprocess
import sys
import hashlib
from pathlib import Path

from scripts import fqis_operator_shadow_console as console
from scripts import fqis_paper_alert_dedupe as dedupe
from scripts import fqis_paper_alert_ranker as ranker
from scripts import fqis_shadow_session_quality_report as quality


ROOT = Path(__file__).resolve().parents[3]
QUALITY_SCRIPT = ROOT / "scripts" / "fqis_shadow_session_quality_report.py"
LEDGER = ROOT / "data" / "pipeline" / "api_sports" / "research_ledger" / "research_candidates_ledger.csv"
QUALITY_OUT_JSON = ROOT / "data" / "pipeline" / "api_sports" / "orchestrator" / "latest_shadow_session_quality_report.json"


def signal(*, odds: float, ev: float = 0.10, edge: float = 0.05) -> dict:
    return {
        "fixture_id": "fixture-1",
        "match": "Home vs Away",
        "minute": 63,
        "score": "1-1",
        "market": "Total Goals FT",
        "selection": "Over 2.5",
        "odds": odds,
        "entry_odds": odds,
        "edge_prob": edge,
        "ev_real": ev,
        "final_pipeline": "research",
        "research_bucket": "EVENTS_ONLY_OVER_2_5_RESEARCH",
        "research_data_tier": "EVENTS_ONLY_RESEARCH",
        "bucket_policy_action": "KEEP_RESEARCH_BUCKET",
        "paper_action": "PAPER_RESEARCH_WATCH",
    }


def write_json(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")


def sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


def patch_dedupe_paths(monkeypatch, tmp_path: Path) -> Path:
    export = tmp_path / "latest_paper_signal_export.json"
    monkeypatch.setattr(dedupe, "PAPER_SIGNAL_EXPORT_JSON", export)
    monkeypatch.setattr(dedupe, "OUT_JSON", tmp_path / "latest_paper_alert_dedupe.json")
    monkeypatch.setattr(dedupe, "OUT_MD", tmp_path / "latest_paper_alert_dedupe.md")
    monkeypatch.setattr(dedupe, "STATE_JSON", tmp_path / "paper_alert_state.json")
    return export


def run_dedupe_with(export: Path, signals: list[dict]) -> dict:
    write_json(export, {"status": "READY", "signals": signals})
    payload, next_state = dedupe.build_payload()
    dedupe.write_outputs(payload, next_state)
    return payload


def test_canonical_dedupe_suppresses_odds_only_duplicate_alerts(monkeypatch, tmp_path):
    export = patch_dedupe_paths(monkeypatch, tmp_path)

    first = run_dedupe_with(export, [signal(odds=2.00)])
    second = run_dedupe_with(export, [signal(odds=2.01)])

    assert first["new_canonical_alerts"] == 1
    assert second["raw_new_alerts"] == 1
    assert second["new_canonical_alerts"] == 0
    assert second["updated_canonical_alerts"] == 0
    assert second["repeated_canonical_alerts"] == 1
    assert second["suppressed_exact_repeats"] == 1
    assert second["repeated_alert_records"][0]["alert_lifecycle_status"] == "REPEATED_CANONICAL"
    assert second["repeated_alert_records"][0]["canonical_alert_key"]
    assert second["can_execute_real_bets"] is False
    assert second["can_enable_live_staking"] is False
    assert second["can_mutate_ledger"] is False
    assert second["live_staking_allowed"] is False
    assert second["promotion_allowed"] is False


def test_material_odds_update_is_tracked_as_update_not_new_canonical(monkeypatch, tmp_path):
    export = patch_dedupe_paths(monkeypatch, tmp_path)

    run_dedupe_with(export, [signal(odds=2.00)])
    second = run_dedupe_with(export, [signal(odds=2.06)])

    assert second["raw_new_alerts"] == 1
    assert second["new_canonical_alerts"] == 0
    assert second["updated_canonical_alerts"] == 1
    assert second["material_updates"] == 1
    record = second["updated_alert_records"][0]
    assert record["alert_lifecycle_status"] == "UPDATED_CANONICAL"
    assert "MATERIAL_ODDS_CHANGE" in record["material_update_reasons"]
    assert record["odds_first"] == 2.0
    assert record["odds_latest"] == 2.06
    assert record["odds_max"] == 2.06


def test_grouped_ranker_keeps_raw_alerts_and_reports_grouped_count(monkeypatch, tmp_path):
    export = tmp_path / "latest_paper_signal_export.json"
    dedupe_json = tmp_path / "latest_paper_alert_dedupe.json"
    go_no_go = tmp_path / "latest_go_no_go_report.json"
    shadow = tmp_path / "latest_shadow_readiness_report.json"
    freshness = tmp_path / "latest_live_freshness_report.json"

    monkeypatch.setattr(ranker, "PAPER_SIGNAL_EXPORT_JSON", export)
    monkeypatch.setattr(ranker, "PAPER_ALERT_DEDUPE_JSON", dedupe_json)
    monkeypatch.setattr(ranker, "GO_NO_GO_JSON", go_no_go)
    monkeypatch.setattr(ranker, "SHADOW_READINESS_JSON", shadow)
    monkeypatch.setattr(ranker, "LIVE_FRESHNESS_JSON", freshness)
    monkeypatch.setattr(ranker, "OUT_JSON", tmp_path / "latest_paper_alert_ranker.json")
    monkeypatch.setattr(ranker, "OUT_MD", tmp_path / "latest_paper_alert_ranker.md")

    signals = [signal(odds=2.00, ev=0.12), signal(odds=2.01, ev=0.11)]
    first_record = {
        "alert_key": ranker.stable_alert_key(signals[0]),
        "canonical_alert_key": ranker.canonical_alert_key(signals[0]),
        "alert_lifecycle_status": "NEW_CANONICAL",
        "discord_sendable": True,
    }
    second_record = {
        "alert_key": ranker.stable_alert_key(signals[1]),
        "canonical_alert_key": ranker.canonical_alert_key(signals[1]),
        "alert_lifecycle_status": "REPEATED_CANONICAL",
    }

    write_json(export, {"status": "READY", "signals": signals})
    write_json(dedupe_json, {"status": "READY", "new_alert_records": [first_record], "repeated_alert_records": [second_record]})
    write_json(go_no_go, {"status": "READY", "go_no_go_state": "NO_GO_DRY_RUN_ONLY"})
    write_json(shadow, {"status": "READY", "shadow_state": "SHADOW_READY"})
    write_json(freshness, {"status": "READY", "freshness_flags": ["OK_FRESH_LIVE_CYCLE"]})

    payload = ranker.build_payload()

    assert payload["status"] == "READY"
    assert payload["raw_ranked_alert_count"] == 2
    assert payload["ranked_alert_count"] == 2
    assert len(payload["raw_ranked_alerts"]) == 2
    assert payload["grouped_ranked_alert_count"] == 1
    assert len(payload["grouped_ranked_alerts"]) == 1
    assert payload["grouped_ranked_alerts"][0]["raw_rank"] == 1
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False


def test_operator_console_monitor_section_marks_partial_context():
    monitor = {
        "status": "READY",
        "generated_at_utc": "2026-04-30T10:00:00+00:00",
        "cycles_completed": 34,
        "cycles_requested": 35,
        "summary": {"all_ledger_preserved": True},
    }
    full_cycle = {"generated_at_utc": "2026-04-30T10:01:00+00:00"}

    section = console.monitor_section(monitor, full_cycle, "2026-04-30T10:01:30+00:00")

    assert section["monitor_context"] == "PARTIAL_MONITOR_CONTEXT"
    assert section["cycles_completed"] == 34
    assert section["monitor_artifact_generated_at_utc"] == "2026-04-30T10:00:00+00:00"
    assert section["operator_console_generated_at_utc"] == "2026-04-30T10:01:30+00:00"


def patch_quality_paths(monkeypatch, tmp_path: Path) -> dict[str, Path]:
    paths = {
        "monitor": tmp_path / "latest_tonight_shadow_monitor.json",
        "digest": tmp_path / "latest_tonight_shadow_digest.json",
        "operator": tmp_path / "latest_operator_shadow_console.json",
        "ranker": tmp_path / "latest_paper_alert_ranker.json",
        "dedupe": tmp_path / "latest_paper_alert_dedupe.json",
        "freshness": tmp_path / "latest_live_freshness_report.json",
        "full_cycle": tmp_path / "latest_full_cycle_report.json",
        "out_json": tmp_path / "latest_shadow_session_quality_report.json",
        "out_md": tmp_path / "latest_shadow_session_quality_report.md",
    }
    monkeypatch.setattr(quality, "MONITOR_JSON", paths["monitor"])
    monkeypatch.setattr(quality, "DIGEST_JSON", paths["digest"])
    monkeypatch.setattr(quality, "OPERATOR_CONSOLE_JSON", paths["operator"])
    monkeypatch.setattr(quality, "PAPER_ALERT_RANKER_JSON", paths["ranker"])
    monkeypatch.setattr(quality, "PAPER_ALERT_DEDUPE_JSON", paths["dedupe"])
    monkeypatch.setattr(quality, "LIVE_FRESHNESS_JSON", paths["freshness"])
    monkeypatch.setattr(quality, "FULL_CYCLE_JSON", paths["full_cycle"])
    monkeypatch.setattr(quality, "OUT_JSON", paths["out_json"])
    monkeypatch.setattr(quality, "OUT_MD", paths["out_md"])
    return paths


def write_quality_inputs(
    paths: dict[str, Path],
    *,
    monitor_status: str = "READY",
    decisions_total: int = 7,
    unsafe: bool = False,
    freshness_flags: list[str] | None = None,
    historical_static_review: list[str] | None = None,
    raw_new_paper_alerts: int = 2,
    canonical_new_alerts: int = 1,
    material_updates: int = 0,
) -> None:
    final_freshness_flags = freshness_flags or ["OK_FRESH_LIVE_CYCLE"]
    final_historical_static_review = (
        ["CONSTANT_POST_QUARANTINE_PNL_REVIEW"]
        if historical_static_review is None
        else historical_static_review
    )
    row = {
        "cycle": 1,
        "full_cycle_status": "READY",
        "shadow_state": "SHADOW_READY",
        "operator_state": "PAPER_READY",
        "live_freshness_status": "READY",
        "decisions_total": decisions_total,
        "candidates_this_cycle": 3,
        "ranked_alert_count": 2,
        "raw_ranked_alert_count": 2,
        "grouped_ranked_alert_count": 1,
        "new_snapshots_appended": 3,
        "new_paper_alerts": 1,
        "raw_new_paper_alerts": raw_new_paper_alerts,
        "new_canonical_alerts": canonical_new_alerts,
        "material_updates": material_updates,
        "ledger_preserved": not unsafe,
        "can_execute_real_bets": False,
        "can_enable_live_staking": False,
        "live_staking_allowed": False,
        "promotion_allowed": False,
    }
    write_json(paths["monitor"], {
        "status": monitor_status,
        "cycles_completed": 1,
        "cycles_requested": 1,
        "summary": {
            "all_ledger_preserved": not unsafe,
            "any_real_bets_enabled": False,
            "any_live_staking_enabled": False,
            "any_promotion_allowed": False,
            "total_new_snapshots_appended": 3,
        },
        "rows": [row],
    })
    write_json(paths["digest"], {
        "final_full_cycle_status": "READY",
        "ledger_preserved_final": not unsafe,
        "freshness_flags_final": final_freshness_flags,
        "historical_static_review_final": final_historical_static_review,
        "any_real_bets_enabled": False,
        "any_live_staking_enabled": False,
        "any_promotion_allowed": False,
    })
    write_json(paths["operator"], {
        "can_execute_real_bets": False,
        "can_enable_live_staking": False,
        "can_mutate_ledger": False,
        "live_staking_allowed": False,
        "promotion_allowed": False,
    })
    write_json(paths["ranker"], {"status": "READY"})
    write_json(paths["dedupe"], {
        "status": "READY",
        "raw_new_alerts": raw_new_paper_alerts,
        "new_canonical_alerts": canonical_new_alerts,
        "material_updates": material_updates,
    })
    write_json(paths["freshness"], {
        "status": "READY",
        "freshness_flags": final_freshness_flags,
        "historical_metric_static_review": final_historical_static_review,
    })
    write_json(paths["full_cycle"], {"status": "READY"})


def test_session_quality_report_emits_quality_states(monkeypatch, tmp_path):
    py_compile.compile(str(QUALITY_SCRIPT), doraise=True)
    paths = patch_quality_paths(monkeypatch, tmp_path)

    payload = quality.build_payload()
    assert payload["quality_state"] == "NO_MONITOR_SESSION_AVAILABLE"

    write_quality_inputs(paths)
    payload = quality.build_payload()
    assert payload["quality_state"] == "SESSION_GREEN"
    assert payload["recommended_next_action"] == "CONTINUE_PAPER_SHADOW_MONITORING"
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False

    write_quality_inputs(paths, decisions_total=0)
    payload = quality.build_payload()
    assert payload["quality_state"] == "SESSION_REVIEW"
    assert payload["zero_decision_cycles"] == 1
    assert payload["recommended_next_action"] == "REVIEW_ZERO_DECISION_CYCLES"

    write_quality_inputs(paths, monitor_status="STOPPED")
    payload = quality.build_payload()
    assert payload["quality_state"] == "SESSION_BLOCKED"
    assert payload["recommended_next_action"] == "STOP_SESSION_AND_INSPECT_SAFETY"


def test_session_quality_recommended_next_action_branches(monkeypatch, tmp_path):
    paths = patch_quality_paths(monkeypatch, tmp_path)

    write_quality_inputs(paths, decisions_total=0)
    zero_only = quality.build_payload()
    assert zero_only["quality_state"] == "SESSION_REVIEW"
    assert zero_only["recommended_next_action"] == "REVIEW_ZERO_DECISION_CYCLES"

    write_quality_inputs(
        paths,
        freshness_flags=["STALE_LIVE_DECISIONS_REVIEW"],
        historical_static_review=[],
    )
    freshness_only = quality.build_payload()
    assert freshness_only["quality_state"] == "SESSION_REVIEW"
    assert freshness_only["live_review_flags_final"] == ["STALE_LIVE_DECISIONS_REVIEW"]
    assert freshness_only["recommended_next_action"] == "REVIEW_FRESHNESS_FLAGS"

    write_quality_inputs(
        paths,
        decisions_total=0,
        freshness_flags=["STALE_LIVE_DECISIONS_REVIEW"],
        historical_static_review=[],
    )
    combined = quality.build_payload()
    assert combined["quality_state"] == "SESSION_REVIEW"
    assert combined["zero_decision_cycles"] == 1
    assert combined["live_review_flags_final"] == ["STALE_LIVE_DECISIONS_REVIEW"]
    assert combined["recommended_next_action"] == "REVIEW_ZERO_DECISIONS_AND_FRESHNESS"

    for payload in [zero_only, freshness_only, combined]:
        assert payload["can_execute_real_bets"] is False
        assert payload["can_enable_live_staking"] is False
        assert payload["can_mutate_ledger"] is False
        assert payload["live_staking_allowed"] is False
        assert payload["promotion_allowed"] is False
        safety = payload["safety_flags"]
        assert safety["any_real_bets_enabled"] is False
        assert safety["any_live_staking_enabled"] is False
        assert safety["any_promotion_allowed"] is False


def test_session_quality_material_updates_count_toward_sendable_ratio(monkeypatch, tmp_path):
    paths = patch_quality_paths(monkeypatch, tmp_path)
    write_quality_inputs(
        paths,
        raw_new_paper_alerts=12,
        canonical_new_alerts=2,
        material_updates=4,
    )

    payload = quality.build_payload()

    assert payload["quality_state"] == "SESSION_GREEN"
    assert payload["total_sendable_canonical_events"] == 6
    assert payload["raw_to_canonical_new_ratio"] == 6.0
    assert payload["raw_to_sendable_canonical_ratio"] == 2.0
    assert payload["alert_noise_ratio"] == payload["raw_to_canonical_new_ratio"]
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False


def test_session_quality_report_script_runs_and_preserves_ledger():
    before = sha256(LEDGER)
    py_compile.compile(str(QUALITY_SCRIPT), doraise=True)

    subprocess.run(
        [sys.executable, str(QUALITY_SCRIPT)],
        cwd=str(ROOT),
        check=True,
        capture_output=True,
        text=True,
        encoding="utf-8",
        errors="replace",
    )

    assert sha256(LEDGER) == before
    payload = json.loads(QUALITY_OUT_JSON.read_text(encoding="utf-8"))
    assert payload["quality_state"] in {
        "SESSION_GREEN",
        "SESSION_REVIEW",
        "SESSION_BLOCKED",
        "NO_MONITOR_SESSION_AVAILABLE",
    }
    assert payload["can_execute_real_bets"] is False
    assert payload["can_enable_live_staking"] is False
    assert payload["can_mutate_ledger"] is False
    assert payload["live_staking_allowed"] is False
    assert payload["promotion_allowed"] is False
