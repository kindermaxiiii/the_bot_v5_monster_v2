from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.fqis.config.profiles import (
    list_shadow_production_profiles,
    load_shadow_production_profile,
    shadow_production_profile_to_record,
)
from app.fqis.orchestration.shadow_production import run_shadow_production


def test_builtin_demo_profile_loads() -> None:
    profile = load_shadow_production_profile(profile_name="demo")

    assert profile.name == "demo"
    assert str(profile.input_path).endswith("hybrid_shadow_input_valid.jsonl")
    assert str(profile.results_path).endswith("match_results_valid.jsonl")
    assert str(profile.closing_path).endswith("closing_odds_valid.jsonl")
    assert str(profile.audit_bundle_root).endswith("fqis_shadow_production_history")
    assert profile.stake == 1.0


def test_list_shadow_production_profiles() -> None:
    profiles = list_shadow_production_profiles()

    assert "demo" in profiles
    assert "dev" in profiles
    assert "live" in profiles


def test_shadow_production_profile_to_record_is_json_serializable() -> None:
    profile = load_shadow_production_profile(profile_name="demo")
    record = shadow_production_profile_to_record(profile)
    encoded = json.dumps(record, sort_keys=True)

    assert "fqis_shadow_production_profile" in encoded
    assert record["name"] == "demo"
    assert "audit_bundle_root" in record


def test_load_profile_from_file(tmp_path: Path) -> None:
    profile_path = tmp_path / "profiles.json"
    profile_path.write_text(
        json.dumps(
            {
                "profiles": {
                    "custom": {
                        "input_path": "input.jsonl",
                        "results_path": "results.jsonl",
                        "closing_path": "closing.jsonl",
                        "output_root": "runs",
                        "audit_bundle_root": "history",
                        "stake": 2.5,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    profile = load_shadow_production_profile(
        profile_name="custom",
        profile_path=profile_path,
    )

    assert profile.name == "custom"
    assert profile.input_path == Path("input.jsonl")
    assert profile.results_path == Path("results.jsonl")
    assert profile.closing_path == Path("closing.jsonl")
    assert profile.output_root == Path("runs")
    assert profile.audit_bundle_root == Path("history")
    assert profile.stake == 2.5


def test_profile_env_overrides(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("FQIS_SHADOW_OUTPUT_ROOT", "data/custom_runs")
    monkeypatch.setenv("FQIS_SHADOW_AUDIT_BUNDLE_ROOT", "data/custom_history")
    monkeypatch.setenv("FQIS_SHADOW_STAKE", "3.0")

    profile = load_shadow_production_profile(profile_name="demo")

    assert profile.output_root == Path("data/custom_runs")
    assert profile.audit_bundle_root == Path("data/custom_history")
    assert profile.stake == 3.0


def test_unknown_builtin_profile_raises() -> None:
    with pytest.raises(ValueError):
        load_shadow_production_profile(profile_name="missing")


def test_profile_driven_shadow_production_uses_shared_history(tmp_path: Path) -> None:
    profile_path = tmp_path / "profiles.json"
    profile_path.write_text(
        json.dumps(
            {
                "profiles": {
                    "test": {
                        "input_path": "tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl",
                        "results_path": "tests/fixtures/fqis/match_results_valid.jsonl",
                        "closing_path": "tests/fixtures/fqis/closing_odds_valid.jsonl",
                        "output_root": str(tmp_path / "runs"),
                        "audit_bundle_root": str(tmp_path / "history"),
                        "stake": 1.0,
                    }
                }
            }
        ),
        encoding="utf-8",
    )

    profile = load_shadow_production_profile(
        profile_name="test",
        profile_path=profile_path,
    )

    first = run_shadow_production(profile.to_config(run_id="profile-run-a"))
    second = run_shadow_production(profile.to_config(run_id="profile-run-b"))

    assert first.readiness.run_count == 1
    assert first.readiness_level == "BLOCKED"

    assert second.readiness.run_count == 2
    assert second.readiness_level == "REVIEW_REQUIRED"
    assert second.readiness_status == "NO_GO"
    assert Path(second.bundle_dir).exists()