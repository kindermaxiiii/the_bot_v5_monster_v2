from __future__ import annotations

import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from app.fqis.orchestration.shadow_production import ShadowProductionConfig


@dataclass(slots=True, frozen=True)
class ShadowProductionProfile:
    name: str
    input_path: Path
    results_path: Path
    closing_path: Path
    output_root: Path
    audit_bundle_root: Path
    stake: float = 1.0

    def to_config(self, *, run_id: str | None = None) -> ShadowProductionConfig:
        return ShadowProductionConfig(
            input_path=self.input_path,
            results_path=self.results_path,
            closing_path=self.closing_path,
            output_root=self.output_root,
            audit_bundle_root=self.audit_bundle_root,
            run_id=run_id,
            stake=self.stake,
        )


BUILTIN_SHADOW_PRODUCTION_PROFILES: dict[str, dict[str, Any]] = {
    "demo": {
        "input_path": "tests/fixtures/fqis/hybrid_shadow_input_valid.jsonl",
        "results_path": "tests/fixtures/fqis/match_results_valid.jsonl",
        "closing_path": "tests/fixtures/fqis/closing_odds_valid.jsonl",
        "output_root": "data/fqis_shadow_production_runs",
        "audit_bundle_root": "data/fqis_shadow_production_history",
        "stake": 1.0,
    },
    "dev": {
        "input_path": "data/fqis/dev/input.jsonl",
        "results_path": "data/fqis/dev/results.jsonl",
        "closing_path": "data/fqis/dev/closing_odds.jsonl",
        "output_root": "data/fqis/dev/shadow_runs",
        "audit_bundle_root": "data/fqis/dev/audit_history",
        "stake": 1.0,
    },
    "live": {
        "input_path": "data/fqis/live/input.jsonl",
        "results_path": "data/fqis/live/results.jsonl",
        "closing_path": "data/fqis/live/closing_odds.jsonl",
        "output_root": "data/fqis/live/shadow_runs",
        "audit_bundle_root": "data/fqis/live/audit_history",
        "stake": 1.0,
    },
}


def load_shadow_production_profile(
    *,
    profile_name: str,
    profile_path: Path | None = None,
    env_prefix: str = "FQIS_SHADOW",
) -> ShadowProductionProfile:
    raw = _load_profile_record(
        profile_name=profile_name,
        profile_path=profile_path,
    )

    raw = _apply_env_overrides(raw, env_prefix=env_prefix)

    return ShadowProductionProfile(
        name=profile_name,
        input_path=Path(str(raw["input_path"])),
        results_path=Path(str(raw["results_path"])),
        closing_path=Path(str(raw["closing_path"])),
        output_root=Path(str(raw["output_root"])),
        audit_bundle_root=Path(str(raw["audit_bundle_root"])),
        stake=float(raw.get("stake", 1.0)),
    )


def list_shadow_production_profiles() -> tuple[str, ...]:
    return tuple(sorted(BUILTIN_SHADOW_PRODUCTION_PROFILES))


def shadow_production_profile_to_record(profile: ShadowProductionProfile) -> dict[str, Any]:
    return {
        "source": "fqis_shadow_production_profile",
        "name": profile.name,
        "input_path": str(profile.input_path),
        "results_path": str(profile.results_path),
        "closing_path": str(profile.closing_path),
        "output_root": str(profile.output_root),
        "audit_bundle_root": str(profile.audit_bundle_root),
        "stake": profile.stake,
    }


def _load_profile_record(
    *,
    profile_name: str,
    profile_path: Path | None,
) -> dict[str, Any]:
    if profile_path is None:
        if profile_name not in BUILTIN_SHADOW_PRODUCTION_PROFILES:
            raise ValueError(f"unknown shadow production profile: {profile_name}")

        return dict(BUILTIN_SHADOW_PRODUCTION_PROFILES[profile_name])

    if not profile_path.exists():
        raise FileNotFoundError(f"profile file not found: {profile_path}")

    payload = json.loads(profile_path.read_text(encoding="utf-8-sig"))

    if not isinstance(payload, dict):
        raise ValueError("profile file must contain a JSON object")

    profiles = payload.get("profiles", payload)

    if not isinstance(profiles, dict):
        raise ValueError("profile file must contain a profiles object or profile object")

    if profile_name in profiles and isinstance(profiles[profile_name], dict):
        return dict(profiles[profile_name])

    required_keys = {
        "input_path",
        "results_path",
        "closing_path",
        "output_root",
        "audit_bundle_root",
    }

    if required_keys.issubset(profiles):
        return dict(profiles)

    raise ValueError(f"profile not found in file: {profile_name}")


def _apply_env_overrides(
    record: dict[str, Any],
    *,
    env_prefix: str,
) -> dict[str, Any]:
    updated = dict(record)

    env_mapping = {
        "input_path": f"{env_prefix}_INPUT_PATH",
        "results_path": f"{env_prefix}_RESULTS_PATH",
        "closing_path": f"{env_prefix}_CLOSING_PATH",
        "output_root": f"{env_prefix}_OUTPUT_ROOT",
        "audit_bundle_root": f"{env_prefix}_AUDIT_BUNDLE_ROOT",
        "stake": f"{env_prefix}_STAKE",
    }

    for key, env_name in env_mapping.items():
        value = os.getenv(env_name)
        if value not in (None, ""):
            updated[key] = value

    return updated