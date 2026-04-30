
from __future__ import annotations

import argparse
import hashlib
import json
import shutil
import sys
from pathlib import Path
from typing import Any

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from app.fqis.integrations.api_sports.audit_bundle import write_api_sports_audit_bundle  # noqa: E402
from app.fqis.integrations.api_sports.audit_index import (  # noqa: E402
    select_latest_audit_bundle,
    write_api_sports_audit_index,
)
from app.fqis.integrations.api_sports.operator_report import write_api_sports_operator_report  # noqa: E402
from app.fqis.integrations.api_sports.release_gate import (  # noqa: E402
    ApiSportsReleaseGateConfig,
    write_api_sports_release_gate,
)
from app.fqis.integrations.api_sports.release_manifest import write_api_sports_release_manifest  # noqa: E402
from app.fqis.integrations.api_sports.release_pack import write_api_sports_release_pack  # noqa: E402
from app.fqis.integrations.api_sports.run_ledger import build_run_ledger_entry  # noqa: E402


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="FQIS API-Sports Level 2 smoke runner.")
    parser.add_argument("--root", default=".tmp/fqis_level2_smoke", help="Smoke workspace root.")
    parser.add_argument("--run-id", default="smoke-level2-001")
    parser.add_argument("--keep-existing", action="store_true")
    parser.add_argument("--include-git", action="store_true")
    parser.add_argument("--allow-warnings", action="store_true")
    parser.add_argument("--no-require-audit-bundle", action="store_true")
    parser.add_argument("--min-ready-runs", type=int, default=1)
    parser.add_argument("--min-ready-audit-bundles", type=int, default=1)
    parser.add_argument("--require-ready", action="store_true")
    args = parser.parse_args(argv)

    try:
        root = Path(args.root)

        if root.exists() and not args.keep_existing:
            shutil.rmtree(root)

        run_dir = root / "run-001"
        bundle_dir = root / "bundles"

        normalized_path = root / "normalized.json"
        manifest_path = run_dir / "pipeline_manifest.json"
        quality_report_path = run_dir / "quality_report.json"
        ledger_path = root / "run_ledger.jsonl"
        audit_index_path = bundle_dir / "audit_bundle_index.json"
        operator_report_path = root / "operator_report.json"
        release_gate_path = root / "release_gate.json"
        release_manifest_path = root / "release_manifest.json"
        release_pack_path = root / "release_pack.json"

        run_dir.mkdir(parents=True, exist_ok=True)
        bundle_dir.mkdir(parents=True, exist_ok=True)

        normalized_payload = {
            "source": "fqis_level2_smoke",
            "fixtures": [
                {
                    "fixture_id": "smoke-fixture-001",
                    "home_team": "Smoke Home",
                    "away_team": "Smoke Away",
                    "league": "Smoke League",
                    "kickoff_utc": "2026-04-28T18:00:00+00:00",
                }
            ],
            "odds_offers": [],
        }
        _write_json(normalized_path, normalized_payload)

        payload_sha256 = _sha256(normalized_path)

        manifest_payload = {
            "run_id": args.run_id,
            "status": "COMPLETED",
            "ready": True,
            "run_dir": str(run_dir),
            "normalized_input": str(normalized_path),
            "payload_sha256": payload_sha256,
            "started_at_utc": "2026-04-28T00:00:00+00:00",
            "completed_at_utc": "2026-04-28T00:01:00+00:00",
            "steps": [
                {"name": "snapshot_quality", "status": "COMPLETED"},
                {"name": "audit_evidence", "status": "COMPLETED"},
                {"name": "release_readiness", "status": "COMPLETED"},
            ],
            "errors": [],
        }
        _write_json(manifest_path, manifest_payload)

        quality_payload = {
            "status": "PASS",
            "ready": True,
            "issues": [],
        }
        _write_json(quality_report_path, quality_payload)

        ledger_entry = build_run_ledger_entry(manifest_path)
        ledger_path.write_text(
            json.dumps(ledger_entry.to_dict(), ensure_ascii=False, sort_keys=True) + "\n",
            encoding="utf-8",
        )

        audit_bundle = write_api_sports_audit_bundle(
            manifest_path,
            output_dir=bundle_dir,
        )

        audit_index = write_api_sports_audit_index(
            bundle_dir=bundle_dir,
            output_path=audit_index_path,
        )

        latest_ready_audit_bundle = select_latest_audit_bundle(
            bundle_dir=bundle_dir,
            ready=True,
        )

        operator_report = write_api_sports_operator_report(
            ledger_path=ledger_path,
            bundle_dir=bundle_dir,
            output_path=operator_report_path,
            require_audit_bundle=not args.no_require_audit_bundle,
        )

        config = ApiSportsReleaseGateConfig(
            min_ready_runs=args.min_ready_runs,
            min_ready_audit_bundles=args.min_ready_audit_bundles,
            allow_warnings=args.allow_warnings,
            require_audit_bundle=not args.no_require_audit_bundle,
        )

        release_gate = write_api_sports_release_gate(
            ledger_path=ledger_path,
            bundle_dir=bundle_dir,
            output_path=release_gate_path,
            config=config,
        )

        release_manifest = write_api_sports_release_manifest(
            ledger_path=ledger_path,
            bundle_dir=bundle_dir,
            release_gate_path=release_gate_path,
            output_path=release_manifest_path,
            config=config,
            include_git=args.include_git,
        )

        release_pack = write_api_sports_release_pack(
            release_manifest_path=release_manifest_path,
            output_path=release_pack_path,
            config=config,
            include_git=args.include_git,
        )

        payload = {
            "status": "READY" if release_pack.release_ready else "BLOCKED",
            "release_ready": release_pack.release_ready,
            "run_id": args.run_id,
            "paths": {
                "root": str(root),
                "normalized": str(normalized_path),
                "pipeline_manifest": str(manifest_path),
                "quality_report": str(quality_report_path),
                "run_ledger": str(ledger_path),
                "audit_bundle": audit_bundle.output_path,
                "audit_index": str(audit_index_path),
                "operator_report": str(operator_report_path),
                "release_gate": str(release_gate_path),
                "release_manifest": str(release_manifest_path),
                "release_pack": str(release_pack_path),
            },
            "components": {
                "ledger_entry": ledger_entry.to_dict(),
                "audit_bundle": audit_bundle.to_dict(),
                "audit_index": audit_index.to_dict(),
                "latest_ready_audit_bundle": (
                    latest_ready_audit_bundle.to_dict()
                    if latest_ready_audit_bundle is not None
                    else None
                ),
                "operator_report": operator_report.to_dict(),
                "release_gate": release_gate.to_dict(),
                "release_manifest": release_manifest.to_dict(),
                "release_pack": release_pack.to_dict(),
            },
        }

        print(json.dumps(payload, indent=2, ensure_ascii=True, sort_keys=True))
        return 1 if args.require_ready and not release_pack.release_ready else 0

    except Exception as exc:
        print(
            json.dumps(
                {
                    "status": "FAILED",
                    "release_ready": False,
                    "reason": str(exc),
                },
                indent=2,
                ensure_ascii=True,
                sort_keys=True,
            )
        )
        return 2


def _write_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )


def _sha256(path: Path) -> str:
    return hashlib.sha256(path.read_bytes()).hexdigest()


if __name__ == "__main__":
    raise SystemExit(main())
