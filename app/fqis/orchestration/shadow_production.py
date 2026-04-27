from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Any

from app.fqis.reporting.audit_bundle import (
    AuditBundleManifest,
    audit_bundle_manifest_to_record,
    build_audit_bundle,
)
from app.fqis.reporting.production_readiness import (
    ProductionReadinessReport,
    evaluate_production_readiness_from_bundle_root,
    production_readiness_report_to_record,
    write_production_readiness_report_json,
)
from app.fqis.runtime.hybrid_batch_shadow import (
    HybridShadowBatchOutcome,
    run_hybrid_shadow_batch_from_jsonl,
    write_hybrid_shadow_batch_jsonl,
)
from app.fqis.settlement.ledger import (
    SettlementReport,
    settle_hybrid_shadow_batch_from_jsonl,
    write_settlement_report_json,
)


@dataclass(slots=True, frozen=True)
class ShadowProductionConfig:
    input_path: Path
    results_path: Path
    closing_path: Path
    output_root: Path
    audit_bundle_root: Path | None = None
    run_id: str | None = None
    stake: float = 1.0


@dataclass(slots=True, frozen=True)
class ShadowProductionOutcome:
    status: str
    run_id: str
    generated_at_utc: str
    output_dir: str
    hybrid_batch_path: str
    settlement_path: str
    bundle_dir: str
    readiness_path: str
    hybrid_batch: HybridShadowBatchOutcome
    settlement: SettlementReport
    audit_bundle: AuditBundleManifest
    readiness: ProductionReadinessReport

    @property
    def readiness_status(self) -> str:
        return self.readiness.readiness_status

    @property
    def readiness_level(self) -> str:
        return self.readiness.readiness_level

    @property
    def is_go(self) -> bool:
        return self.readiness.is_go


def run_shadow_production(config: ShadowProductionConfig) -> ShadowProductionOutcome:
    run_id = config.run_id or _default_run_id()
    output_dir = config.output_root / run_id
    output_dir.mkdir(parents=True, exist_ok=True)

    hybrid_batch_path = output_dir / "hybrid_shadow_batch.jsonl"
    settlement_path = output_dir / "settlement_report.json"
    bundle_root = config.audit_bundle_root or output_dir / "audit_bundles"
    readiness_path = output_dir / "production_readiness_report.json"

    hybrid_batch = run_hybrid_shadow_batch_from_jsonl(config.input_path)
    write_hybrid_shadow_batch_jsonl(hybrid_batch, hybrid_batch_path)

    settlement = settle_hybrid_shadow_batch_from_jsonl(
        batch_path=hybrid_batch_path,
        results_path=config.results_path,
        stake=config.stake,
    )
    write_settlement_report_json(settlement, settlement_path)

    audit_bundle = build_audit_bundle(
        hybrid_batch_path=hybrid_batch_path,
        settlement_path=settlement_path,
        closing_path=config.closing_path,
        output_dir=bundle_root,
        run_id=run_id,
    )

    readiness = evaluate_production_readiness_from_bundle_root(bundle_root)
    write_production_readiness_report_json(readiness, readiness_path)

    return ShadowProductionOutcome(
        status="ok",
        run_id=run_id,
        generated_at_utc=datetime.now(UTC).isoformat(),
        output_dir=str(output_dir),
        hybrid_batch_path=str(hybrid_batch_path),
        settlement_path=str(settlement_path),
        bundle_dir=audit_bundle.bundle_dir,
        readiness_path=str(readiness_path),
        hybrid_batch=hybrid_batch,
        settlement=settlement,
        audit_bundle=audit_bundle,
        readiness=readiness,
    )


def shadow_production_outcome_to_record(outcome: ShadowProductionOutcome) -> dict[str, Any]:
    return {
        "status": outcome.status,
        "source": "fqis_shadow_production_outcome",
        "run_id": outcome.run_id,
        "generated_at_utc": outcome.generated_at_utc,
        "output_dir": outcome.output_dir,
        "paths": {
            "hybrid_batch_path": outcome.hybrid_batch_path,
            "settlement_path": outcome.settlement_path,
            "bundle_dir": outcome.bundle_dir,
            "readiness_path": outcome.readiness_path,
        },
        "headline": {
            "matches": outcome.hybrid_batch.match_count,
            "accepted_matches": outcome.hybrid_batch.accepted_match_count,
            "rejected_matches": outcome.hybrid_batch.rejected_match_count,
            "accepted_bets": outcome.hybrid_batch.accepted_bet_count,
            "settled_bets": outcome.settlement.settled_bet_count,
            "unsettled_bets": outcome.settlement.unsettled_bet_count,
            "won": outcome.settlement.won_count,
            "lost": outcome.settlement.lost_count,
            "push": outcome.settlement.push_count,
            "roi": outcome.settlement.roi,
            "readiness_status": outcome.readiness_status,
            "readiness_level": outcome.readiness_level,
            "is_go": outcome.is_go,
            "blockers": outcome.readiness.blocker_count,
            "warnings": outcome.readiness.warning_count,
            "failures": outcome.readiness.failure_count,
            "bundle_files": outcome.audit_bundle.file_count,
            "bundle_health": outcome.audit_bundle.health_status,
        },
        "audit_bundle_manifest": audit_bundle_manifest_to_record(outcome.audit_bundle),
        "readiness_report": production_readiness_report_to_record(outcome.readiness),
    }


def write_shadow_production_outcome_json(
    outcome: ShadowProductionOutcome,
    path: Path,
) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)

    path.write_text(
        json.dumps(
            shadow_production_outcome_to_record(outcome),
            ensure_ascii=False,
            indent=2,
            sort_keys=True,
        )
        + "\n",
        encoding="utf-8",
    )

    return path


def _default_run_id() -> str:
    return datetime.now(UTC).strftime("fqis_shadow_production_%Y%m%d_%H%M%S")

    