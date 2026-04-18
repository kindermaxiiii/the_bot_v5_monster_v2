from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

from app.vnext.ops.models import PublishedArtifactRecord, RuntimeCycleAuditRecord, RuntimeFixtureAuditRecord
from app.vnext.publication.models import PublicMatchPayload


def replay_runtime_export(path: Path) -> tuple[RuntimeCycleAuditRecord, ...]:
    if not path.exists():
        raise FileNotFoundError("replay_source_missing")

    records: list[RuntimeCycleAuditRecord] = []
    try:
        lines = path.read_text(encoding="utf-8").splitlines()
        for line in lines:
            text = line.strip()
            if not text:
                continue
            row = json.loads(text)
            payloads = tuple(
                PublicMatchPayload(
                    fixture_id=int(payload["fixture_id"]),
                    public_status=str(payload["public_status"]),  # type: ignore[arg-type]
                    publish_channel=str(payload["publish_channel"]),  # type: ignore[arg-type]
                    match_label=str(payload["match_label"]),
                    competition_label=str(payload["competition_label"]),
                    market_label=str(payload["market_label"]),
                    line_label=str(payload["line_label"]),
                    bookmaker_label=str(payload["bookmaker_label"]),
                    odds_label=str(payload["odds_label"]),
                    confidence_band=str(payload["confidence_band"]),  # type: ignore[arg-type]
                    public_summary=str(payload["public_summary"]),
                    source=str(payload.get("source") or "public_payload.v1"),
                )
                for payload in row.get("payloads", [])
                if isinstance(payload, dict)
            )
            fixture_audits = tuple(
                RuntimeFixtureAuditRecord(
                    fixture_id=int(audit["fixture_id"]),
                    match_label=str(audit["match_label"]),
                    competition_label=str(audit["competition_label"]),
                    governed_public_status=str(audit["governed_public_status"]),
                    publish_status=str(audit["publish_status"]),
                    template_key=str(audit["template_key"]) if audit.get("template_key") is not None else None,
                    bookmaker_id=int(audit["bookmaker_id"]) if audit.get("bookmaker_id") is not None else None,
                    line=float(audit["line"]) if audit.get("line") is not None else None,
                    odds_decimal=float(audit["odds_decimal"]) if audit.get("odds_decimal") is not None else None,
                    governance_refusal_summary=tuple(str(item) for item in audit.get("governance_refusal_summary", [])),
                    execution_refusal_summary=tuple(str(item) for item in audit.get("execution_refusal_summary", [])),
                    candidate_not_selectable_reason=str(audit["candidate_not_selectable_reason"]) if audit.get("candidate_not_selectable_reason") is not None else None,
                    translated_candidate_count=int(audit["translated_candidate_count"]) if audit.get("translated_candidate_count") is not None else None,
                    selectable_candidate_count=int(audit["selectable_candidate_count"]) if audit.get("selectable_candidate_count") is not None else None,
                    best_candidate_family=str(audit["best_candidate_family"]) if audit.get("best_candidate_family") is not None else None,
                    best_candidate_exists=bool(audit["best_candidate_exists"]) if audit.get("best_candidate_exists") is not None else None,
                    best_candidate_selectable=bool(audit["best_candidate_selectable"]) if audit.get("best_candidate_selectable") is not None else None,
                    best_candidate_blockers=tuple(str(item) for item in audit.get("best_candidate_blockers", [])),
                    distinct_candidate_blockers_summary=tuple(str(item) for item in audit.get("distinct_candidate_blockers_summary", [])),
                    execution_candidate_count=int(audit["execution_candidate_count"]) if audit.get("execution_candidate_count") is not None else None,
                    execution_selectable_count=int(audit["execution_selectable_count"]) if audit.get("execution_selectable_count") is not None else None,
                    attempted_template_keys=tuple(str(item) for item in audit.get("attempted_template_keys", [])),
                    offer_present_template_keys=tuple(str(item) for item in audit.get("offer_present_template_keys", [])),
                    missing_offer_template_keys=tuple(str(item) for item in audit.get("missing_offer_template_keys", [])),
                    blocked_execution_reasons_summary=tuple(str(item) for item in audit.get("blocked_execution_reasons_summary", [])),
                    final_execution_refusal_reason=str(audit["final_execution_refusal_reason"]) if audit.get("final_execution_refusal_reason") is not None else None,
                    publishability_score=float(audit["publishability_score"]) if audit.get("publishability_score") is not None else None,
                    template_binding_score=float(audit["template_binding_score"]) if audit.get("template_binding_score") is not None else None,
                    bookmaker_diversity_score=float(audit["bookmaker_diversity_score"]) if audit.get("bookmaker_diversity_score") is not None else None,
                    price_integrity_score=float(audit["price_integrity_score"]) if audit.get("price_integrity_score") is not None else None,
                    retrievability_score=float(audit["retrievability_score"]) if audit.get("retrievability_score") is not None else None,
                    source=str(audit.get("source") or "runtime_fixture_audit.v1"),
                )
                for audit in row.get("fixture_audits", [])
                if isinstance(audit, dict)
            )
            publication_records = tuple(
                PublishedArtifactRecord(
                    cycle_id=int(publication["cycle_id"]),
                    timestamp_utc=datetime.fromisoformat(str(publication["timestamp_utc"])),
                    fixture_id=int(publication["fixture_id"]),
                    public_status=str(publication["public_status"]),
                    publish_channel=str(publication["publish_channel"]),
                    template_key=str(publication["template_key"]) if publication.get("template_key") is not None else None,
                    bookmaker_id=int(publication["bookmaker_id"]) if publication.get("bookmaker_id") is not None else None,
                    bookmaker_name=str(publication["bookmaker_name"]) if publication.get("bookmaker_name") is not None else None,
                    line=float(publication["line"]) if publication.get("line") is not None else None,
                    odds_decimal=float(publication["odds_decimal"]) if publication.get("odds_decimal") is not None else None,
                    public_summary=str(publication["public_summary"]),
                    disposition=str(publication["disposition"]),  # type: ignore[arg-type]
                    notified=bool(publication["notified"]),
                    dedupe_origin=str(publication["dedupe_origin"]) if publication.get("dedupe_origin") is not None else None,  # type: ignore[arg-type]
                    source=str(publication.get("source") or "published_artifact.v1"),
                )
                for publication in row.get("publication_records", [])
                if isinstance(publication, dict)
            )
            records.append(
                RuntimeCycleAuditRecord(
                    cycle_id=int(row["cycle_id"]),
                    timestamp_utc=datetime.fromisoformat(str(row["timestamp_utc"])),
                    fixture_count_seen=int(row["fixture_count_seen"]),
                    pipeline_publish_count=int(row["pipeline_publish_count"]),
                    deduped_count=int(row["deduped_count"]),
                    notified_count=int(row["notified_count"]),
                    silent_count=int(row.get("silent_count", 0)),
                    unsent_shadow_count=int(row.get("unsent_shadow_count", 0)),
                    notifier_attempt_count=int(row.get("notifier_attempt_count", 0)),
                    payloads=payloads,
                    refusal_summaries=tuple(str(item) for item in row.get("refusal_summaries", [])),
                    fixture_audits=fixture_audits,
                    publication_records=publication_records,
                    ops_flags=tuple(str(item) for item in row.get("ops_flags", [])),
                    notifier_mode=str(row.get("notifier_mode") or "none"),  # type: ignore[arg-type]
                    source=str(row.get("source") or "runtime_cycle_audit.v1"),
                )
            )
    except Exception as exc:
        raise ValueError("replay_source_invalid") from exc

    return tuple(records)
