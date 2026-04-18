from __future__ import annotations

import json
import os
from dataclasses import asdict
from datetime import datetime
from pathlib import Path
import tempfile

from app.vnext.ops.models import (
    DedupRecord,
    PublishedArtifactRecord,
    RuntimeCycleAuditRecord,
    RuntimeFixtureAuditRecord,
)
from app.vnext.publication.models import PublicMatchPayload


def _write_jsonl_record(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True) + "\n")


def _write_json_file_with_fallback(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    text = json.dumps(payload, ensure_ascii=True)
    temp_path: Path | None = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            delete=False,
            dir=path.parent,
            suffix=".tmp",
        ) as handle:
            handle.write(text)
            temp_path = Path(handle.name)
        try:
            os.replace(temp_path, path)
            temp_path = None
            return
        except OSError:
            path.write_text(text, encoding="utf-8")
    finally:
        if temp_path is not None and temp_path.exists():
            try:
                temp_path.unlink(missing_ok=True)
            except PermissionError:
                pass


def _read_jsonl_rows(path: Path) -> list[dict[str, object]]:
    if not path.exists():
        return []
    rows: list[dict[str, object]] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            text = line.strip()
            if not text:
                continue
            rows.append(json.loads(text))
    return rows


def _serialize_public_payload(payload: PublicMatchPayload) -> dict[str, object]:
    return asdict(payload)


def _deserialize_public_payload(row: dict[str, object]) -> PublicMatchPayload:
    return PublicMatchPayload(
        fixture_id=int(row["fixture_id"]),
        public_status=str(row["public_status"]),  # type: ignore[arg-type]
        publish_channel=str(row["publish_channel"]),  # type: ignore[arg-type]
        match_label=str(row["match_label"]),
        competition_label=str(row["competition_label"]),
        market_label=str(row["market_label"]),
        line_label=str(row["line_label"]),
        bookmaker_label=str(row["bookmaker_label"]),
        odds_label=str(row["odds_label"]),
        confidence_band=str(row["confidence_band"]),  # type: ignore[arg-type]
        public_summary=str(row["public_summary"]),
        source=str(row.get("source") or "public_payload.v1"),
    )


class VnextOpsStore:
    def __init__(self, root: Path) -> None:
        self.root = root
        self.cycle_audit_path = root / "cycle_audits.jsonl"
        self.publication_record_path = root / "publication_records.jsonl"
        self.dedup_state_path = root / "dedupe_state.json"

    def probe_write_access(self) -> None:
        self.root.mkdir(parents=True, exist_ok=True)
        with tempfile.TemporaryFile(mode="w+b", dir=self.root):
            return

    def append_cycle_audit(self, record: RuntimeCycleAuditRecord) -> None:
        payload = {
            "cycle_id": record.cycle_id,
            "timestamp_utc": record.timestamp_utc.isoformat(),
            "fixture_count_seen": record.fixture_count_seen,
            "pipeline_publish_count": record.pipeline_publish_count,
            "deduped_count": record.deduped_count,
            "notified_count": record.notified_count,
            "silent_count": record.silent_count,
            "unsent_shadow_count": record.unsent_shadow_count,
            "notifier_attempt_count": record.notifier_attempt_count,
            "payloads": [_serialize_public_payload(payload) for payload in record.payloads],
            "refusal_summaries": list(record.refusal_summaries),
            "fixture_audits": [asdict(audit) for audit in record.fixture_audits],
            "publication_records": [
                {
                    "cycle_id": publication.cycle_id,
                    "timestamp_utc": publication.timestamp_utc.isoformat(),
                    "fixture_id": publication.fixture_id,
                    "public_status": publication.public_status,
                    "publish_channel": publication.publish_channel,
                    "template_key": publication.template_key,
                    "bookmaker_id": publication.bookmaker_id,
                    "bookmaker_name": publication.bookmaker_name,
                    "line": publication.line,
                    "odds_decimal": publication.odds_decimal,
                    "public_summary": publication.public_summary,
                    "disposition": publication.disposition,
                    "notified": publication.notified,
                    "dedupe_origin": publication.dedupe_origin,
                    "source": publication.source,
                }
                for publication in record.publication_records
            ],
            "ops_flags": list(record.ops_flags),
            "notifier_mode": record.notifier_mode,
            "source": record.source,
        }
        _write_jsonl_record(self.cycle_audit_path, payload)

    def list_cycle_audits(self) -> tuple[RuntimeCycleAuditRecord, ...]:
        records: list[RuntimeCycleAuditRecord] = []
        for row in _read_jsonl_rows(self.cycle_audit_path):
            payloads = tuple(
                _deserialize_public_payload(payload)
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
            records.append(
                RuntimeCycleAuditRecord(
                    cycle_id=int(row["cycle_id"]),
                    timestamp_utc=datetime.fromisoformat(str(row["timestamp_utc"])),
                    fixture_count_seen=int(row["fixture_count_seen"]),
                    pipeline_publish_count=int(row["pipeline_publish_count"]),
                    deduped_count=int(row["deduped_count"]),
                    notified_count=int(row["notified_count"]),
                    silent_count=int(row["silent_count"]),
                    unsent_shadow_count=int(row.get("unsent_shadow_count", 0)),
                    notifier_attempt_count=int(row.get("notifier_attempt_count", 0)),
                    payloads=payloads,
                    refusal_summaries=tuple(str(item) for item in row.get("refusal_summaries", [])),
                    fixture_audits=fixture_audits,
                    publication_records=tuple(
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
                    ),
                    ops_flags=tuple(str(item) for item in row.get("ops_flags", [])),
                    notifier_mode=str(row.get("notifier_mode") or "none"),  # type: ignore[arg-type]
                    source=str(row.get("source") or "runtime_cycle_audit.v1"),
                )
            )
        return tuple(records)

    def append_publication_records(
        self,
        records: tuple[PublishedArtifactRecord, ...],
    ) -> None:
        for record in records:
            payload = {
                "cycle_id": record.cycle_id,
                "timestamp_utc": record.timestamp_utc.isoformat(),
                "fixture_id": record.fixture_id,
                "public_status": record.public_status,
                "publish_channel": record.publish_channel,
                "template_key": record.template_key,
                "bookmaker_id": record.bookmaker_id,
                "bookmaker_name": record.bookmaker_name,
                "line": record.line,
                "odds_decimal": record.odds_decimal,
                "public_summary": record.public_summary,
                "disposition": record.disposition,
                "notified": record.notified,
                "dedupe_origin": record.dedupe_origin,
                "source": record.source,
            }
            _write_jsonl_record(self.publication_record_path, payload)

    def list_publication_records(self) -> tuple[PublishedArtifactRecord, ...]:
        records: list[PublishedArtifactRecord] = []
        for row in _read_jsonl_rows(self.publication_record_path):
            records.append(
                PublishedArtifactRecord(
                    cycle_id=int(row["cycle_id"]),
                    timestamp_utc=datetime.fromisoformat(str(row["timestamp_utc"])),
                    fixture_id=int(row["fixture_id"]),
                    public_status=str(row["public_status"]),
                    publish_channel=str(row["publish_channel"]),
                    template_key=str(row["template_key"]) if row.get("template_key") is not None else None,
                    bookmaker_id=int(row["bookmaker_id"]) if row.get("bookmaker_id") is not None else None,
                    bookmaker_name=str(row["bookmaker_name"]) if row.get("bookmaker_name") is not None else None,
                    line=float(row["line"]) if row.get("line") is not None else None,
                    odds_decimal=float(row["odds_decimal"]) if row.get("odds_decimal") is not None else None,
                    public_summary=str(row["public_summary"]),
                    disposition=str(row["disposition"]),  # type: ignore[arg-type]
                    notified=bool(row["notified"]),
                    dedupe_origin=str(row["dedupe_origin"]) if row.get("dedupe_origin") is not None else None,  # type: ignore[arg-type]
                    source=str(row.get("source") or "published_artifact.v1"),
                )
            )
        return tuple(records)

    def save_dedup_records(self, records: tuple[DedupRecord, ...]) -> None:
        self.dedup_state_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "records": [
                {
                    "key": record.key,
                    "last_seen_utc": record.last_seen_utc.isoformat(),
                    "source": record.source,
                }
                for record in records
            ]
        }
        _write_json_file_with_fallback(self.dedup_state_path, payload)

    def load_dedup_records(self) -> tuple[DedupRecord, ...]:
        if not self.dedup_state_path.exists():
            return ()
        payload = json.loads(self.dedup_state_path.read_text(encoding="utf-8"))
        records = []
        for row in payload.get("records", []):
            records.append(
                DedupRecord(
                    key=str(row["key"]),
                    last_seen_utc=datetime.fromisoformat(str(row["last_seen_utc"])),
                    source=str(row.get("source") or "ops_dedup_record.v1"),
                )
            )
        return tuple(records)
