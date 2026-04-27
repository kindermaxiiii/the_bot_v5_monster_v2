import json
from dataclasses import dataclass
from pathlib import Path

import pytest

from app.fqis.integrations.api_sports.snapshots import (
    ApiSportsSnapshotCollector,
    ApiSportsSnapshotKind,
    ApiSportsSnapshotSecurityError,
    ApiSportsSnapshotWriter,
)


@dataclass
class FakePaging:
    current: int = 1
    total: int = 1


@dataclass
class FakeResponse:
    endpoint: str
    results: int
    response: object
    raw: dict
    paging: FakePaging


class FakeClient:
    def __init__(self):
        self.calls = []

    def fixtures_by_date(self, date, timezone="Europe/Paris"):
        self.calls.append(("fixtures_by_date", date, timezone))
        return FakeResponse(
            endpoint="fixtures",
            results=1,
            response=[{"fixture": {"id": 1001}}],
            paging=FakePaging(current=1, total=1),
            raw={
                "get": "fixtures",
                "parameters": {"date": date, "timezone": timezone},
                "errors": [],
                "results": 1,
                "paging": {"current": 1, "total": 1},
                "response": [{"fixture": {"id": 1001}}],
            },
        )

    def odds_by_date(self, date, timezone="Europe/Paris", page=1):
        self.calls.append(("odds_by_date", date, timezone, page))
        return FakeResponse(
            endpoint="odds",
            results=1,
            response=[{"fixture": {"id": 1001}, "bookmakers": []}],
            paging=FakePaging(current=page, total=2),
            raw={
                "get": "odds",
                "parameters": {"date": date, "timezone": timezone, "page": page},
                "errors": [],
                "results": 1,
                "paging": {"current": page, "total": 2},
                "response": [{"fixture": {"id": 1001}, "bookmakers": []}],
            },
        )

    def live_fixtures(self):
        self.calls.append(("live_fixtures",))
        return FakeResponse(
            endpoint="fixtures",
            results=0,
            response=[],
            paging=FakePaging(current=1, total=1),
            raw={"get": "fixtures", "parameters": {"live": "all"}, "errors": [], "results": 0, "response": []},
        )

    def live_odds(self):
        self.calls.append(("live_odds",))
        return FakeResponse(
            endpoint="odds/live",
            results=0,
            response=[],
            paging=FakePaging(current=1, total=1),
            raw={"get": "odds/live", "parameters": {}, "errors": [], "results": 0, "response": []},
        )


def test_snapshot_writer_writes_raw_envelope_without_secret(tmp_path):
    writer = ApiSportsSnapshotWriter(tmp_path)

    record = writer.write(
        kind=ApiSportsSnapshotKind.FIXTURES_BY_DATE,
        endpoint="fixtures",
        params={"date": "2026-04-27", "timezone": "Europe/Paris"},
        raw_payload={"get": "fixtures", "errors": [], "results": 0, "response": []},
        run_id="run-test",
        captured_at_utc="2026-04-27T12:00:00Z",
    )

    assert record.path.exists()
    envelope = json.loads(record.path.read_text(encoding="utf-8"))
    assert envelope["mode"] == "shadow_only_raw_snapshot"
    assert envelope["secret_policy"]["api_key"] == "***REDACTED***"
    assert "SECRET" not in record.path.read_text(encoding="utf-8")


def test_snapshot_writer_rejects_secret_like_keys(tmp_path):
    writer = ApiSportsSnapshotWriter(tmp_path)

    with pytest.raises(ApiSportsSnapshotSecurityError):
        writer.write(
            kind=ApiSportsSnapshotKind.FIXTURES_BY_DATE,
            endpoint="fixtures",
            params={},
            raw_payload={"headers": {"x-apisports-key": "SECRET"}},
            run_id="run-test",
        )


def test_snapshot_collector_collects_fixtures_odds_and_live(tmp_path):
    client = FakeClient()
    writer = ApiSportsSnapshotWriter(tmp_path)
    collector = ApiSportsSnapshotCollector(client=client, writer=writer)

    manifest = collector.collect_date(
        date="2026-04-27",
        timezone="Europe/Paris",
        include_odds=True,
        include_live=True,
        max_odds_pages=2,
        run_id="run-test",
    )

    assert manifest.status == "COMPLETED"
    assert manifest.snapshot_count == 5
    assert ("fixtures_by_date", "2026-04-27", "Europe/Paris") in client.calls
    assert ("odds_by_date", "2026-04-27", "Europe/Paris", 1) in client.calls
    assert ("odds_by_date", "2026-04-27", "Europe/Paris", 2) in client.calls
    assert ("live_fixtures",) in client.calls
    assert ("live_odds",) in client.calls
    assert all(snapshot.path.exists() for snapshot in manifest.snapshots)


def test_snapshot_collector_caps_odds_pages(tmp_path):
    client = FakeClient()
    writer = ApiSportsSnapshotWriter(tmp_path)
    collector = ApiSportsSnapshotCollector(client=client, writer=writer)

    manifest = collector.collect_date(
        date="2026-04-27",
        include_odds=True,
        include_live=False,
        max_odds_pages=1,
        run_id="run-test",
    )

    assert manifest.snapshot_count == 2
    assert manifest.warnings
    assert "pagination capped" in manifest.warnings[0]


def test_snapshot_manifest_to_dict(tmp_path):
    client = FakeClient()
    writer = ApiSportsSnapshotWriter(tmp_path)
    collector = ApiSportsSnapshotCollector(client=client, writer=writer)

    manifest = collector.collect_date(date="2026-04-27", include_odds=False, run_id="run-test")
    data = manifest.to_dict()

    assert data["mode"] == "shadow_only_fixtures_odds_snapshot"
    assert data["snapshot_count"] == 1
    assert data["snapshots"][0]["kind"] == "fixtures_by_date"
