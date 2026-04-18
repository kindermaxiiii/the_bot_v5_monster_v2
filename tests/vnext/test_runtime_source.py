from __future__ import annotations

from app.vnext.runtime.demo_data import build_demo_offers, build_demo_snapshot
from app.vnext.runtime.source import SnapshotSource


def test_snapshot_source_exposes_snapshots_and_offers() -> None:
    snapshot = build_demo_snapshot()
    offers = build_demo_offers()
    source = SnapshotSource(
        snapshots=(snapshot,),
        offers_by_fixture={snapshot.fixture_id: offers},
    )

    snapshots = source.fetch_live_snapshots(5)
    fixture_offers = source.fetch_market_offers(snapshot.fixture_id)

    assert len(snapshots) == 1
    assert snapshots[0].fixture_id == snapshot.fixture_id
    assert fixture_offers
