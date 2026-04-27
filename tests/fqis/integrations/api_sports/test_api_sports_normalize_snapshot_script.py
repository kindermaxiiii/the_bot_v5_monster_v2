import json

from scripts.fqis_api_sports_normalize_snapshot import _collect_snapshot_files


def test_collect_snapshot_files_file(tmp_path):
    path = tmp_path / "snapshot.json"
    path.write_text("{}", encoding="utf-8")

    assert _collect_snapshot_files(path) == [path]


def test_collect_snapshot_files_directory(tmp_path):
    a = tmp_path / "a.json"
    b = tmp_path / "nested" / "b.json"
    b.parent.mkdir()
    a.write_text(json.dumps({}), encoding="utf-8")
    b.write_text(json.dumps({}), encoding="utf-8")
    (tmp_path / "ignore.txt").write_text("x", encoding="utf-8")

    assert _collect_snapshot_files(tmp_path) == [a, b]
