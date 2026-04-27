import json
import os
import subprocess
import sys
from pathlib import Path


def test_snapshot_script_fails_safely_without_key(tmp_path):
    repo_root = Path(__file__).resolve().parents[4]
    script = repo_root / "scripts" / "fqis_api_sports_snapshot.py"
    env = os.environ.copy()
    env.pop("APISPORTS_KEY", None)

    result = subprocess.run(
        [sys.executable, str(script), "--date", "2026-04-27", "--snapshot-dir", str(tmp_path / "snapshots")],
        cwd=tmp_path,
        env=env,
        text=True,
        capture_output=True,
        check=False,
    )

    assert result.returncode == 2
    payload = json.loads(result.stdout)
    assert payload["status"] == "FAILED"
    assert "APISPORTS_KEY is missing" in payload["reason"]
    assert "SECRET" not in result.stdout
