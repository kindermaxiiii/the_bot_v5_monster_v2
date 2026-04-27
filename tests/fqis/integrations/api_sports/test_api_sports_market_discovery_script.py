import json
import os
import subprocess
import sys
from pathlib import Path


def test_market_discovery_script_safe_failure_without_key(tmp_path):
    script = Path("scripts/fqis_api_sports_market_discovery.py").resolve()
    env = os.environ.copy()
    env.pop("APISPORTS_KEY", None)

    result = subprocess.run(
        [sys.executable, str(script), "--source", "pre_match"],
        check=False,
        capture_output=True,
        text=True,
        cwd=tmp_path,
        env=env,
    )

    payload = json.loads(result.stdout)
    assert result.returncode == 2
    assert payload["status"] == "FAILED"
    assert "APISPORTS_KEY is missing" in payload["reason"]