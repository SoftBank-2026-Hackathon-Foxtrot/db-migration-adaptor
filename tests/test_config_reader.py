import os
import subprocess
from pathlib import Path

import pytest


@pytest.mark.integration
def test_distinct_linux_pgbouncer_reader_reloads_preserved_config():
    if os.environ.get("RUN_DB_OPERATOR_INTEGRATION") != "1":
        pytest.skip("Requires the local lab image and Docker")
    root = Path(__file__).resolve().parents[1]
    result = subprocess.run(
        [
            "docker",
            "run",
            "--rm",
            "--user",
            "root",
            "--entrypoint",
            "/bin/sh",
            "-v",
            f"{root / 'src/db_operator'}:/fixture/db_operator:ro",
            "-v",
            f"{root / 'tests/fixtures/config_reader.py'}:/fixture/check.py:ro",
            "foxtrot-db-operator-lab-pooler",
            "-c",
            "apk add --no-cache python3 >/dev/null && python3 /fixture/check.py",
        ],
        capture_output=True,
        text=True,
        timeout=120,
    )
    assert result.returncode == 0, result.stdout + result.stderr
    assert "PASS:" in result.stdout
