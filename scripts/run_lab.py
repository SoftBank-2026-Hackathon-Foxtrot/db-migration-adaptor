"""Run operator commands with private local fixture settings, without shell eval."""

import json
import os
from pathlib import Path

from db_operator.cli import main

path = Path(__file__).resolve().parents[1] / "runtime/lab/environment.json"
if not path.exists():
    raise SystemExit("Run: uv run --locked python scripts/lab.py up")
os.environ.update(json.loads(path.read_text()))
main()
