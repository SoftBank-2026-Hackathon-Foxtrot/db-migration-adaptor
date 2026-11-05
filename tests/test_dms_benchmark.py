import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "scripts"))
from benchmark_dms import log_latencies  # noqa: E402


def test_skipped_and_failed_requests_are_not_completed_latency(tmp_path):
    path = tmp_path / "pgbench.log"
    path.write_text(
        "0 1 11000 0 100 0 8000\n"
        "0 2 skipped 0 100 1 1000000\n"
        "0 3 failed 0 100 2 900000\n"
        "0 4 21000 0 100 3 1000\n"
    )
    result = log_latencies([path], throttled=True)
    assert result["scheduled_latency_ms"]["samples"] == 2
    assert result["scheduled_latency_ms"]["p50"] == 11
    assert result["execution_latency_ms"]["p50"] == 3
    assert result["execution_latency_ms"]["p95"] == 20


def test_unthrottled_log_does_not_require_schedule_lag_column(tmp_path):
    path = tmp_path / "pgbench.log"
    path.write_text("0 1 12500 0 100 0\n0 2 25000 0 100 1\n")
    result = log_latencies([path], throttled=False)
    assert result["execution_latency_ms"] == result["scheduled_latency_ms"]
    assert result["execution_latency_ms"]["p95"] == 25
