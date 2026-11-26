"""Configuration validation."""

import pytest

from dtq.common.config import BrokerSettings, WorkerSettings, _split_csv


# ---------------------------------------------------------------------- config
def test_config_rejects_inconsistent_failure_detector_timing():
    with pytest.raises(ValueError):
        BrokerSettings(heartbeat_interval_s=5, worker_suspect_after_s=3, worker_dead_after_s=10)
    with pytest.raises(ValueError):
        BrokerSettings(retry_backoff_base_s=60, retry_backoff_max_s=30)


def test_config_helpers():
    assert _split_csv("http://a:1/, http://b:2 ,") == ["http://a:1", "http://b:2"]
    assert WorkerSettings().worker_name.startswith("worker-")
    assert BrokerSettings(lease_duration_s=15).renew_interval_s == 5
