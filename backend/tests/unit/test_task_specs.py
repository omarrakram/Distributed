import pytest
from pydantic import ValidationError

from dtq.common.task_specs import TASK_TYPES, UnknownTaskType, validate_payload


def test_all_seven_task_types_are_registered():
    assert set(TASK_TYPES) == {"prime_count", "matrix_multiply", "image_process", "sha256", "delay",
                               "fail_transient", "fail_permanent"}
    assert {s.kind for s in TASK_TYPES.values()} == {"cpu", "io"}


@pytest.mark.parametrize("task_type", sorted(TASK_TYPES))
def test_examples_validate_and_have_default_timeouts(task_type):
    payload, timeout = validate_payload(task_type, TASK_TYPES[task_type].example)
    assert isinstance(payload, dict) and timeout >= 1


@pytest.mark.parametrize("task_type,payload", [
    ("prime_count", {"start": 10, "end": 5}),                  # end must exceed start
    ("prime_count", {"start": 0, "end": 30_000_000}),          # range limit 20M
    ("matrix_multiply", {"size": 301}),                         # size limit
    ("image_process", {"operations": ["rm -rf /"]}),            # whitelist only
    ("image_process", {"width": 4096}),                          # resource limit
    ("sha256", {"iterations": 0}),
    ("delay", {"seconds": -1}),
    ("delay", {"seconds": 5, "code": "import os"}),              # unknown fields are rejected
    ("fail_transient", {"fail_times": 11}),
])
def test_invalid_payloads_are_rejected(task_type, payload):
    with pytest.raises(ValidationError):
        validate_payload(task_type, payload)


def test_unknown_task_type_is_rejected():
    with pytest.raises(UnknownTaskType):
        validate_payload("eval_python", {"code": "1+1"})


def test_delay_timeout_tracks_requested_duration():
    _, timeout = validate_payload("delay", {"seconds": 200})
    assert timeout >= 200
