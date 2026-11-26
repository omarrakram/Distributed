"""Scheduler planning: FIFO tasks to least-utilised, round-robin-fair workers within free capacity."""

from dtq.broker.scheduler import Candidate, plan_assignments


# --------------------------------------------------------------------- scheduler
def test_plan_never_exceeds_free_slots_and_balances_round_robin():
    cands = [Candidate("a", 2, 0, 2), Candidate("b", 2, 0, 2), Candidate("c", 2, 0, 2)]
    plan = plan_assignments(cands, list(range(10)))
    assert len(plan) == 6
    counts = {w: sum(1 for _, x in plan if x == w) for w in "abc"}
    assert counts == {"a": 2, "b": 2, "c": 2}
    assert [w for _, w in plan[:3]] == ["a", "b", "c"]  # FIFO tasks spread one per worker first


def test_plan_prefers_least_utilised_worker():
    cands = [Candidate("busy", 4, 3, 1), Candidate("idle", 4, 0, 4)]
    plan = plan_assignments(cands, ["t1", "t2", "t3"])
    assert plan[0] == ("t1", "idle")
    assert sum(1 for _, w in plan if w == "busy") <= 1


def test_plan_breaks_ties_by_least_recently_assigned():
    cands = [Candidate("recent", 1, 0, 1, last_assigned=200.0), Candidate("old", 1, 0, 1, last_assigned=100.0)]
    assert plan_assignments(cands, ["t"]) == [("t", "old")]


def test_plan_with_no_capacity_assigns_nothing():
    assert plan_assignments([Candidate("a", 2, 2, 0)], ["t"]) == []
