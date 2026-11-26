"""Atomic claiming under concurrency: many dispatchers (as if many broker replicas) race for the same tasks."""

import asyncio

from dtq.broker.scheduler import Claimant, Dispatcher
from tests.conftest import fast_broker_settings
from tests.helpers import submit


async def test_concurrent_dispatchers_never_double_assign(pool, database_url):
    """20 dispatchers (as if 20 broker replicas) race over 200 tasks for 40 workers."""
    settings = fast_broker_settings(database_url)
    workers = [await pool.fetchval("INSERT INTO workers (name, host, capacity) VALUES ($1, 'h', 5) RETURNING id",
                                   f"w{i}") for i in range(40)]
    for _ in range(200):
        await submit(pool, "delay", {"seconds": 1})
    dispatchers = []
    loop = asyncio.get_running_loop()
    for i in range(20):
        d = Dispatcher(pool, settings)
        for w in workers[i * 2: i * 2 + 2]:
            d._claimants[w] = Claimant(w, 5, loop.create_future(), 0.0)
        dispatchers.append(d)
    totals = await asyncio.gather(*(d.dispatch_cycle() for d in dispatchers))
    assert sum(totals) == 200
    dupes = await pool.fetchval("SELECT count(*) FROM (SELECT task_id FROM task_attempts GROUP BY task_id HAVING count(*) > 1) x")
    assert dupes == 0
    over = await pool.fetchval("""SELECT count(*) FROM (SELECT assigned_worker_id, count(*) n FROM tasks
                                  WHERE status='ASSIGNED' GROUP BY 1) x WHERE n > 5""")
    assert over == 0
    assert await pool.fetchval("SELECT count(*) FROM tasks WHERE status='ASSIGNED'") == 200
