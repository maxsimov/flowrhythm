"""Tests for M2a — multi-worker stages and the drain cascade with N>1.

For now, worker count is set via the hidden `_workers_per_stage` kwarg to
`flow()`. M2b will replace this with the public `configure()` API.
"""

import asyncio

from flowrhythm import flow


# ---------------------------------------------------------------------------
# Smoke test — the M2a milestone goal
# ---------------------------------------------------------------------------


async def test_multi_worker_stage_processes_all_items():
    """4 workers per stage, 100 items, all arrive (order may differ)."""
    received = []
    received_lock = asyncio.Lock()

    async def double(x):
        return x * 2

    async def collect(x):
        async with received_lock:
            received.append(x)

    async def items():
        for i in range(100):
            yield i

    chain = flow(double, collect, _workers_per_stage=4)
    await chain.run(items)

    expected = sorted(i * 2 for i in range(100))
    assert sorted(received) == expected
    assert len(received) == 100  # nothing lost
    assert len(set(received)) == 100  # nothing duplicated


# ---------------------------------------------------------------------------
# Concurrency is observable
# ---------------------------------------------------------------------------


async def test_workers_run_concurrently():
    """4 workers must be in-flight simultaneously when items are available.

    Each worker enters `slow`, increments in_flight, and waits on a barrier
    that fires only when in_flight reaches 4. If the framework runs workers
    sequentially (bug), this test will deadlock — wrapped in
    `asyncio.timeout` as a safety net so it fails fast rather than hanging.
    """
    in_flight = 0
    max_in_flight = 0
    state_lock = asyncio.Lock()
    all_four_in_flight = asyncio.Event()

    async def slow(x):
        nonlocal in_flight, max_in_flight
        async with state_lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            if in_flight >= 4:
                all_four_in_flight.set()
        # Block until all 4 are in-flight. No real time is spent — the event
        # fires the moment the 4th worker enters.
        await all_four_in_flight.wait()
        async with state_lock:
            in_flight -= 1
        return x

    async def sink(x):
        pass

    async def items():
        for i in range(10):
            yield i

    chain = flow(slow, sink, _workers_per_stage=4)
    async with asyncio.timeout(2):  # safety net against deadlock; not measuring time
        await chain.run(items)

    # Strict equality: proves all 4 workers were truly concurrent at peak.
    assert max_in_flight == 4


async def test_default_remains_single_worker():
    """No _workers_per_stage kwarg → still 1 worker per stage (preserves M1 ordering)."""
    received = []

    async def double(x):
        return x * 2

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(50):
            yield i

    chain = flow(double, collect)  # no kwarg → default
    await chain.run(items)

    # Single worker per stage preserves order
    assert received == [i * 2 for i in range(50)]


# ---------------------------------------------------------------------------
# Drain cascade with N > 1
# ---------------------------------------------------------------------------


async def test_drain_cascade_with_multiple_workers_per_stage():
    """Multi-worker stages: every worker exits, run() returns cleanly, no hang."""
    received = []
    received_lock = asyncio.Lock()

    async def t1(x):
        return x + 1

    async def t2(x):
        return x * 10

    async def collect(x):
        async with received_lock:
            received.append(x)

    async def items():
        for i in range(50):
            yield i

    chain = flow(t1, t2, collect, _workers_per_stage=4)
    # If the cascade is broken (e.g., shutdown isn't called when alive count
    # hits 0), this will hang and pytest will time out. The fact that run()
    # returns at all proves the multi-worker drain cascade works.
    await chain.run(items)

    expected = sorted((i + 1) * 10 for i in range(50))
    assert sorted(received) == expected
    assert len(received) == 50


async def test_drain_cascade_with_empty_source_and_many_workers():
    """8 workers per stage, empty source — all workers must exit promptly."""
    received = []

    async def passthrough(x):
        return x

    async def collect(x):
        received.append(x)

    async def items():
        return
        yield  # unreachable

    chain = flow(passthrough, collect, _workers_per_stage=8)
    await chain.run(items)

    assert received == []
