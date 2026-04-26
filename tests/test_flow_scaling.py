"""Tests for M2c — UtilizationScaling integration with the worker pool.

Verifies that on_enqueue / on_dequeue deltas drive actual worker spawning
and that FixedScaling continues to work as before.
"""

import asyncio

from flowrhythm import FixedScaling, UtilizationScaling, flow


# ---------------------------------------------------------------------------
# UtilizationScaling actually grows the worker pool under load
# ---------------------------------------------------------------------------


async def test_utilization_scaling_grows_under_load():
    """A slow stage with rising utilization should add workers."""
    in_flight = 0
    max_in_flight = 0
    state_lock = asyncio.Lock()
    barrier = asyncio.Event()
    saw_growth = asyncio.Event()

    async def slow(x):
        nonlocal in_flight, max_in_flight
        async with state_lock:
            in_flight += 1
            max_in_flight = max(max_in_flight, in_flight)
            if max_in_flight >= 4:
                saw_growth.set()
        await barrier.wait()  # all blocked until released
        async with state_lock:
            in_flight -= 1
        return x

    async def sink(x):
        pass

    async def items():
        for i in range(10):
            yield i

    # Aggressive scaling: cooldown=0, dampening=1, fast ramp
    strategy = UtilizationScaling(
        min_workers=1,
        max_workers=8,
        lower_utilization=0.2,
        upper_utilization=0.5,
        upscaling_rate=4,
        downscaling_rate=1,
        cooldown_seconds=0.0,
        dampening=1.0,
    )

    chain = flow(slow, sink, default_scaling=strategy)

    run_task = asyncio.create_task(chain.run(items))
    try:
        async with asyncio.timeout(2):
            await saw_growth.wait()  # fires once max_in_flight reaches 4
    finally:
        barrier.set()  # release blocked workers so the run can drain
        await run_task

    # Initial workers = 1; without scaling, max_in_flight = 1.
    # With UtilizationScaling, we should observe at least 4 workers concurrent.
    assert max_in_flight >= 4


# ---------------------------------------------------------------------------
# FixedScaling regression: explicit workers=N still works after the refactor
# ---------------------------------------------------------------------------


async def test_fixed_scaling_after_runtime_refactor():
    """FixedScaling(workers=4) should still drive 4 concurrent workers."""
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
        await all_four_in_flight.wait()
        async with state_lock:
            in_flight -= 1
        return x

    async def sink(x):
        pass

    async def items():
        for i in range(10):
            yield i

    chain = flow(slow, sink, default_scaling=FixedScaling(workers=4))
    async with asyncio.timeout(2):
        await chain.run(items)

    assert max_in_flight == 4


# ---------------------------------------------------------------------------
# Items aren't lost or duplicated when scaling is happening
# ---------------------------------------------------------------------------


async def test_no_items_lost_or_duplicated_with_dynamic_scaling():
    """End-to-end with UtilizationScaling: every item reaches the sink exactly once."""
    received = []
    received_lock = asyncio.Lock()

    async def double(x):
        return x * 2

    async def collect(x):
        async with received_lock:
            received.append(x)

    async def items():
        for i in range(50):
            yield i

    strategy = UtilizationScaling(
        min_workers=1,
        max_workers=8,
        lower_utilization=0.2,
        upper_utilization=0.5,
        upscaling_rate=2,
        downscaling_rate=1,
        cooldown_seconds=0.0,
        dampening=1.0,
    )

    chain = flow(double, collect, default_scaling=strategy)
    await chain.run(items)

    assert sorted(received) == sorted(i * 2 for i in range(50))
    assert len(received) == 50  # nothing lost
    assert len(set(received)) == 50  # nothing duplicated
