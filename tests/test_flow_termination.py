"""Tests for M5 — Last(value), Flow.drain(), Flow.stop().

Last(value) — graceful termination from inside a transformer:
  1. Kill upstream (shutdown source's queue + stages 0..i)
  2. Kill idle sibling workers in stage i (state 3 — safe to cancel)
  3. Wait until busy siblings finish their current item (their results flow
     into queue i+1 BEFORE Last's value)
  4. Propagate Last.value into queue i+1 (the absolute last item to enter
     downstream)

Flow.drain() — graceful from outside (shutdown queue 0, wait for drain)
Flow.stop() — immediate abort (shutdown all queues, cancel all workers)
"""

import asyncio

from flowrhythm import (
    DropReason,
    Dropped,
    FixedScaling,
    Last,
    flow,
)


# ---------------------------------------------------------------------------
# Last(value) — single-worker stage (the easy case)
# ---------------------------------------------------------------------------


async def test_last_single_worker_value_reaches_sink_last():
    received = []

    async def maybe_terminate(x):
        if x == 5:
            return Last(f"final-{x}")
        return x * 10

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(20):
            yield i

    chain = flow(maybe_terminate, collect)
    await chain.run(items)

    # Items 0..4 produced 0,10,20,30,40 (FIFO single worker → in order)
    assert received[:5] == [0, 10, 20, 30, 40]
    # The very last item is the wrapped value
    assert received[-1] == "final-5"
    # Items 6..19 from source are not present
    assert all(not isinstance(r, int) or r <= 40 for r in received)


# ---------------------------------------------------------------------------
# Last(value) — multi-worker stage: value enters downstream LAST, after siblings
# ---------------------------------------------------------------------------


async def test_last_multi_worker_value_enters_downstream_last():
    """With multi-worker stage that returns Last, sibling workers finish
    their current items first; Last's value enters queue i+1 last."""
    sink_received = []

    async def maybe_terminate(x):
        # Worker that pulls x==99 returns Last
        if x == 99:
            return Last("FINAL")
        return f"item-{x}"

    async def collect(x):
        sink_received.append(x)

    async def items():
        # Push some normal items, then 99, then more (which should be dropped)
        for i in range(10):
            yield i
        yield 99
        for i in range(100, 110):
            yield i  # may or may not get pushed before drain fires

    # First stage is multi-worker (the case we're testing); sink is
    # single-worker so we can assert the receiving order.
    chain = flow(maybe_terminate, collect)
    chain.configure("maybe_terminate", scaling=FixedScaling(workers=4))
    chain.configure("collect", scaling=FixedScaling(workers=1))
    async with asyncio.timeout(2):
        await chain.run(items)

    # FINAL must be present and must be the last item the sink received.
    # (Last guarantees the wrapped value is the last item to enter the
    #  downstream queue; with a single-worker sink, FIFO ordering preserves
    #  that through to the receiver.)
    assert "FINAL" in sink_received
    assert sink_received[-1] == "FINAL"


# ---------------------------------------------------------------------------
# Source items that couldn't be pushed → Dropped event with UPSTREAM_TERMINATED
# ---------------------------------------------------------------------------


async def test_source_items_dropped_after_last_emit_dropped_event():
    drops = []

    async def maybe_terminate(x):
        if x == 2:
            return Last("done")
        return x

    async def sink(x):
        # Slow enough that source has time to attempt a push after queue close
        await asyncio.sleep(0)

    async def items():
        for i in range(100):
            yield i

    async def on_error(event):
        if isinstance(event, Dropped):
            drops.append(event)

    chain = flow(maybe_terminate, sink, on_error=on_error)
    async with asyncio.timeout(2):
        await chain.run(items)

    # At least one item from source should have been dropped after Last fired.
    # All drops should have UPSTREAM_TERMINATED reason.
    assert len(drops) >= 1
    for d in drops:
        assert d.reason is DropReason.UPSTREAM_TERMINATED


# ---------------------------------------------------------------------------
# Flow.drain() — graceful from outside
# ---------------------------------------------------------------------------


async def test_drain_from_outside():
    """Calling chain.drain() while run() is in progress: source stops, items
    in flight finish, run() returns."""
    received = []
    received_lock = asyncio.Lock()
    started = asyncio.Event()

    async def slow(x):
        async with received_lock:
            received.append(x)
            if len(received) >= 3:
                started.set()
        return x

    async def items():
        for i in range(1_000_000):  # essentially infinite source
            yield i

    chain = flow(slow)
    run_task = asyncio.create_task(chain.run(items))

    async with asyncio.timeout(2):
        await started.wait()  # ensure several items processed
        await chain.drain()
        await run_task

    # Drain stopped the source; received contains some items but far from 1M
    assert len(received) >= 3
    assert len(received) < 100  # arbitrary upper bound — we drained quickly


async def test_drain_when_no_run_in_progress_is_noop():
    """Calling drain() when no run is active should be a no-op."""

    async def fn(x):
        return x

    chain = flow(fn)
    # No active run
    await chain.drain()  # must not raise


# ---------------------------------------------------------------------------
# Flow.stop() — immediate abort
# ---------------------------------------------------------------------------


async def test_stop_aborts_immediately():
    """stop() cancels everything; in-flight items may be lost."""
    enters = 0
    exits = 0
    counts_lock = asyncio.Lock()
    started = asyncio.Event()

    async def fn(x):
        nonlocal enters, exits
        async with counts_lock:
            enters += 1
            if enters >= 1:
                started.set()
        try:
            await asyncio.sleep(60)  # long sleep — would never finish
        finally:
            async with counts_lock:
                exits += 1
        return x

    async def items():
        for i in range(1_000_000):
            yield i

    chain = flow(fn)
    run_task = asyncio.create_task(chain.run(items))

    async with asyncio.timeout(2):
        await started.wait()
        await chain.stop()
        await run_task

    # All workers that started should have had their finally run (cleanup)
    assert exits == enters


async def test_stop_runs_cm_aexit_on_cancel():
    """When stop() cancels a worker, the per-worker CM __aexit__ runs."""
    from contextlib import asynccontextmanager

    aenter_count = 0
    aexit_count = 0
    started = asyncio.Event()

    @asynccontextmanager
    async def factory():
        nonlocal aenter_count, aexit_count
        aenter_count += 1
        started.set()
        try:

            async def fn(x):
                await asyncio.sleep(60)
                return x

            yield fn
        finally:
            aexit_count += 1

    async def items():
        for i in range(1_000_000):
            yield i

    chain = flow(factory)
    run_task = asyncio.create_task(chain.run(items))

    async with asyncio.timeout(2):
        await started.wait()
        await chain.stop()
        await run_task

    # CM __aexit__ ran for every worker that __aenter__ed
    assert aexit_count == aenter_count
    assert aenter_count >= 1


async def test_stop_when_no_run_is_noop():
    async def fn(x):
        return x

    chain = flow(fn)
    await chain.stop()  # no active run; no error


# ---------------------------------------------------------------------------
# Last is exported from the public API
# ---------------------------------------------------------------------------


def test_last_class_is_constructable_and_holds_value():
    last = Last(42)
    assert last.value == 42
    last2 = Last("anything")
    assert last2.value == "anything"
