"""Tests for M6 — push() + PushHandle.

Push-mode activation: ``async with chain.push() as h: await h.send(item)``.
Workers spawn on __aenter__; on __aexit__, complete() + drain.
"""

import asyncio

import pytest

from flowrhythm import PushHandle, flow


# ---------------------------------------------------------------------------
# Basic push: items reach the sink
# ---------------------------------------------------------------------------


async def test_push_single_stage_items_reach_sink():
    received = []

    async def collect(x):
        received.append(x)

    chain = flow(collect)
    async with chain.push() as h:
        assert isinstance(h, PushHandle)
        await h.send(1)
        await h.send(2)
        await h.send(3)

    assert received == [1, 2, 3]


async def test_push_multi_stage_items_flow_through():
    received = []

    async def double(x):
        return x * 2

    async def collect(x):
        received.append(x)

    chain = flow(double, collect)
    async with chain.push() as h:
        for i in range(5):
            await h.send(i)

    assert received == [0, 2, 4, 6, 8]


# ---------------------------------------------------------------------------
# Drain on async-with exit: all pushed items are processed before exit returns
# ---------------------------------------------------------------------------


async def test_push_drain_on_exit_processes_all_items():
    received = []

    async def slow(x):
        await asyncio.sleep(0)  # let other tasks run; no real wait
        received.append(x)

    chain = flow(slow)
    async with asyncio.timeout(2):
        async with chain.push() as h:
            for i in range(20):
                await h.send(i)
        # On exit, every pushed item should have reached the sink (drain)
    assert sorted(received) == list(range(20))


# ---------------------------------------------------------------------------
# complete() is idempotent; send() after complete() raises
# ---------------------------------------------------------------------------


async def test_push_complete_is_idempotent():
    async def fn(x):
        pass

    chain = flow(fn)
    async with chain.push() as h:
        await h.send(1)
        await h.complete()
        await h.complete()  # must not raise


async def test_push_send_after_complete_raises():
    async def fn(x):
        pass

    chain = flow(fn)
    async with chain.push() as h:
        await h.send(1)
        await h.complete()
        with pytest.raises(RuntimeError, match="complete"):
            await h.send(2)


async def test_push_complete_inside_body_drains_and_exit_is_clean():
    """User can call complete() explicitly inside the body. Items already
    pushed flow through; subsequent send() raises; __aexit__ stays clean."""
    received = []

    async def collect(x):
        received.append(x)

    chain = flow(collect)
    async with chain.push() as h:
        await h.send(1)
        await h.send(2)
        await h.complete()
        with pytest.raises(RuntimeError):
            await h.send(99)

    assert sorted(received) == [1, 2]


# ---------------------------------------------------------------------------
# stop() from another task aborts the push run
# ---------------------------------------------------------------------------


async def test_push_stop_aborts_blocked_send():
    """chain.stop() from another task aborts; a send() blocked on a full
    queue raises asyncio.QueueShutDown."""
    started = asyncio.Event()

    async def slow(x):
        started.set()
        await asyncio.sleep(60)  # never finishes; stop cancels the worker

    chain = flow(slow)

    async def driver():
        async with chain.push() as h:
            # With default maxsize=1 + one busy worker, items 0 and 1 fit
            # (worker dequeued item 0; item 1 in queue). Item 2 blocks on put.
            for i in range(10):
                await h.send(i)

    drive_task = asyncio.create_task(driver())

    async with asyncio.timeout(2):
        await started.wait()
        await chain.stop()
        with pytest.raises(asyncio.QueueShutDown):
            await drive_task


async def test_push_stop_runs_worker_cleanup():
    """When stop() cancels a worker, its CM __aexit__ runs."""
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

            yield fn
        finally:
            aexit_count += 1

    chain = flow(factory)

    async def driver():
        async with chain.push() as h:
            await h.send(1)
            await asyncio.sleep(60)

    drive_task = asyncio.create_task(driver())

    async with asyncio.timeout(2):
        await started.wait()
        await chain.stop()
        drive_task.cancel()
        try:
            await drive_task
        except (asyncio.CancelledError, asyncio.QueueShutDown):
            pass

    assert aexit_count == aenter_count >= 1


# ---------------------------------------------------------------------------
# Reuse: a flow can be activated again after a clean push session ends
# ---------------------------------------------------------------------------


async def test_push_can_be_reactivated_after_clean_exit():
    received = []

    async def collect(x):
        received.append(x)

    chain = flow(collect)
    async with chain.push() as h:
        await h.send("a")
    async with chain.push() as h:
        await h.send("b")

    assert received == ["a", "b"]


# ---------------------------------------------------------------------------
# Public API: PushHandle is importable
# ---------------------------------------------------------------------------


def test_push_handle_is_exported():
    assert PushHandle is not None
