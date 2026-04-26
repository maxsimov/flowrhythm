import asyncio

import pytest

from flowrhythm._queue import (
    AsyncQueueInterface,
    fifo_queue,
    lifo_queue,
    priority_queue,
)


# ---------------------------------------------------------------------------
# Ordering behavior — basic sanity per queue type
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "factory, values, expected",
    [
        (fifo_queue, [1, 2, 3], [1, 2, 3]),
        (
            priority_queue,
            [(2, "b"), (1, "a"), (3, "c")],
            [(1, "a"), (2, "b"), (3, "c")],
        ),
        (lifo_queue, [1, 2, 3], [3, 2, 1]),
    ],
)
async def test_queue_order(factory, values, expected):
    q: AsyncQueueInterface = factory(10)
    for v in values:
        await q.put(v)
    results = [await q.get() for _ in values]
    assert results == expected
    assert isinstance(q, AsyncQueueInterface)


# ---------------------------------------------------------------------------
# Default maxsize — must be 1 for backpressure (per DESIGN.md)
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_default_maxsize_is_one(factory):
    q = factory()
    assert q.maxsize == 1


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_default_maxsize_blocks_second_put(factory):
    q = factory()
    # priority_queue needs orderable items
    item = (1, "a") if factory is priority_queue else 1
    item2 = (2, "b") if factory is priority_queue else 2

    await q.put(item)
    # Queue is full: a non-blocking put raises immediately, proving the limit
    # without any wall-clock waiting.
    with pytest.raises(asyncio.QueueFull):
        q.put_nowait(item2)
    assert q.full()


# ---------------------------------------------------------------------------
# Graceful shutdown — shutdown(immediate=False)
#   - get() returns remaining items first
#   - then raises QueueShutDown
#   - put() raises QueueShutDown immediately
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_graceful_shutdown_drains_remaining_items(factory):
    q = factory(10)
    items = [(i, str(i)) if factory is priority_queue else i for i in range(3)]
    for item in items:
        await q.put(item)

    q.shutdown(immediate=False)

    # All remaining items can still be retrieved
    drained = [await q.get() for _ in items]
    assert len(drained) == 3

    # After draining, get() raises
    with pytest.raises(asyncio.QueueShutDown):
        await q.get()


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_put_after_shutdown_raises(factory):
    q = factory(10)
    q.shutdown(immediate=False)
    item = (1, "a") if factory is priority_queue else 1
    with pytest.raises(asyncio.QueueShutDown):
        await q.put(item)


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_blocked_put_unblocks_with_shutdown(factory):
    q = factory(1)  # explicit maxsize=1
    item = (1, "a") if factory is priority_queue else 1
    item2 = (2, "b") if factory is priority_queue else 2

    await q.put(item)  # fills queue

    # second put will block
    blocked_put = asyncio.create_task(q.put(item2))
    await asyncio.sleep(0)  # let it start awaiting

    q.shutdown(immediate=False)

    with pytest.raises(asyncio.QueueShutDown):
        await blocked_put


# ---------------------------------------------------------------------------
# Immediate shutdown — shutdown(immediate=True)
#   - drains the queue
#   - all blocked get() callers raise QueueShutDown
# ---------------------------------------------------------------------------


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_immediate_shutdown_unblocks_all_waiting_workers(factory):
    q = factory(10)

    async def worker():
        await q.get()

    tasks = [asyncio.create_task(worker()) for _ in range(4)]
    await asyncio.sleep(0)  # let them all start awaiting

    q.shutdown(immediate=True)

    results = await asyncio.gather(*tasks, return_exceptions=True)
    assert all(isinstance(r, asyncio.QueueShutDown) for r in results)


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_immediate_shutdown_drops_queued_items(factory):
    q = factory(10)
    items = [(i, str(i)) if factory is priority_queue else i for i in range(3)]
    for item in items:
        await q.put(item)

    q.shutdown(immediate=True)

    # All items dropped — even though queue had 3 items, get() raises
    with pytest.raises(asyncio.QueueShutDown):
        await q.get()
