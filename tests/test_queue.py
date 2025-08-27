import pytest

from flowrhythm._queue import (
    AsyncQueueInterface,
    fifo_queue,
    lifo_queue,
    priority_queue,
)


@pytest.mark.asyncio
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
