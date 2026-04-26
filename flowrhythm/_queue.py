"""Queue factories for stage input queues.

The framework uses bounded queues by default (`maxsize=1`) so backpressure
propagates naturally: a slow downstream stage causes upstream `put()` to
block, which throttles the source. See DESIGN.md "Queue size and
backpressure" for the rationale.

End-of-stream propagation uses stdlib `asyncio.Queue.shutdown()` (added in
Python 3.13). LIFO and Priority variants inherit `shutdown()` from
`asyncio.Queue`. See DESIGN.md "EOF / drain cascade".
"""

import asyncio
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AsyncQueueInterface(Protocol):
    async def put(self, item: Any) -> None: ...
    async def get(self) -> Any: ...
    def shutdown(self, immediate: bool = False) -> None: ...


class AsyncQueueFactory(Protocol):
    def __call__(self, maxsize: int = 1) -> AsyncQueueInterface: ...


def fifo_queue(maxsize: int = 1) -> asyncio.Queue:
    return asyncio.Queue(maxsize=maxsize)


def priority_queue(maxsize: int = 1) -> asyncio.PriorityQueue:
    return asyncio.PriorityQueue(maxsize=maxsize)


def lifo_queue(maxsize: int = 1) -> asyncio.LifoQueue:
    return asyncio.LifoQueue(maxsize=maxsize)
