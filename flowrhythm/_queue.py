import asyncio
from typing import Any, Protocol, runtime_checkable


@runtime_checkable
class AsyncQueueInterface(Protocol):
    async def put(self, item: Any) -> None: ...
    async def get(self) -> Any: ...


class AsyncQueueFactory(Protocol):
    def __call__(self, maxsize: int) -> AsyncQueueInterface: ...


def fifo_queue(maxsize: int) -> asyncio.Queue:
    return asyncio.Queue(maxsize=maxsize)


def priority_queue(maxsize: int) -> asyncio.PriorityQueue:
    return asyncio.PriorityQueue(maxsize=maxsize)


def lifo_queue(maxsize: int) -> asyncio.LifoQueue:
    return asyncio.LifoQueue(maxsize=maxsize)
