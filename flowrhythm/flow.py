import uuid
from contextlib import asynccontextmanager
from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Hashable,
    Self,
    TypeVar,
    Union,
)

from flowrhythm._queue import fifo_queue
from flowrhythm._scaling import FixedScaling, ScalingStrategy
from flowrhythm._types import (
    Branch,
    ErrorHandler,
    Producer,
    Transformer,
)


async def _default_producer() -> AsyncGenerator[Any, None]:
    if False:
        yield


@asynccontextmanager
async def _default_error_ctx():
    async def identity(_item: Any, _exc: Exception) -> None:
        pass

    yield identity


@asynccontextmanager
async def _direct_link(id: Hashable):
    async def identity(_) -> Hashable:
        return id

    yield identity


T = TypeVar("T")


def to_ctx(obj: Union[T, AsyncContextManager[T]]) -> AsyncContextManager[T]:
    if isinstance(obj, AsyncContextManager):
        return obj

    @asynccontextmanager
    async def ctx():
        yield obj

    return ctx()


@asynccontextmanager
async def _default_sink_ctx():
    async def identity(_):
        pass

    yield identity


class Flow:
    def __init__(self, *, queue_factory=fifo_queue) -> None:
        self._queue_factory = queue_factory
        self._producer: Producer = _default_producer
        self._error: ErrorHandler = _default_error_ctx()

    async def start(self) -> None:
        pass

    async def wait(self) -> None:
        pass

    async def stop(self) -> None:
        pass
