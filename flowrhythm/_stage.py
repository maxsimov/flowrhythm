from contextlib import asynccontextmanager
from typing import Hashable, Protocol, Self

from flowrhythm._scaling import ScalingStrategy
from flowrhythm._types import Branch, SinkCtx


class Stage(Protocol):
    def name(self) -> Hashable: ...
    async def __aenter__(self) -> "Stage": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...
    async def receive(self, Any): ...


@asynccontextmanager
async def _default_sink_ctx():
    async def identity(_):
        pass

    yield identity


class SinkStage:
    def __init__(
        self,
        name: Hashable,
    ):
        self._name = name
        self._sink: SinkCtx = _default_sink_ctx()

    def name(self) -> Hashable:
        return self._name

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    async def receive(self, Any):
        pass


class TransformerStage:
    def __init__(self, name: Hashable, target: Hashable):
        self._name = name
        self._target = target

    def name(self) -> Hashable:
        return self._name

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    async def receive(self, Any):
        pass


class BranchStage:
    def __init__(
        self,
        name: Hashable,
        brancher: Branch,
    ):
        self._name = name
        self._brancher = brancher

    def name(self) -> Hashable:
        return self._name

    async def __aenter__(self) -> Self:
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        pass

    async def receive(self, Any):
        pass
