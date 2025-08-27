from typing import Hashable, Optional, Protocol, Self

from flowrhythm._types import Branch, SinkCtx, SinkFn


class Stage(Protocol):
    def name(self) -> Hashable: ...
    async def __aenter__(self) -> "Stage": ...
    async def __aexit__(self, exc_type, exc, tb) -> None: ...
    async def receive(self, Any): ...


class SinkStage:
    def __init__(self, name: Hashable, sinkctx: SinkCtx):
        self._name = name
        self._sinkctx = sinkctx
        self._sinkfn: Optional[SinkFn] = None

    def name(self) -> Hashable:
        return self._name

    async def __aenter__(self) -> Self:
        self._sinkfn = await self._sinkctx.__aenter__()
        return self

    async def __aexit__(self, exc_type, exc, tb) -> None:
        await self._sinkctx.__aexit__(exc_type, exc, tb)
        self._sinkfn = None

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
