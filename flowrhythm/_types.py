from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Hashable,
    Union,
)

Producer = Callable[[], AsyncGenerator[Any, None]]

TransformerFn = Callable[[Any], Awaitable[Any]]
TransformerCtx = AsyncContextManager[TransformerFn]
Transformer = Union[TransformerFn, TransformerCtx]

SinkFn = Callable[[Any], Awaitable[None]]
SinkCtx = AsyncContextManager[SinkFn]
Sink = Union[SinkFn, SinkCtx]

BranchFn = Callable[[Any], Awaitable[Hashable]]
BranchCtx = AsyncContextManager[BranchFn]
Branch = Union[BranchFn, BranchCtx]

ErrorHandlerFn = Callable[[Any, Exception], Awaitable[None]]
ErrorHandlerCtx = AsyncContextManager[ErrorHandlerFn]
ErrorHandler = Union[ErrorHandlerFn, ErrorHandlerCtx]
