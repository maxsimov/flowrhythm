from typing import (
    Any,
    AsyncContextManager,
    AsyncGenerator,
    Awaitable,
    Callable,
    Optional,
    Union,
)

Producer = Callable[[], AsyncGenerator[Any, None]]

TransformerFn = Callable[[Any], Awaitable[Any]]
TransformerCtx = AsyncContextManager[TransformerFn]
Transformer = Union[TransformerFn, TransformerCtx]

ConsumerFn = Callable[[Any], Awaitable[None]]
ConsumerCtx = AsyncContextManager[ConsumerFn]
Consumer = Union[ConsumerFn, ConsumerCtx]

ErrorHandlerFn = Callable[[Any, Exception], Awaitable[None]]
ErrorHandlerCtx = AsyncContextManager[ErrorHandlerFn]
ErrorHandler = Union[ErrorHandlerFn, ErrorHandlerCtx]


class Flow:
    def __init__(self) -> None:
        pass

    def set_producer(self, producer: Producer) -> None:
        pass

    def add_transformer(
        self, transformer: Transformer, *, name: Optional[str] = None
    ) -> None:
        pass

    def set_consumer(self, consumer: Consumer) -> None:
        pass

    def set_error_handler(self, handler: ErrorHandler) -> None:
        pass

    async def start(self) -> None:
        pass

    async def wait(self) -> None:
        pass

    async def stop(self) -> None:
        pass
