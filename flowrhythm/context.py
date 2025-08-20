from typing import Any, Awaitable, Protocol


class AsyncCallable(Protocol):
    def __call__(self, *args: Any, **kwargs: Any) -> Awaitable[Any]: ...


class _StubContextManager:
    def __init__(self, body=None):
        self._body = body

    async def __aenter__(self):
        return self._body or self

    async def __aexit__(self, exc_type, exc_value, traceback):
        del exc_type, exc_value, traceback
        pass


def _is_async_cman(obj):
    return hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__")
