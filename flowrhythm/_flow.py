"""Minimum linear flow runtime (M1).

A `Flow` holds a list of named stages and a `run(source)` method that drives
them. M1 supports only plain async functions as stages and one worker per
stage. Multi-worker stages, CM factories, sub-flows, routers, push mode,
configuration, and error handling all come in later milestones.

End-of-stream propagates via stdlib `asyncio.Queue.shutdown()` (see
DESIGN.md "EOF / drain cascade").
"""

import asyncio
import inspect
from typing import Any, AsyncGenerator, Callable

from flowrhythm._queue import fifo_queue


class Flow:
    """A pipeline of stages. Construct via the `flow()` factory."""

    def __init__(self, stages: list[tuple[str, Callable[[Any], Any]]]) -> None:
        self._stages = stages

    @property
    def stage_names(self) -> list[str]:
        return [name for name, _ in self._stages]

    async def run(self, source: Callable[[], AsyncGenerator[Any, None]]) -> None:
        """Drive the chain by iterating `source` and pushing items through.

        `source` must be the async generator function itself, not a called
        generator. The framework owns iteration so it can manage the source
        lifecycle.
        """
        if inspect.isasyncgen(source):
            raise TypeError(
                "pass the generator function, not the called generator. "
                "e.g., chain.run(my_items)  not  chain.run(my_items())"
            )
        if not inspect.isasyncgenfunction(source):
            raise TypeError(
                "run(source) requires an async generator function "
                f"(got {type(source).__name__})"
            )

        await self._start_and_join(source)

    async def _start_and_join(
        self, source: Callable[[], AsyncGenerator[Any, None]]
    ) -> None:
        # N stages → N input queues. queues[i] is stage[i]'s input.
        # The source feeds queues[0]. The last stage's result is dropped.
        n = len(self._stages)
        queues = [fifo_queue() for _ in range(n)]

        async def source_task() -> None:
            try:
                async for item in source():
                    await queues[0].put(item)
            finally:
                # Trigger the drain cascade: shutdown(immediate=False) lets
                # workers drain remaining items before exiting.
                queues[0].shutdown(immediate=False)

        async def worker_task(i: int) -> None:
            _, fn = self._stages[i]
            try:
                while True:
                    try:
                        item = await queues[i].get()
                    except asyncio.QueueShutDown:
                        return
                    result = await fn(item)
                    if i + 1 < n:
                        try:
                            await queues[i + 1].put(result)
                        except asyncio.QueueShutDown:
                            return
            finally:
                # M1 has one worker per stage, so this single worker IS the
                # last worker. Closing the downstream queue cascades the
                # shutdown. (M2 will add a per-stage worker counter so only
                # the truly-last worker triggers the next shutdown.)
                if i + 1 < n:
                    queues[i + 1].shutdown(immediate=False)

        tasks = [asyncio.create_task(source_task())]
        for i in range(n):
            tasks.append(asyncio.create_task(worker_task(i)))

        await asyncio.gather(*tasks)


def flow(*stages: Callable[[Any], Any]) -> Flow:
    """Construct a flow from a sequence of async transformer functions.

    Each stage must be an async function taking exactly one argument (the
    item) and returning one item. The last stage acts as the sink — its
    return value is dropped.

    Validation rules (per DESIGN.md):
    - Async generators are rejected — they are sources, not stages, and
      belong to `run()`.
    - Sync functions are rejected — wrap with `asyncio.to_thread` or
      `sync_stage()` (planned).
    - Each stage must take exactly one argument.

    Stage names are auto-derived from function names; collisions get a
    numeric suffix (`normalize`, `normalize_2`, ...).
    """
    if not stages:
        raise TypeError("flow() requires at least one stage")

    validated: list[Callable[[Any], Any]] = []
    for stage in stages:
        if inspect.isasyncgenfunction(stage):
            raise TypeError(
                f"flow() does not accept async generators "
                f"(got {stage.__name__!r}); pass sources to run() instead"
            )
        if not callable(stage):
            raise TypeError(
                f"flow() arguments must be callable, got {type(stage).__name__}"
            )
        if not inspect.iscoroutinefunction(stage):
            name = getattr(stage, "__name__", repr(stage))
            raise TypeError(
                f"flow() requires async functions (got sync {name!r}); "
                f"wrap with asyncio.to_thread or use sync_stage()"
            )
        sig = inspect.signature(stage)
        param_count = len(sig.parameters)
        if param_count != 1:
            name = getattr(stage, "__name__", repr(stage))
            raise TypeError(
                f"transformer must take exactly 1 argument "
                f"(got {param_count} for {name!r})"
            )
        validated.append(stage)

    # Auto-name with collision suffix
    counts: dict[str, int] = {}
    named: list[tuple[str, Callable[[Any], Any]]] = []
    for fn in validated:
        base = fn.__name__
        counts[base] = counts.get(base, 0) + 1
        name = base if counts[base] == 1 else f"{base}_{counts[base]}"
        named.append((name, fn))

    return Flow(stages=named)
