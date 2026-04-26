"""Linear flow runtime (M1 + M2a + M2b).

A `Flow` holds a list of named stages, per-stage and pipeline-wide
configuration, and a `run(source)` method that drives the pipeline. Each
stage runs N workers per its `ScalingStrategy.initial_workers()`. CM
factories, sub-flows, routers, push mode, and error handling all come in
later milestones.

End-of-stream propagates via stdlib `asyncio.Queue.shutdown()` (see
DESIGN.md "EOF / drain cascade"). Each stage tracks its alive worker count;
when the count drops to 0, the stage shuts down its downstream queue,
cascading drain through the rest of the pipeline.
"""

import asyncio
import inspect
from typing import Any, AsyncGenerator, Callable

from flowrhythm._queue import AsyncQueueFactory, fifo_queue
from flowrhythm._scaling import FixedScaling, ScalingStrategy


class Flow:
    """A pipeline of stages. Construct via the `flow()` factory."""

    def __init__(
        self,
        stages: list[tuple[str, Callable[[Any], Any]]],
        default_scaling: ScalingStrategy | None = None,
        default_queue: AsyncQueueFactory | None = None,
        default_queue_size: int | None = None,
    ) -> None:
        self._stages = stages
        self._default_config: dict[str, Any] = {}
        if default_scaling is not None:
            self._default_config["scaling"] = default_scaling
        if default_queue is not None:
            self._default_config["queue"] = default_queue
        if default_queue_size is not None:
            self._default_config["queue_size"] = default_queue_size
        self._stage_config: dict[str, dict[str, Any]] = {}

    @property
    def stage_names(self) -> list[str]:
        return [name for name, _ in self._stages]

    def configure(
        self,
        name: str,
        *,
        scaling: ScalingStrategy | None = None,
        queue: AsyncQueueFactory | None = None,
        queue_size: int | None = None,
    ) -> None:
        """Set per-stage scaling, queue type, and/or queue size.

        `None` means "no override" (keep whatever was there or fall back to
        defaults). Unknown stage names are silently stored — see DESIGN.md
        open question; this may change later.
        """
        cfg = self._stage_config.setdefault(name, {})
        if scaling is not None:
            cfg["scaling"] = scaling
        if queue is not None:
            cfg["queue"] = queue
        if queue_size is not None:
            cfg["queue_size"] = queue_size

    def configure_default(
        self,
        *,
        scaling: ScalingStrategy | None = None,
        queue: AsyncQueueFactory | None = None,
        queue_size: int | None = None,
    ) -> None:
        """Set pipeline-wide defaults. Per-stage `configure()` overrides these."""
        if scaling is not None:
            self._default_config["scaling"] = scaling
        if queue is not None:
            self._default_config["queue"] = queue
        if queue_size is not None:
            self._default_config["queue_size"] = queue_size

    def _resolve_config(self, name: str) -> dict[str, Any]:
        """Per-stage override → pipeline default → built-in default."""
        per_stage = self._stage_config.get(name, {})
        return {
            "scaling": per_stage.get("scaling")
            or self._default_config.get("scaling")
            or FixedScaling(workers=1),
            "queue": per_stage.get("queue")
            or self._default_config.get("queue")
            or fifo_queue,
            "queue_size": per_stage.get("queue_size")
            or self._default_config.get("queue_size")
            or 1,
        }

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
        # Resolve effective config per stage and build queues + worker counts.
        n = len(self._stages)
        configs = [self._resolve_config(name) for name, _ in self._stages]
        queues = [c["queue"](maxsize=c["queue_size"]) for c in configs]
        worker_counts = [c["scaling"].initial_workers() for c in configs]

        # Per-stage alive-worker counter. When it hits 0, the worker that
        # decremented to 0 shuts down the next queue — cascading drain.
        # asyncio is single-threaded cooperative, so plain ints are safe.
        alive = list(worker_counts)

        async def source_task() -> None:
            try:
                async for item in source():
                    await queues[0].put(item)
            finally:
                # Trigger the drain cascade: shutdown(immediate=False) lets
                # workers drain remaining items before exiting.
                queues[0].shutdown(immediate=False)

        async def worker_task(stage_idx: int) -> None:
            _, fn = self._stages[stage_idx]
            try:
                while True:
                    try:
                        item = await queues[stage_idx].get()
                    except asyncio.QueueShutDown:
                        return
                    result = await fn(item)
                    if stage_idx + 1 < n:
                        try:
                            await queues[stage_idx + 1].put(result)
                        except asyncio.QueueShutDown:
                            return
            finally:
                # Decrement the alive count for this stage. Only the LAST
                # worker out (alive count → 0) shuts down the next queue,
                # so downstream sees a single drain trigger no matter how
                # many workers this stage had.
                alive[stage_idx] -= 1
                if alive[stage_idx] == 0 and stage_idx + 1 < n:
                    queues[stage_idx + 1].shutdown(immediate=False)

        tasks = [asyncio.create_task(source_task())]
        for stage_idx in range(n):
            for _ in range(worker_counts[stage_idx]):
                tasks.append(asyncio.create_task(worker_task(stage_idx)))

        await asyncio.gather(*tasks)


def flow(
    *stages: Callable[[Any], Any],
    default_scaling: ScalingStrategy | None = None,
    default_queue: AsyncQueueFactory | None = None,
    default_queue_size: int | None = None,
) -> Flow:
    """Construct a flow from a sequence of async transformer functions.

    Each stage must be an async function taking exactly one argument (the
    item) and returning one item. The last stage acts as the sink — its
    return value is dropped.

    The optional `default_*` kwargs are shorthand for calling
    `configure_default(...)` on the resulting Flow.

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

    return Flow(
        stages=named,
        default_scaling=default_scaling,
        default_queue=default_queue,
        default_queue_size=default_queue_size,
    )
