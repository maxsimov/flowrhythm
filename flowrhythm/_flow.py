"""Flow runtime (M1 + M2a + M2b + M2c).

A `Flow` holds a sequence of named stages plus per-stage and pipeline-wide
configuration. `Flow.run(source)` constructs a `_FlowRun` (an internal
per-execution object) and executes it.

CM factories, sub-flows, routers, push mode, error handling, and dump()
all come in later milestones. See `todos/implement-runtime.md`.

End-of-stream propagates via stdlib `asyncio.Queue.shutdown()` (see
DESIGN.md "EOF / drain cascade"). Worker pool sizing is driven by the
`ScalingStrategy` Protocol — `initial_workers()` at startup, then
`on_enqueue()` / `on_dequeue()` returning deltas during execution. See
DESIGN.md "Worker pool internals".
"""

import asyncio
import inspect
from typing import Any, AsyncGenerator, Callable

from flowrhythm._queue import AsyncQueueFactory, fifo_queue
from flowrhythm._scaling import FixedScaling, ScalingStrategy, StageSnapshot


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

        runner = _FlowRun(self, source)
        await runner.execute()


class _FlowRun:
    """Internal: per-run state and execution loop for a `Flow`.

    Holds all mutable state for one invocation of `Flow.run()`. Workers and
    the source task are spawned as fire-and-forget asyncio tasks; completion
    is detected via `_done_event` set when source has finished AND all
    per-stage `_alive` counters are zero. See DESIGN.md "Worker pool
    internals".
    """

    def __init__(
        self,
        flow: Flow,
        source: Callable[[], AsyncGenerator[Any, None]],
    ) -> None:
        self._flow = flow
        self._source = source

        self._n = len(flow._stages)
        self._configs = [flow._resolve_config(name) for name, _ in flow._stages]
        self._queues = [c["queue"](maxsize=c["queue_size"]) for c in self._configs]
        self._strategies: list[ScalingStrategy] = [c["scaling"] for c in self._configs]

        # Per-stage state — see DESIGN.md "Worker pool internals"
        self._target = [s.initial_workers() for s in self._strategies]
        # `_alive` starts at zero and is incremented by `_spawn_worker()`
        # (the sole source of truth). `execute()` spawns the initial workers
        # and brings _alive up to _target.
        self._alive = [0] * self._n
        self._busy = [0] * self._n
        self._all_workers: list[set[asyncio.Task]] = [set() for _ in range(self._n)]
        self._idle_workers: list[set[asyncio.Task]] = [set() for _ in range(self._n)]
        # Per-stage flag: True once this stage's INPUT queue has been shut
        # down (i.e., upstream is done feeding). Distinguishes drain
        # completion (downstream cascade should fire) from voluntary worker
        # retirement (downstream should NOT cascade).
        self._input_drained = [False] * self._n

        # Run-completion state
        self._source_finished = False
        self._done_event = asyncio.Event()

    # --- Snapshots, completion, and pool management ----------------------------

    def _make_snapshot(self, stage_idx: int) -> StageSnapshot:
        # Timestamps are placeholder zeros for now; M9/M10 will fill them in
        # when `dump(stats)` lands.
        return StageSnapshot(
            stage_name=self._flow._stages[stage_idx][0],
            busy_workers=self._busy[stage_idx],
            idle_workers=self._alive[stage_idx] - self._busy[stage_idx],
            queue_length=self._queues[stage_idx].qsize(),
            oldest_item_enqueued_at=0.0,
            last_enqueue_at=0.0,
            last_dequeue_at=0.0,
            last_scale_up_at=0.0,
            last_scale_down_at=0.0,
            last_error_at=None,
        )

    def _check_done(self) -> None:
        if self._source_finished and all(a == 0 for a in self._alive):
            self._done_event.set()

    def _spawn_worker(self, stage_idx: int) -> None:
        task = asyncio.create_task(self._worker_task(stage_idx))
        self._all_workers[stage_idx].add(task)
        self._alive[stage_idx] += 1

    def _apply_delta(self, stage_idx: int, delta: int) -> None:
        if delta == 0:
            return
        self._target[stage_idx] = max(0, self._target[stage_idx] + delta)
        diff = self._target[stage_idx] - self._alive[stage_idx]
        if diff > 0:
            for _ in range(diff):
                self._spawn_worker(stage_idx)
        # diff < 0: polling check at top of worker loop handles voluntary
        # retirement. Targeted-cancel for scale-to-zero is deferred (M2d).

    # --- Tasks -----------------------------------------------------------------

    async def _source_task(self) -> None:
        try:
            async for item in self._source():
                await self._queues[0].put(item)
                # Notify the first stage's strategy that an item arrived
                self._apply_delta(
                    0, self._strategies[0].on_enqueue(self._make_snapshot(0))
                )
        finally:
            self._queues[0].shutdown(immediate=False)
            self._input_drained[0] = True
            self._source_finished = True
            self._check_done()

    async def _worker_task(self, stage_idx: int) -> None:
        my_task = asyncio.current_task()
        try:
            while True:
                # Polling retirement check (per DESIGN.md)
                if self._alive[stage_idx] > self._target[stage_idx]:
                    return

                self._idle_workers[stage_idx].add(my_task)
                try:
                    item = await self._queues[stage_idx].get()
                except (asyncio.QueueShutDown, asyncio.CancelledError):
                    return
                finally:
                    self._idle_workers[stage_idx].discard(my_task)

                # Dequeue happened; busy++ before the user's transformer runs
                self._busy[stage_idx] += 1
                # Notify strategy: util is now higher
                self._apply_delta(
                    stage_idx,
                    self._strategies[stage_idx].on_dequeue(
                        self._make_snapshot(stage_idx)
                    ),
                )

                try:
                    result = await self._flow._stages[stage_idx][1](item)
                finally:
                    self._busy[stage_idx] -= 1

                if stage_idx + 1 < self._n:
                    try:
                        await self._queues[stage_idx + 1].put(result)
                    except asyncio.QueueShutDown:
                        return
                    # Notify downstream stage that an item arrived
                    self._apply_delta(
                        stage_idx + 1,
                        self._strategies[stage_idx + 1].on_enqueue(
                            self._make_snapshot(stage_idx + 1)
                        ),
                    )
        finally:
            self._all_workers[stage_idx].discard(my_task)
            self._alive[stage_idx] -= 1
            # Drain cascade: only fire downstream shutdown when this stage is
            # fully drained (its input was shut down upstream and we're the
            # last worker out). Voluntary retirement (target shrunk) doesn't
            # trigger cascade because the source / upstream is still running.
            if (
                self._alive[stage_idx] == 0
                and stage_idx + 1 < self._n
                and self._input_drained[stage_idx]
            ):
                self._queues[stage_idx + 1].shutdown(immediate=False)
                self._input_drained[stage_idx + 1] = True
            self._check_done()

    # --- Entry point -----------------------------------------------------------

    async def execute(self) -> None:
        for stage_idx in range(self._n):
            for _ in range(self._target[stage_idx]):
                self._spawn_worker(stage_idx)
        asyncio.create_task(self._source_task())
        # Edge case: if the chain is empty (no stages, no workers spawned),
        # check_done can be true immediately. Source still sets the flag.
        self._check_done()
        await self._done_event.wait()


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
