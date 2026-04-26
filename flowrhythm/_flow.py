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
import functools
import inspect
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncContextManager, AsyncGenerator, Awaitable, Callable

from flowrhythm._errors import (
    ErrorEvent,
    SourceError,
    TransformerError,
    default_handler,
)
from flowrhythm._queue import AsyncQueueFactory, AsyncQueueInterface, fifo_queue
from flowrhythm._scaling import FixedScaling, ScalingStrategy, StageSnapshot

# Type alias for the error handler: `async def handler(event) -> None`
ErrorHandler = Callable[[ErrorEvent], Awaitable[None]]


# A normalised stage factory: a no-arg callable returning an
# AsyncContextManager whose `__aenter__` yields the per-item callable.
TransformerFn = Callable[[Any], Any]
StageFactory = Callable[[], AsyncContextManager[TransformerFn]]


def _wrap_plain_as_factory(fn: TransformerFn) -> StageFactory:
    """Wrap a plain async transformer in a CM-factory shape.

    Plain functions and CM factories use one uniform code path in the worker
    loop (`async with stage.factory() as user_fn`). Plain functions are
    wrapped here at construction so the runtime never has to discriminate.
    """

    @asynccontextmanager
    async def factory() -> AsyncGenerator[TransformerFn, None]:
        yield fn

    return factory


def sync_stage(fn: Callable[[Any], Any]) -> Callable[[Any], Any]:
    """Wrap a sync function so it can be used as a flow stage.

    The framework rejects sync transformers (would block the event loop).
    This helper offloads each call to a thread via `asyncio.to_thread`,
    making a sync function safe to use as an async stage.

    Stage names auto-derive from `fn.__name__` (preserved via functools.wraps).

        chain = flow(sync_stage(json.loads), normalize, db_write)
    """

    @functools.wraps(fn)
    async def wrapped(item: Any) -> Any:
        return await asyncio.to_thread(fn, item)

    return wrapped


@dataclass
class _StageRuntime:
    """Per-stage runtime state for one `_FlowRun`.

    All scattered state for stage i — the factory, queue, scaling strategy,
    counters, drain flag, worker task sets — lives here in one place. New
    per-stage state added by future milestones (M9/M10's timestamps, etc.)
    lands as new fields on this dataclass rather than as another parallel
    list on `_FlowRun`.

    See DESIGN.md "Per-stage state organization" for the rationale.
    """

    name: str
    # Normalised CM-factory form: factory() returns an AsyncContextManager
    # whose `__aenter__` yields the per-item callable. Plain async functions
    # are wrapped at construction; class-based and @asynccontextmanager
    # factories are stored as-is.
    factory: StageFactory
    queue: AsyncQueueInterface
    strategy: ScalingStrategy
    target: int
    alive: int = 0
    busy: int = 0
    # True once this stage's input queue has been shut down (upstream is done
    # feeding). Distinguishes drain cascade from voluntary worker retirement.
    input_drained: bool = False
    all_workers: set[asyncio.Task] = field(default_factory=set)
    idle_workers: set[asyncio.Task] = field(default_factory=set)


class Flow:
    """A pipeline of stages. Construct via the `flow()` factory."""

    def __init__(
        self,
        stages: list[tuple[str, StageFactory]],
        default_scaling: ScalingStrategy | None = None,
        default_queue: AsyncQueueFactory | None = None,
        default_queue_size: int | None = None,
        on_error: ErrorHandler | None = None,
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
        self._error_handler: ErrorHandler = on_error or default_handler

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

    def set_error_handler(self, handler: ErrorHandler) -> None:
        """Replace the error handler.

        Equivalent to passing `on_error=handler` to `flow()`. The handler is
        an `async def handler(event)` callable that receives one of the
        typed events from `flowrhythm._errors` (TransformerError, SourceError,
        Dropped). Return to continue; raise to abort the run.
        """
        self._error_handler = handler

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

    One `_FlowRun` is created per call to `Flow.run()`. It owns all mutable
    state for that execution: the list of `_StageRuntime` (per-stage state),
    the source generator, and the run-completion event.

    Workers and the source task are spawned as fire-and-forget asyncio tasks
    (no `gather`, no `TaskGroup`). Completion is detected from runtime state
    via `_done_event`: set when source has finished AND all per-stage
    `alive` counters are zero.

    See DESIGN.md "Worker pool internals" for the design (worker lifecycle
    states, two-counter pool sizing, drain cascade) and "Run completion:
    state-driven, not task-driven" for the wait-for-done mechanism.
    """

    def __init__(
        self,
        flow: Flow,
        source: Callable[[], AsyncGenerator[Any, None]],
    ) -> None:
        self._source = source

        # Build per-stage runtime objects. All scattered state for stage i
        # lives in self._stages[i] — see _StageRuntime docstring and
        # DESIGN.md "Per-stage state organization".
        self._stages: list[_StageRuntime] = []
        for name, factory in flow._stages:
            cfg = flow._resolve_config(name)
            strategy = cfg["scaling"]
            self._stages.append(
                _StageRuntime(
                    name=name,
                    factory=factory,
                    queue=cfg["queue"](maxsize=cfg["queue_size"]),
                    strategy=strategy,
                    target=strategy.initial_workers(),
                    # `alive` starts at 0 and is incremented by
                    # `_spawn_worker()` (sole source of truth). `execute()`
                    # spawns the initial workers and brings alive up to target.
                )
            )

        # Error handling
        self._handler: ErrorHandler = flow._error_handler
        # First exception captured by _abort (handler-raise or framework
        # bug). Stored so execute() can re-raise from run() after the
        # pipeline drains.
        self._abort_exception: BaseException | None = None

        # Run-completion state
        self._source_finished = False
        self._done_event = asyncio.Event()

    @property
    def _n(self) -> int:
        return len(self._stages)

    # --- Snapshots, completion, and pool management ----------------------------

    def _make_snapshot(self, stage_idx: int) -> StageSnapshot:
        # Timestamps are placeholder zeros for now; M9/M10 will fill them in
        # when `dump(stats)` lands.
        s = self._stages[stage_idx]
        return StageSnapshot(
            stage_name=s.name,
            busy_workers=s.busy,
            idle_workers=s.alive - s.busy,
            queue_length=s.queue.qsize(),  # type: ignore[attr-defined]
            oldest_item_enqueued_at=0.0,
            last_enqueue_at=0.0,
            last_dequeue_at=0.0,
            last_scale_up_at=0.0,
            last_scale_down_at=0.0,
            last_error_at=None,
        )

    def _check_done(self) -> None:
        if self._source_finished and all(s.alive == 0 for s in self._stages):
            self._done_event.set()

    def _spawn_worker(self, stage_idx: int) -> None:
        task = asyncio.create_task(self._worker_task(stage_idx))
        s = self._stages[stage_idx]
        s.all_workers.add(task)
        s.alive += 1

    async def _handle_error(self, event: ErrorEvent) -> None:
        """Call the user's error handler with `event`.

        If the handler raises, capture the exception and trigger an abort.
        Catches `Exception` only (NOT `BaseException`) so cooperative
        cancellation propagates — see DESIGN.md "Asyncio safety notes".
        """
        try:
            await self._handler(event)
        except Exception as exc:
            self._abort(exc)

    def _abort(self, exc: BaseException) -> None:
        """Initiate immediate-abort cascade with `exc` as the cause.

        Captures the first exception (subsequent calls are no-ops). Calls
        `shutdown(immediate=True)` on every queue so workers awaiting
        `get()` unblock with `QueueShutDown` and exit. `execute()` re-raises
        the captured exception after `_done_event` fires.
        """
        if self._abort_exception is not None:
            return
        self._abort_exception = exc
        for s in self._stages:
            s.queue.shutdown(immediate=True)  # type: ignore[attr-defined]
            s.input_drained = True

    def _maybe_cascade(self, stage_idx: int) -> None:
        """If stage `stage_idx` is fully drained, shut down downstream and recurse.

        Called from both source_task's and worker_task's `finally`. The two
        callers can fire in either order — whichever runs last (sets the
        last condition true) triggers the cascade. Recursion handles the
        case where stages were already empty: e.g. all stage 0 workers died
        before source ended; when source's finally sets input_drained, the
        cascade rolls through stages 1..N here.
        """
        s = self._stages[stage_idx]
        if not (s.alive == 0 and s.input_drained):
            return
        if stage_idx + 1 >= self._n:
            return
        downstream = self._stages[stage_idx + 1]
        if downstream.input_drained:
            return
        downstream.queue.shutdown(immediate=False)  # type: ignore[attr-defined]
        downstream.input_drained = True
        self._maybe_cascade(stage_idx + 1)

    def _apply_delta(self, stage_idx: int, delta: int) -> None:
        if delta == 0:
            return
        s = self._stages[stage_idx]
        s.target = max(0, s.target + delta)
        diff = s.target - s.alive
        if diff > 0:
            for _ in range(diff):
                self._spawn_worker(stage_idx)
        # diff < 0: polling check at top of worker loop handles voluntary
        # retirement. Targeted-cancel for scale-to-zero is deferred (M2d).

    # --- Tasks -----------------------------------------------------------------

    async def _source_task(self) -> None:
        first = self._stages[0]
        try:
            try:
                async for item in self._source():
                    await first.queue.put(item)
                    # Notify the first stage's strategy that an item arrived
                    self._apply_delta(
                        0, first.strategy.on_enqueue(self._make_snapshot(0))
                    )
            except Exception as exc:
                # Source raised. Route to the error handler. If the handler
                # returns, source is treated as exhausted (drain proceeds).
                # If the handler raises, _handle_error captures and aborts.
                # Catches Exception (not BaseException) so CancelledError
                # propagates correctly — see DESIGN.md "Asyncio safety notes".
                await self._handle_error(SourceError(exception=exc))
        finally:
            first.queue.shutdown(immediate=False)  # type: ignore[attr-defined]
            first.input_drained = True
            self._source_finished = True
            self._maybe_cascade(0)
            self._check_done()

    async def _worker_task(self, stage_idx: int) -> None:
        s = self._stages[stage_idx]
        downstream = self._stages[stage_idx + 1] if stage_idx + 1 < self._n else None
        my_task = asyncio.current_task()
        try:
            # State 1 → 2: enter the per-worker CM. For plain transformers
            # this is a no-op wrapper; for CM factories it acquires
            # resources. The worker is NOT in idle_workers here, so it
            # cannot be targeted for cancellation during __aenter__ — see
            # DESIGN.md "Worker lifecycle states".
            async with s.factory() as user_fn:
                while True:
                    # Polling retirement check (per DESIGN.md)
                    if s.alive > s.target:
                        return

                    s.idle_workers.add(my_task)
                    try:
                        item = await s.queue.get()
                    except (asyncio.QueueShutDown, asyncio.CancelledError):
                        return
                    finally:
                        s.idle_workers.discard(my_task)

                    # Dequeue happened; busy++ before the user's transformer runs
                    s.busy += 1
                    # Notify strategy: util is now higher
                    self._apply_delta(
                        stage_idx,
                        s.strategy.on_dequeue(self._make_snapshot(stage_idx)),
                    )

                    try:
                        try:
                            result = await user_fn(item)
                        finally:
                            s.busy -= 1
                    except Exception as exc:
                        # Transformer raised. Route to handler. The item is
                        # dropped (no put downstream). Catches Exception
                        # only so CancelledError propagates — see DESIGN.md
                        # "Asyncio safety notes".
                        await self._handle_error(
                            TransformerError(
                                item=item, exception=exc, stage=s.name
                            )
                        )
                        continue

                    if downstream is not None:
                        try:
                            await downstream.queue.put(result)
                        except asyncio.QueueShutDown:
                            return
                        # Notify downstream stage that an item arrived
                        self._apply_delta(
                            stage_idx + 1,
                            downstream.strategy.on_enqueue(
                                self._make_snapshot(stage_idx + 1)
                            ),
                        )
            # __aexit__ runs here on normal loop exit
        finally:
            s.all_workers.discard(my_task)
            s.alive -= 1
            # Drain cascade: only fire downstream shutdown when this stage is
            # fully drained (its input was shut down upstream and we're the
            # last worker out). Voluntary retirement (target shrunk) doesn't
            # trigger cascade because the source / upstream is still running.
            self._maybe_cascade(stage_idx)
            self._check_done()

    # --- Entry point -----------------------------------------------------------

    async def execute(self) -> None:
        for stage_idx, s in enumerate(self._stages):
            for _ in range(s.target):
                self._spawn_worker(stage_idx)
        asyncio.create_task(self._source_task())
        # Edge case: if the chain is empty (no stages, no workers spawned),
        # check_done can be true immediately. Source still sets the flag.
        self._check_done()
        await self._done_event.wait()
        # If the error handler raised (or any other framework path called
        # _abort), surface that exception out of run().
        if self._abort_exception is not None:
            raise self._abort_exception


def flow(
    *stages: Callable[[Any], Any],
    default_scaling: ScalingStrategy | None = None,
    default_queue: AsyncQueueFactory | None = None,
    default_queue_size: int | None = None,
    on_error: ErrorHandler | None = None,
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

    # Validate each stage and normalise to (name, factory) tuples.
    # Two accepted shapes:
    #   - 1-arg async function — plain transformer; wrapped to factory shape
    #   - 0-arg callable (function or class) — CM factory; used as-is
    normalised: list[tuple[str, StageFactory]] = []
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

        sig = inspect.signature(stage)
        param_count = len(sig.parameters)
        name = getattr(stage, "__name__", repr(stage))

        if param_count == 1:
            # Plain transformer — must be async
            if not inspect.iscoroutinefunction(stage):
                raise TypeError(
                    f"transformer {name!r} is sync; wrap with sync_stage() "
                    f"to run it via asyncio.to_thread, or rewrite as async"
                )
            factory = _wrap_plain_as_factory(stage)
        elif param_count == 0:
            # CM factory — function (@asynccontextmanager) or class with
            # __aenter__ / __aexit__ and a no-arg constructor. We can't
            # verify the result is a CM until call time; the framework will
            # raise then if it isn't.
            factory = stage  # type: ignore[assignment]
        else:
            raise TypeError(
                f"stage {name!r} must take 0 args (CM factory) or 1 arg "
                f"(transformer); got {param_count}"
            )

        normalised.append((name, factory))

    # Auto-name with collision suffix (uses original name from above).
    counts: dict[str, int] = {}
    named: list[tuple[str, StageFactory]] = []
    for base_name, factory in normalised:
        counts[base_name] = counts.get(base_name, 0) + 1
        name = base_name if counts[base_name] == 1 else f"{base_name}_{counts[base_name]}"
        named.append((name, factory))

    return Flow(
        stages=named,
        default_scaling=default_scaling,
        default_queue=default_queue,
        default_queue_size=default_queue_size,
        on_error=on_error,
    )
