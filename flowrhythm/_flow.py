"""Flow runtime (M1 + M2a + M2b + M2c + M3 + M4 + M5 + M6).

A `Flow` holds a sequence of named stages plus per-stage and pipeline-wide
configuration. `Flow.run(source)` and `Flow.push()` construct a `_FlowRun`
(an internal per-execution object) and execute it. `run()` consumes a source
generator; `push()` yields a `PushHandle` that the user feeds via `send()`.

Sub-flows, routers, and dump() come in later milestones.
See `todos/implement-runtime.md`.

End-of-stream propagates via stdlib `asyncio.Queue.shutdown()` (see
DESIGN.md "EOF / drain cascade"). Worker pool sizing is driven by the
`ScalingStrategy` Protocol — `initial_workers()` at startup, then
`on_enqueue()` / `on_dequeue()` returning deltas during execution. See
DESIGN.md "Worker pool internals".
"""

import asyncio
import functools
import inspect
import sys
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from typing import Any, AsyncContextManager, AsyncGenerator, Awaitable, Callable

from flowrhythm._errors import (
    DropReason,
    Dropped,
    ErrorEvent,
    SourceError,
    TransformerError,
    default_handler,
)
from flowrhythm._queue import AsyncQueueFactory, AsyncQueueInterface, fifo_queue
from flowrhythm._scaling import FixedScaling, ScalingStrategy, StageSnapshot

# Type alias for the error handler: `async def handler(event) -> None`
ErrorHandler = Callable[[ErrorEvent], Awaitable[None]]


class Last:
    """Wrap a transformer's return value to mean "this is the absolute
    last item the pipeline should produce."

        async def fn(item):
            if item.is_terminator:
                return Last(process(item))
            return process(item)

    When a worker returns `Last(value)`:
      1. Upstream is killed (source's queue is shut down; cascades through
         stages 0..i).
      2. Idle sibling workers in stage i are cancelled.
      3. Busy sibling workers finish their current item; their results
         flow into the downstream queue first.
      4. Once all siblings have exited, this worker propagates `value`
         downstream — the absolute last item to enter the next queue.

    Source items that couldn't be pushed because the source's queue was
    shut down emit `Dropped(item, "<source>", DropReason.UPSTREAM_TERMINATED)`
    events to the error handler.

    See README "Stopping from inside a transformer" and DESIGN.md
    "Termination".
    """

    __slots__ = ("value",)

    def __init__(self, value: Any) -> None:
        self.value = value

    def __repr__(self) -> str:
        return f"Last({self.value!r})"


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
class _NamedStage:
    """Internal wrapper from `stage(target, name=...)`. Carries the explicit
    name override that `flow()` recognises — for transformer renaming or
    sub-flow prefixing."""

    target: Any
    name: str


def stage(target: Any, *, name: str) -> _NamedStage:
    """Override the auto-derived name of a transformer or assign an explicit
    prefix to a sub-flow.

        chain = flow(
            stage(normalize, name="parse"),     # rename a transformer
            stage(inner_flow, name="ingest"),   # name a sub-flow
            sink,
        )
    """
    if not isinstance(name, str) or not name:
        raise TypeError("stage(name=...) must be a non-empty string")
    return _NamedStage(target=target, name=name)


class Router:
    """A routing decision: classifier(item) → label, dispatched to arms.

    Construct via the `router()` factory — never directly. See README
    "Routing" for usage.
    """

    __slots__ = ("classifier", "arms", "default")

    def __init__(
        self,
        classifier: TransformerFn,
        arms: dict[str, Any],
        default: Any | None,
    ) -> None:
        self.classifier = classifier
        self.arms = arms
        self.default = default


def router(classifier: TransformerFn, /, **arms: Any) -> Router:
    """Construct a router for branching by label.

        router(classify, fast=quick, slow=heavy_path, default=passthrough)

    `classifier` is `async def (item) -> label`. Each keyword arg is an arm
    keyed by label; arms can be plain async functions, CM factories, or
    `Flow` instances. The reserved keyword `default=` is a fallback arm for
    labels that don't match any other arm. If no `default` is set and the
    classifier returns an unknown label, the item is dropped and the error
    handler receives a `Dropped(reason=DropReason.ROUTER_MISS)` event.
    """
    if not inspect.iscoroutinefunction(classifier):
        raise TypeError(
            "router() classifier must be an async function "
            "(async def classify(item) -> label)"
        )
    sig = inspect.signature(classifier)
    if len(sig.parameters) != 1:
        raise TypeError(
            "router() classifier must take exactly one argument (the item)"
        )

    default = arms.pop("default", None)
    if not arms and default is None:
        raise TypeError("router() requires at least one arm")

    return Router(classifier=classifier, arms=arms, default=default)


@dataclass
class _ClassifierHint:
    """Topology hint: this stage is a router classifier. Carries the
    user's classifier function plus the indices (in the parent's stages
    list) of each arm's first stage. The dispatch wrapper is built at
    `_FlowRun.__init__` time once arm-queue references exist."""

    classifier_fn: TransformerFn
    arm_first_stage_idx: dict[str, int]  # label → first-stage index
    default_first_stage_idx: int | None
    router_name: str  # for the Dropped event's `stage` field


@dataclass
class _ArmEndHint:
    """Topology hint: this stage is the last stage of a router arm. Its
    output goes to the merge point — the first stage right after the
    router's expansion in the parent. Resolved at the end of `flow()`
    once expansion is complete."""

    merge_stage_idx: int  # -1 if router is last in parent (output dropped)


def _reindex_topology_hint(hint: Any, offset: int) -> Any:
    """Shift a topology hint's absolute stage indices by `offset`.

    A `_ClassifierHint` / `_ArmEndHint` carries indices into a specific
    `_stages` list. When a Flow containing such hints is inlined into a
    parent (as a sub-flow), the indices need to point into the parent's
    expanded list — `offset` is the parent's `len(raw_items)` at the
    moment we start splicing the sub-flow in.
    """
    if isinstance(hint, _ClassifierHint):
        return _ClassifierHint(
            classifier_fn=hint.classifier_fn,
            arm_first_stage_idx={
                label: idx + offset
                for label, idx in hint.arm_first_stage_idx.items()
            },
            default_first_stage_idx=(
                hint.default_first_stage_idx + offset
                if hint.default_first_stage_idx is not None
                else None
            ),
            router_name=hint.router_name,
        )
    if isinstance(hint, _ArmEndHint):
        return _ArmEndHint(
            merge_stage_idx=(
                hint.merge_stage_idx + offset
                if hint.merge_stage_idx >= 0
                else -1
            )
        )
    return hint


def _resolve_subflow_stage_config(
    subflow: "Flow", sub_name: str
) -> dict[str, Any]:
    """Effective per-stage config for one sub-stage being inlined.

    Bakes the sub-flow's per-stage settings + its defaults (most specific
    wins) into a single dict. Built-in defaults are NOT baked — those fall
    through to the parent's `_resolve_config` so a parent default can still
    fill an unset key.

    See DESIGN.md "Sub-flow autonomy" for the rationale.
    """
    per_stage = subflow._stage_config.get(sub_name, {})
    sub_defaults = subflow._default_config
    effective: dict[str, Any] = {}
    for key in ("scaling", "queue", "queue_size"):
        if key in per_stage:
            effective[key] = per_stage[key]
        elif key in sub_defaults:
            effective[key] = sub_defaults[key]
    return effective


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

    # --- Topology (M8: router support) ---
    # Index of the destination stage in `_FlowRun._stages`. None = sink (no
    # downstream). For normal stages: i+1. For arm-end stages: the merge
    # point (the stage right after the router's expansion). For classifier
    # stages: None — the classifier dispatches via its wrapped user_fn.
    downstream_stage_idx: int | None = None
    # If set, this stage cascades to multiple downstream stages on exit
    # (used by the classifier — its arm queues all need shutdown when the
    # classifier finishes). Overrides downstream_stage_idx for cascade.
    cascade_targets: list[int] | None = None
    # Number of upstream sources still feeding this stage's input queue.
    # Default 1 (one previous stage). Merge-point stages set N = number of
    # contributing arm-ends; their queue is shut down only when all
    # contributors have signaled drain.
    pending_inputs: int = 1
    # If this stage is inside a router arm sub-graph, the merge index of
    # the enclosing arm — used by `_handle_last` to extend the kill range
    # to the merge (rather than just the immediate downstream), so a Last
    # firing from inside an arm correctly halts sibling arms too. None
    # for stages outside any arm. For nested arms the OUTERMOST merge
    # wins (first-set takes precedence in `_FlowRun.__init__`).
    arm_merge_idx: int | None = None


class Flow:
    """A pipeline of stages. Construct via the `flow()` factory."""

    def __init__(
        self,
        stages: list[tuple[str, StageFactory, Any | None]],
        default_scaling: ScalingStrategy | None = None,
        default_queue: AsyncQueueFactory | None = None,
        default_queue_size: int | None = None,
        on_error: ErrorHandler | None = None,
    ) -> None:
        # Each tuple is (name, factory, topology_hint). Hint is None for
        # plain linear stages, _ClassifierHint for router classifiers,
        # _ArmEndHint for arm-end stages.
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
        # Set while a run is in progress; used by drain() / stop() to
        # reach into the active _FlowRun. None when no run is active.
        self._current_run: "_FlowRun | None" = None

    @property
    def stage_names(self) -> list[str]:
        return [name for name, _, _ in self._stages]

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
        self._current_run = runner
        try:
            await runner.execute()
        finally:
            self._current_run = None

    @asynccontextmanager
    async def push(self) -> "AsyncGenerator[PushHandle, None]":
        """Activate the flow in push mode.

            async with chain.push() as h:
                await h.send(item)
            # on exit: complete() is called; pipeline drains; resources released.

        The yielded `PushHandle` is the only way to feed items in this mode;
        `send()` blocks under backpressure (queue full). Calling `complete()`
        explicitly inside the body is fine — subsequent `send()` raises.

        Use `chain.stop()` from another task to abort instead of draining.
        """
        if self._current_run is not None:
            raise RuntimeError("flow already has an active run")
        run = _FlowRun(self, source=None)
        self._current_run = run
        exec_task = asyncio.create_task(run.execute())
        handle = PushHandle(run)
        try:
            yield handle
        finally:
            try:
                await handle.complete()
                await exec_task
            finally:
                self._current_run = None

    async def drain(self) -> None:
        """Graceful shutdown of the in-progress run. No-op if no run is
        active. Returns when the run completes."""
        if self._current_run is not None:
            await self._current_run.drain()

    async def stop(self) -> None:
        """Immediate abort of the in-progress run. No-op if no run is
        active. In-flight items are dropped; per-worker resources are
        released via `__aexit__`. Returns when all workers have exited."""
        if self._current_run is not None:
            await self._current_run.stop()


class PushHandle:
    """Handle for pushing items into a flow activated via `Flow.push()`.

    Yielded from `chain.push()`'s async context manager. `send(item)` enqueues
    one item (blocks under backpressure when the first stage's queue is full);
    `complete()` signals end-of-stream (idempotent). `complete()` is called
    automatically on `async with` exit, so most code never calls it explicitly.

    See README "Push — `chain.push()` + `send()`" for usage and DESIGN.md
    "Drive modes" for the mode-symmetry rationale.
    """

    __slots__ = ("_run", "_completed")

    def __init__(self, run: "_FlowRun") -> None:
        self._run = run
        self._completed = False

    async def send(self, item: Any) -> None:
        """Enqueue `item`. Blocks if the first stage's queue is full.

        Raises RuntimeError if `complete()` has already been called.
        Propagates `asyncio.QueueShutDown` if the run was aborted via
        `Flow.stop()`.
        """
        if self._completed:
            raise RuntimeError("cannot send() after complete()")
        await self._run._push_item(item)

    async def complete(self) -> None:
        """Signal end-of-stream. Idempotent; subsequent `send()` raises.

        The pipeline drains naturally — items already pushed continue to
        the sink. To abort instead, call `Flow.stop()` from another task.
        """
        if self._completed:
            return
        self._completed = True
        self._run._mark_complete()


class _FlowRun:
    """Internal: per-run state and execution loop for a `Flow`.

    One `_FlowRun` is created per call to `Flow.run()` or `Flow.push()`. It
    owns all mutable state for that execution: the list of `_StageRuntime`
    (per-stage state), the source generator (None in push mode), and the
    run-completion event.

    Workers and the source task are spawned as fire-and-forget asyncio tasks
    (no `gather`, no `TaskGroup`). Completion is detected from runtime state
    via `_done_event`: set when source has finished AND all per-stage
    `alive` counters are zero. In push mode there's no source task — the
    `_source_finished` flag is set directly by `_mark_complete()`,
    `drain()`, or `stop()`.

    See DESIGN.md "Worker pool internals" for the design (worker lifecycle
    states, two-counter pool sizing, drain cascade) and "Run completion:
    state-driven, not task-driven" for the wait-for-done mechanism.
    """

    def __init__(
        self,
        flow: Flow,
        source: Callable[[], AsyncGenerator[Any, None]] | None,
    ) -> None:
        self._source = source

        # Build per-stage runtime objects. All scattered state for stage i
        # lives in self._stages[i] — see _StageRuntime docstring and
        # DESIGN.md "Per-stage state organization".
        self._stages: list[_StageRuntime] = []
        for name, factory, _hint in flow._stages:
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

        # Wire topology (M8). For each stage:
        #   - Default: linear chain (downstream = next stage; pending_inputs = 1)
        #   - Classifier: replace factory with dispatch wrapper; cascade_targets
        #     covers all arm queues; downstream_stage_idx = None.
        #   - Arm-end: downstream_stage_idx = merge_stage_idx (or None if router
        #     was last); contributes to merge's pending_inputs.
        n = len(self._stages)
        # Default linear wiring for every stage
        for i, s in enumerate(self._stages):
            s.downstream_stage_idx = i + 1 if i + 1 < n else None

        for i, (_, _, hint) in enumerate(flow._stages):
            if isinstance(hint, _ClassifierHint):
                s = self._stages[i]
                # Build dispatch wrapper that closes over arm-queue refs
                wrapped = self._make_classifier_wrapper(hint)
                s.factory = _wrap_plain_as_factory(wrapped)
                s.downstream_stage_idx = None  # dispatched via wrapper
                # Cascade: when classifier exits, shut down all arm queues
                cascade = list(hint.arm_first_stage_idx.values())
                if hint.default_first_stage_idx is not None:
                    cascade.append(hint.default_first_stage_idx)
                s.cascade_targets = cascade
            elif isinstance(hint, _ArmEndHint):
                s = self._stages[i]
                merge_idx = hint.merge_stage_idx
                s.downstream_stage_idx = merge_idx if merge_idx >= 0 else None
                # Contribute to merge's pending_inputs counter
                if merge_idx >= 0:
                    self._stages[merge_idx].pending_inputs += 1

        # Merge stages: subtract 1 (we initialised pending_inputs=1 default,
        # then +1 per contributing arm-end above, so a merge with N arms
        # ends up at 1+N. The "1" represented "previous stage in linear
        # order" which doesn't apply to merge stages — their input comes
        # ONLY from arms, not from a previous-in-list stage). Subtract it
        # to leave pending_inputs = N.
        merge_indices: set[int] = set()
        for _, _, hint in flow._stages:
            if isinstance(hint, _ArmEndHint) and hint.merge_stage_idx >= 0:
                merge_indices.add(hint.merge_stage_idx)
        for idx in merge_indices:
            self._stages[idx].pending_inputs -= 1

        # Annotate each in-arm stage with its enclosing arm's merge index.
        # Used by `_handle_last` to extend the Last cascade's kill range
        # to the merge — without this, a Last firing from the middle of
        # a multi-stage arm only kills its immediate downstream chain,
        # leaving sibling arms running and injecting items past Last.
        # Iteration order matters: outer classifiers come before inner
        # in stage-index order, so first-wins (don't overwrite already-
        # set values) yields the OUTERMOST arm_merge_idx for nested arms.
        for i, (_, _, hint) in enumerate(flow._stages):
            if not isinstance(hint, _ClassifierHint):
                continue
            arm_starts = sorted(hint.arm_first_stage_idx.values())
            if hint.default_first_stage_idx is not None:
                arm_starts = sorted(arm_starts + [hint.default_first_stage_idx])
            # Find merge_idx by inspecting any arm-end's _ArmEndHint
            merge_idx: int | None = None
            for arm_first in arm_starts:
                for k in range(arm_first, len(flow._stages)):
                    _, _, h = flow._stages[k]
                    if isinstance(h, _ArmEndHint):
                        if h.merge_stage_idx >= 0:
                            merge_idx = h.merge_stage_idx
                        break
                if merge_idx is not None:
                    break
            if merge_idx is None:
                continue  # router with no merge (arm outputs sink)
            # Each arm spans [arm_starts[j], arm_starts[j+1] - 1] (or
            # merge_idx - 1 for the last arm). Mark every stage in that
            # range with arm_merge_idx — first-wins (outer arms override
            # inner reassignments).
            for j, arm_first in enumerate(arm_starts):
                arm_last = (
                    arm_starts[j + 1] - 1
                    if j + 1 < len(arm_starts)
                    else merge_idx - 1
                )
                for k in range(arm_first, arm_last + 1):
                    if self._stages[k].arm_merge_idx is None:
                        self._stages[k].arm_merge_idx = merge_idx

        # Error handling — observer pattern: handler is called with typed
        # events. If the handler raises, we log and continue (handler is
        # not a flow-control mechanism; use Flow.stop() to abort externally).
        # See DESIGN.md "Error handler is observer-only".
        self._handler: ErrorHandler = flow._error_handler

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

    async def _handle_last(self, stage_idx: int, last_value: Any) -> None:
        """Initiate the Last(value) cascade.

        Topology-aware: the kill boundary is `downstream_stage_idx` (the
        merge for arm-end stages, `stage_idx + 1` for normal stages, or
        `_n` if the firing stage is the last in the pipeline).

        Steps:
          1. Shutdown every queue in `[0, downstream_stage_idx)` so all
             upstream paths stop feeding the destination — for routers
             this includes the classifier AND every sibling arm, not just
             the firing arm.
          2. Cancel idle workers in `stage_idx` (state 3 — safe). Sibling
             arms' idle workers wake on `QueueShutDown` from the same
             shutdown call; cancelling them too is unnecessary.
          3. Poll until every stage in the kill range has fully drained
             (alive == 0 for non-self stages; alive == 1 for self — just
             this initiator left). `await asyncio.sleep(0)` yields one
             event-loop tick per iteration so worker finally blocks can
             run and decrement `alive`.
          4. Put `last_value` to the downstream queue. By this point all
             upstream items have flowed through, so `last_value` is the
             last item to enter `downstream_stage_idx`'s queue.
        """
        s = self._stages[stage_idx]
        my_task = asyncio.current_task()
        # Compute the kill range. For stages inside a router arm, extend
        # to the enclosing arm's merge — otherwise sibling arms would
        # keep injecting items past the Last value. For stages outside
        # any arm, fall back to `downstream_stage_idx` (or `_n` if this
        # is the last stage).
        if s.arm_merge_idx is not None:
            end_exclusive = s.arm_merge_idx
        elif s.downstream_stage_idx is not None:
            end_exclusive = s.downstream_stage_idx
        else:
            end_exclusive = self._n

        # If we're inside an arm and the kill range extends past our own
        # immediate downstream, walk forward along OUR chain (downstream →
        # downstream → ...) up to but not including the merge — those
        # stages must stay alive so the Last value can propagate. Stages
        # outside our chain (sibling arms, classifier, upstream) get
        # killed normally.
        my_chain: set[int] = set()
        if s.arm_merge_idx is not None and s.downstream_stage_idx is not None:
            cur: int | None = s.downstream_stage_idx
            while cur is not None and cur < end_exclusive:
                my_chain.add(cur)
                cur = self._stages[cur].downstream_stage_idx

        # Step 1: kill every upstream path that could feed the destination,
        # except this initiator's own downstream chain.
        for i in range(end_exclusive):
            if i in my_chain:
                continue
            try:
                self._stages[i].queue.shutdown(immediate=False)  # type: ignore[attr-defined]
            except Exception:
                pass  # already shut down
            self._stages[i].input_drained = True

        # Step 2: cancel idle siblings in MY stage. (Other stages' idle
        # workers wake on `QueueShutDown` from step 1 — no extra cancel
        # needed.)
        for sibling in list(s.idle_workers):
            if sibling is not my_task:
                sibling.cancel()

        # Step 3: poll until every killed stage has fully drained.
        while True:
            done = True
            for i in range(end_exclusive):
                if i in my_chain:
                    continue  # chain stays alive to deliver Last
                expected = 1 if i == stage_idx else 0
                if self._stages[i].alive > expected:
                    done = False
                    break
            if done:
                break
            await asyncio.sleep(0)

        # Step 4: put last_value to the downstream queue (if any).
        downstream_idx = s.downstream_stage_idx
        if downstream_idx is not None:
            downstream = self._stages[downstream_idx]
            try:
                await downstream.queue.put(last_value)
            except asyncio.QueueShutDown:
                return  # downstream already torn down (e.g., stop())
            self._apply_delta(
                downstream_idx,
                downstream.strategy.on_enqueue(self._make_snapshot(downstream_idx)),
            )

    async def _push_item(self, item: Any) -> None:
        """Called by `PushHandle.send()`. Enqueue + notify the strategy."""
        first = self._stages[0]
        await first.queue.put(item)
        self._apply_delta(0, first.strategy.on_enqueue(self._make_snapshot(0)))

    def _mark_complete(self) -> None:
        """Called by `PushHandle.complete()`. Signal "no more items will
        come"; let the drain cascade run.

        In push mode there's no source_task whose `finally` flips
        `_source_finished`; we set it here. Idempotent if the queue is
        already shut down.
        """
        try:
            self._stages[0].queue.shutdown(immediate=False)  # type: ignore[attr-defined]
        except Exception:
            pass
        self._stages[0].input_drained = True
        self._source_finished = True
        self._maybe_cascade(0)
        self._check_done()

    async def drain(self) -> None:
        """Initiate graceful drain from outside the run.

        Source's queue is shut down; the source's next put raises and the
        source ends. Items in flight finish through the cascade. Returns
        when the run completes.
        """
        if self._source_finished:
            await self._done_event.wait()
            return
        try:
            self._stages[0].queue.shutdown(immediate=False)  # type: ignore[attr-defined]
        except Exception:
            pass  # already shut down
        self._stages[0].input_drained = True
        if self._source is None:
            # Push mode: no source_task to flip _source_finished, so done_event
            # would never fire. Mark it here and let the cascade roll.
            self._source_finished = True
            self._check_done()
        await self._done_event.wait()

    async def stop(self) -> None:
        """Initiate immediate abort.

        Every queue is shut down with `immediate=True` (in-flight items in
        queues are dropped). Every worker task is cancelled — including
        busy ones (state 4 — `processing`); their `__aexit__` still runs
        because cancellation occurs inside the `async with` body. Returns
        when all workers have exited.
        """
        for s in self._stages:
            try:
                s.queue.shutdown(immediate=True)  # type: ignore[attr-defined]
            except Exception:
                pass
            s.input_drained = True
            for task in list(s.all_workers):
                task.cancel()
        if self._source is None:
            # Push mode: no source_task to flip _source_finished — see drain().
            self._source_finished = True
            self._check_done()
        await self._done_event.wait()

    async def _handle_error(self, event: ErrorEvent) -> None:
        """Call the user's error handler with `event`.

        The handler is an **observer**, not a controller — see DESIGN.md
        "Error handler is observer-only". If the handler raises, the
        exception is logged to stderr; the failed item is dropped (already
        was, since it was never put downstream); the pipeline continues.
        To stop the run, use `Flow.stop()` from outside the handler.

        Catches `Exception` only (NOT `BaseException`) so cooperative
        cancellation propagates — see DESIGN.md "Asyncio safety notes".
        """
        try:
            await self._handler(event)
        except Exception as exc:
            print(
                f"[flowrhythm] error handler raised {type(exc).__name__}: {exc} "
                f"while processing {type(event).__name__}; "
                f"item dropped, pipeline continues",
                file=sys.stderr,
            )

    def _maybe_cascade(self, stage_idx: int) -> None:
        """If stage `stage_idx` is fully drained, signal its destination(s).

        Called from source_task's and worker_task's `finally`. The two
        callers can fire in either order — whichever runs last (sets the
        last condition true) triggers the cascade. Recursion handles the
        case where stages were already empty.

        For routers (M8): a classifier stage cascades to ALL arm queues
        via `cascade_targets`. An arm-end signals the merge point, which
        only shuts down once all arm-ends have signaled.
        """
        s = self._stages[stage_idx]
        if not (s.alive == 0 and s.input_drained):
            return

        # Multi-target cascade (classifier → all arm queues)
        if s.cascade_targets is not None:
            for target_idx in s.cascade_targets:
                self._signal_input_drain(target_idx)
            return

        # Single-target cascade (normal stage or arm-end → merge)
        next_idx = s.downstream_stage_idx
        if next_idx is None:
            return
        self._signal_input_drain(next_idx)

    def _signal_input_drain(self, target_idx: int) -> None:
        """One upstream source has finished feeding `target_idx`. Decrement
        pending_inputs; when it hits 0, shut down the queue and cascade.

        For most stages pending_inputs starts at 1, so the first signal
        immediately shuts down. Merge stages start at N (one per arm-end),
        so they wait for all arms before draining.
        """
        target = self._stages[target_idx]
        if target.input_drained:
            return  # already shut down (idempotent)
        target.pending_inputs -= 1
        if target.pending_inputs > 0:
            return  # still waiting for other contributors
        try:
            target.queue.shutdown(immediate=False)  # type: ignore[attr-defined]
        except Exception:
            pass
        target.input_drained = True
        self._maybe_cascade(target_idx)

    def _make_classifier_wrapper(
        self, hint: _ClassifierHint
    ) -> "Callable[[Any], Awaitable[Any]]":
        """Build the classifier's per-item wrapper.

        Calls the user's classifier(item) → label, then dispatches `item`
        (NOT the label) to the matching arm's input queue. Unknown labels
        with no `default` → emit `Dropped(reason=ROUTER_MISS)`. Returns
        None — the worker loop skips its "put downstream" step because the
        classifier's `downstream_stage_idx` is None.
        """
        arm_first_stage_idx = hint.arm_first_stage_idx
        default_idx = hint.default_first_stage_idx
        classifier_fn = hint.classifier_fn
        router_name = hint.router_name

        async def dispatch(item: Any) -> None:
            label = await classifier_fn(item)
            if isinstance(label, Last):
                # Classifiers must return a label, not Last. To terminate
                # the pipeline, return a label and have the corresponding
                # arm return Last(value), or call Flow.stop() externally.
                raise TypeError(
                    f"router classifier {router_name!r} returned "
                    f"Last(value); classifiers must return a label string"
                )
            if label in arm_first_stage_idx:
                target_idx = arm_first_stage_idx[label]
            elif default_idx is not None:
                target_idx = default_idx
            else:
                await self._handle_error(
                    Dropped(
                        item=item,
                        stage=router_name,
                        reason=DropReason.ROUTER_MISS,
                    )
                )
                return

            target = self._stages[target_idx]
            try:
                await target.queue.put(item)
            except asyncio.QueueShutDown:
                return  # arm torn down — silently drop
            self._apply_delta(
                target_idx,
                target.strategy.on_enqueue(self._make_snapshot(target_idx)),
            )

        return dispatch

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
        src_gen = self._source()
        try:
            try:
                async for item in src_gen:
                    try:
                        await first.queue.put(item)
                    except asyncio.QueueShutDown:
                        # Framework shut down the source's queue (drain(),
                        # stop(), or Last() cascade). The item we held is
                        # dropped; report it as a Dropped event with the
                        # UPSTREAM_TERMINATED reason. Then exit the loop.
                        await self._handle_error(
                            Dropped(
                                item=item,
                                stage="<source>",
                                reason=DropReason.UPSTREAM_TERMINATED,
                            )
                        )
                        break
                    # Notify the first stage's strategy that an item arrived
                    self._apply_delta(
                        0, first.strategy.on_enqueue(self._make_snapshot(0))
                    )
            except Exception as exc:
                # Source generator itself raised. Route to the handler.
                # Catches Exception (not BaseException) so CancelledError
                # propagates correctly — see DESIGN.md "Asyncio safety notes".
                await self._handle_error(SourceError(exception=exc))
        finally:
            # Best-effort: close the generator so any try/finally cleanup
            # inside the user's source runs. Errors during aclose are
            # swallowed — we're already in cleanup.
            try:
                await src_gen.aclose()
            except Exception:
                pass
            try:
                first.queue.shutdown(immediate=False)  # type: ignore[attr-defined]
            except Exception:
                pass  # already shut down (drain/stop/Last)
            first.input_drained = True
            self._source_finished = True
            self._maybe_cascade(0)
            self._check_done()

    async def _worker_task(self, stage_idx: int) -> None:
        s = self._stages[stage_idx]
        # Topology-aware downstream: for normal stages it's the next stage
        # in the list; for arm-end stages it's the merge point; for the
        # classifier it's None (the dispatch wrapper handles routing).
        downstream_idx = s.downstream_stage_idx
        downstream = (
            self._stages[downstream_idx] if downstream_idx is not None else None
        )
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

                    if isinstance(result, Last):
                        # Initiate the Last cascade: kill upstream, cancel
                        # idle siblings, wait for busy siblings, then
                        # propagate the wrapped value as the LAST item to
                        # enter the downstream queue.
                        await self._handle_last(stage_idx, result.value)
                        return

                    if downstream is not None:
                        try:
                            await downstream.queue.put(result)
                        except asyncio.QueueShutDown:
                            return
                        # Notify downstream stage that an item arrived
                        self._apply_delta(
                            downstream_idx,  # type: ignore[arg-type]
                            downstream.strategy.on_enqueue(
                                self._make_snapshot(downstream_idx)  # type: ignore[arg-type]
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
            # Any in-progress Last cascade is awaiting via polling — see
            # `_handle_last`; the alive decrement above is what advances it.
            self._maybe_cascade(stage_idx)
            self._check_done()

    # --- Entry point -----------------------------------------------------------

    async def execute(self) -> None:
        for stage_idx, s in enumerate(self._stages):
            for _ in range(s.target):
                self._spawn_worker(stage_idx)
        if self._source is not None:
            asyncio.create_task(self._source_task())
        # Push mode: no source task; _source_finished is set by
        # _mark_complete() / drain() / stop() instead.
        self._check_done()
        await self._done_event.wait()


def flow(
    *stages: Any,
    default_scaling: ScalingStrategy | None = None,
    default_queue: AsyncQueueFactory | None = None,
    default_queue_size: int | None = None,
    on_error: ErrorHandler | None = None,
) -> Flow:
    """Construct a flow from a sequence of stages.

    Each stage is one of:
      - 1-arg async function (plain transformer) — called per item
      - 0-arg callable returning an `AsyncContextManager` (CM factory)
      - A `Flow` (sub-flow) — its stages are inlined into this flow's graph
        with names prefixed by `_subflow_N` (or by the explicit prefix from
        `stage(inner, name="...")`)
      - `stage(target, name="...")` — wraps any of the above with an
        explicit name override

    The last stage acts as the sink when run autonomously — its return
    value is dropped.

    Optional `default_*` kwargs are shorthand for `configure_default(...)`.

    Validation rules:
      - Async generators are rejected — they are sources, not stages.
      - Sync functions are rejected — wrap with `sync_stage()` or
        `asyncio.to_thread`.
      - Each transformer must take exactly one argument.

    Stage names auto-derive from function names; collisions get a numeric
    suffix (`normalize`, `normalize_2`, ...). Sub-flow inlining and
    autonomy rules: see DESIGN.md "Composition (sub-flows)".
    """
    if not stages:
        raise TypeError("flow() requires at least one stage")

    # Phase 1: walk args, expand sub-flows + routers, collect raw items.
    # Each entry: (base_name, factory, topology_hint, inlined_config_or_None).
    raw_items: list[
        tuple[str, StageFactory, Any | None, dict[str, Any] | None]
    ] = []
    subflow_counter = 0
    router_counter = 0
    # Pending arm-end resolution: list of (router_id, arm_end_indices_list).
    # After all args are processed, each router's _ArmEndHint stages get
    # merge_stage_idx = (last arm-end index) + 1, or -1 if router was last.
    pending_router_arm_ends: list[list[int]] = []

    for arg in stages:
        # Unwrap stage(target, name=...) wrapper
        if isinstance(arg, _NamedStage):
            explicit_name = arg.name
            target = arg.target
        else:
            explicit_name = None
            target = arg

        # Router expansion: classifier stage + per-arm sub-graphs
        if isinstance(target, Router):
            router_name = (
                explicit_name
                if explicit_name is not None
                else f"_router_{router_counter}"
            )
            router_counter += 1

            # 1. Reserve the classifier stage slot. Factory is a placeholder;
            #    _FlowRun.__init__ replaces it with the dispatch wrapper
            #    once arm-queue references exist.
            classifier_hint = _ClassifierHint(
                classifier_fn=target.classifier,
                arm_first_stage_idx={},  # filled below
                default_first_stage_idx=None,
                router_name=router_name,
            )
            raw_items.append(
                (router_name, _wrap_plain_as_factory(target.classifier),
                 classifier_hint, None)
            )

            # 2. Expand each arm. Track each arm's last-stage index.
            arm_end_indices: list[int] = []

            def _expand_arm_into_raw(label: str, arm_target: Any) -> None:
                arm_first = len(raw_items)
                arm_prefix = f"{router_name}.{label}"

                # Arm can be a Flow (sub-flow inlining) or a callable.
                if isinstance(arm_target, Flow):
                    # Splice the arm Flow's stages into raw_items, re-indexing
                    # any internal topology hints by the current offset.
                    # Terminal stages of the inner Flow (the ones that would
                    # produce items to consumers) become outer arm-ends.
                    inner_offset = len(raw_items)
                    inner_start = inner_offset
                    inner_end = inner_offset + len(arm_target._stages) - 1

                    for sub_name, sub_factory, sub_hint in arm_target._stages:
                        effective = _resolve_subflow_stage_config(
                            arm_target, sub_name
                        )
                        adjusted = (
                            _reindex_topology_hint(sub_hint, inner_offset)
                            if sub_hint is not None
                            else None
                        )
                        raw_items.append(
                            (
                                f"{arm_prefix}.{sub_name}",
                                sub_factory,
                                adjusted,
                                effective or None,
                            )
                        )

                    # Identify the terminal stages of the inner Flow — they
                    # collectively become this outer arm's "ends" and feed
                    # the outer merge. Cases:
                    #  - Last stage hint=None: single terminal (the last).
                    #  - Last stage is an inner-arm-end with merge=-1: that
                    #    means the inner router was the last thing and its
                    #    arm-ends had nowhere to go. Promote ALL such
                    #    inner-arm-ends to outer arm-ends.
                    #  - Inner has its own merge (e.g. flow(router(...), collect)):
                    #    the last stage is `collect` with hint=None — single
                    #    terminal, same as the linear case.
                    last_idx = inner_end
                    last_hint = raw_items[last_idx][2]
                    inner_terminals: list[int] = []
                    if isinstance(last_hint, _ArmEndHint) and last_hint.merge_stage_idx == -1:
                        # Promote every inner-arm-end with merge=-1
                        for k in range(inner_start, inner_end + 1):
                            h = raw_items[k][2]
                            if isinstance(h, _ArmEndHint) and h.merge_stage_idx == -1:
                                inner_terminals.append(k)
                    else:
                        inner_terminals.append(last_idx)

                    # Each terminal becomes an outer arm-end. Overwrite its
                    # hint slot with a fresh _ArmEndHint (merge=-1 placeholder
                    # resolved by the post-args pass). For terminals that
                    # already had their own _ArmEndHint(-1), this transfers
                    # ownership from the inner router to the outer.
                    for term_idx in inner_terminals:
                        bn, fac, _, eff = raw_items[term_idx]
                        raw_items[term_idx] = (
                            bn,
                            fac,
                            _ArmEndHint(merge_stage_idx=-1),
                            eff,
                        )
                        arm_end_indices.append(term_idx)
                    return inner_start
                elif callable(arm_target):
                    arm_sig = inspect.signature(arm_target)
                    arm_param_count = len(arm_sig.parameters)
                    if arm_param_count == 1:
                        if not inspect.iscoroutinefunction(arm_target):
                            raise TypeError(
                                f"router arm {label!r} is sync; wrap with "
                                f"sync_stage() or rewrite as async"
                            )
                        arm_factory = _wrap_plain_as_factory(arm_target)
                    elif arm_param_count == 0:
                        arm_factory = arm_target
                    else:
                        raise TypeError(
                            f"router arm {label!r} must take 0 args (CM "
                            f"factory) or 1 arg (transformer); got "
                            f"{arm_param_count}"
                        )
                    raw_items.append((arm_prefix, arm_factory, None, None))
                else:
                    raise TypeError(
                        f"router arm {label!r} must be callable or a Flow, "
                        f"got {type(arm_target).__name__}"
                    )

                # Record this arm's last-stage index
                arm_end = len(raw_items) - 1
                arm_end_indices.append(arm_end)
                return arm_first

            for label, arm_target in target.arms.items():
                first_idx = _expand_arm_into_raw(label, arm_target)
                classifier_hint.arm_first_stage_idx[label] = first_idx

            if target.default is not None:
                first_idx = _expand_arm_into_raw("default", target.default)
                classifier_hint.default_first_stage_idx = first_idx

            # 3. Tag each arm-end with _ArmEndHint (merge_stage_idx
            #    resolved after all args are processed).
            for end_idx in arm_end_indices:
                base_name, factory, _, eff = raw_items[end_idx]
                raw_items[end_idx] = (
                    base_name,
                    factory,
                    _ArmEndHint(merge_stage_idx=-1),
                    eff,
                )
            pending_router_arm_ends.append(arm_end_indices)
            continue

        # Sub-flow: inline its stages with prefixed names + resolved config.
        # If the sub-flow contains routers, re-index its hints so absolute
        # stage indices point into THIS parent's expanded list.
        if isinstance(target, Flow):
            prefix = (
                explicit_name
                if explicit_name is not None
                else f"_subflow_{subflow_counter}"
            )
            subflow_counter += 1
            base_offset = len(raw_items)
            for sub_name, sub_factory, sub_hint in target._stages:
                effective = _resolve_subflow_stage_config(target, sub_name)
                adjusted_hint = (
                    _reindex_topology_hint(sub_hint, base_offset)
                    if sub_hint is not None
                    else None
                )
                raw_items.append(
                    (
                        f"{prefix}.{sub_name}",
                        sub_factory,
                        adjusted_hint,
                        effective or None,
                    )
                )
            continue

        # Plain transformer / CM factory
        if inspect.isasyncgenfunction(target):
            raise TypeError(
                f"flow() does not accept async generators "
                f"(got {target.__name__!r}); pass sources to run() instead"
            )
        if not callable(target):
            raise TypeError(
                f"flow() arguments must be callable or a Flow, got "
                f"{type(target).__name__}"
            )

        sig = inspect.signature(target)
        param_count = len(sig.parameters)
        auto_name = getattr(target, "__name__", repr(target))
        base_name = explicit_name if explicit_name is not None else auto_name

        if param_count == 1:
            if not inspect.iscoroutinefunction(target):
                raise TypeError(
                    f"transformer {auto_name!r} is sync; wrap with sync_stage() "
                    f"to run it via asyncio.to_thread, or rewrite as async"
                )
            factory = _wrap_plain_as_factory(target)
        elif param_count == 0:
            factory = target  # type: ignore[assignment]
        else:
            raise TypeError(
                f"stage {auto_name!r} must take 0 args (CM factory) or 1 arg "
                f"(transformer); got {param_count}"
            )

        raw_items.append((base_name, factory, None, None))

    # Resolve arm-end merge points. The merge index for a router is the
    # first stage AFTER its last arm-end; -1 if router was last in args.
    for arm_end_indices in pending_router_arm_ends:
        last_arm_end = max(arm_end_indices)
        merge_idx = last_arm_end + 1 if last_arm_end + 1 < len(raw_items) else -1
        for end_idx in arm_end_indices:
            base_name, factory, hint, eff = raw_items[end_idx]
            assert isinstance(hint, _ArmEndHint)
            hint.merge_stage_idx = merge_idx

    # Phase 2: collision-suffix names
    counts: dict[str, int] = {}
    final_items: list[
        tuple[str, StageFactory, Any | None, dict[str, Any] | None]
    ] = []
    for base_name, factory, hint, effective in raw_items:
        counts[base_name] = counts.get(base_name, 0) + 1
        name = (
            base_name
            if counts[base_name] == 1
            else f"{base_name}_{counts[base_name]}"
        )
        final_items.append((name, factory, hint, effective))

    # Phase 3: construct Flow, then bake inlined sub-flow configs into the
    # parent's per-stage config map. Parent's later configure() calls
    # overwrite per key (most-specific wins, per key — not per stage).
    flow_obj = Flow(
        stages=[(n, f, h) for n, f, h, _ in final_items],
        default_scaling=default_scaling,
        default_queue=default_queue,
        default_queue_size=default_queue_size,
        on_error=on_error,
    )
    for name, _, _, effective in final_items:
        if effective:
            flow_obj._stage_config[name] = dict(effective)

    return flow_obj
