# Implement the runtime

**Status:** planned
**Updated:** 2026-04-26

## Motivation

The README documents the `flow()` function, `Flow`/`Router`/`PushHandle` design with sub-pipeline inlining, typed error events, three drive modes, and `Last(value)` termination. The runtime needs to be built to match.

This is a from-scratch implementation. The legacy `Builder` DSL has been deleted; the kept primitives (`_queue.py`, `_scaling.py`, `_utilizationscaling.py`) are the foundation.

See [`DESIGN.md`](../DESIGN.md) for the design that this implements.

## Strategy

**Outside-in vertical slices.** One small primitive milestone first (M0 — verify stdlib `asyncio.Queue.shutdown()` behaves the way we need); everything else lands as thin end-to-end slices, each with a smoke test. After every milestone, the package is in a runnable state and tests pass — we can stop at any milestone with a working subset.

**Test-first per milestone.** Write the smoke test for the milestone first, watch it fail, then implement the minimum needed to pass.

**One module per concept** as they emerge — start with `_flow.py` for the runtime; add `_router.py`, `_subflow.py`, etc. when those slices land.

## Milestones

### M0 — Verify stdlib `Queue.shutdown()` covers our needs

Goal: confirm that **stdlib `asyncio.Queue.shutdown()` (Python 3.13+)** does what every later milestone needs. This is mostly verification — not implementation.

Key insight from research: `asyncio.Queue` got `shutdown(immediate=False)` and `shutdown(immediate=True)` in Python 3.13 — covering both graceful drain (`Flow.drain()`) and abort (`Flow.stop()`). LIFO and Priority variants inherit it. We bumped `requires-python = ">=3.13"` in `pyproject.toml`. No custom queue subclass needed.

- [x] Update existing queue factories (`fifo_queue`, `lifo_queue`, `priority_queue`) to default `maxsize=1`. Done — `_queue.py` defaults `maxsize=1`; `AsyncQueueInterface` Protocol gained `shutdown(immediate)` method; module docstring added pointing to DESIGN.md.
- [x] Tests covering the stdlib behavior we depend on. Done — `tests/test_queue.py` now has 30 tests (was 1), parametrised across all three queue types, covering:
  - graceful shutdown drains remaining items then raises `QueueShutDown`
  - put-after-shutdown raises immediately
  - blocked `put()` callers unblock with `QueueShutDown` on shutdown
  - immediate shutdown unblocks all waiting workers
  - immediate shutdown drops queued items
  - default `maxsize=1` blocks the second put
- [x] Fixed Makefile `lint` target — was using stale `ruff flowrhythm tests` syntax; updated to `uv run ruff check flowrhythm tests`. `make lint` and `make cov` both pass; coverage at 98%.

### M1 — Minimum linear flow

Goal: `flow(t1, t2).run(source)` works for plain async functions, single-worker stages, no errors, no scaling complications.

- [x] `flow(*stages)` constructor — `flowrhythm/_flow.py`
- [x] Stage role detection for plain async functions only (CM factory deferred to M3, Router to M8, sub-flow to M7)
- [x] Reject async generators in `flow()` args with clear `TypeError`
- [x] Reject sync functions in `flow()` args; non-callables; wrong arity
- [x] Auto-name stages from function names; numeric suffix on collisions
- [x] `Flow.run(source)` — bounded mode; consume source generator with one task
- [x] `Flow.run(source)` — reject already-instantiated generators with helpful error
- [x] `Flow.run(source)` — also reject async functions that aren't generators
- [x] Internal `_start_and_join()` — single private method spawns source-task + per-stage worker tasks, awaits all
- [x] Per-stage worker pool with one worker (no scaling strategy yet)
- [x] EOF cascade for the linear case (source ends → `shutdown(immediate=False)` on stage[0] input → worker drains, exits → finally closes downstream → cascade to sink)
- [x] `__init__.py` exports: `flow`, `Flow`, plus the M0 queue factories
- [x] Smoke test: 3-stage linear flow processes 100 items end-to-end, items arrive at sink in order. Plus 13 other tests covering single-stage, two-stage, empty source, validation rules, naming.

Coverage: 97% on `_flow.py`, 98% overall. 45 tests pass in 0.03s. `make lint` clean.

### M2a — Multi-worker pool + drain cascade

Goal: a stage can have N workers (configured at construction); the EOF cascade works correctly with N>1.

- [x] Refactor `_start_and_join()` to spawn N worker tasks per stage. Hidden `_workers_per_stage` kwarg on `flow()` for M2a-only testing; M2b replaces it with the public `configure()` API.
- [x] Per-stage alive-worker counter (plain list of ints — asyncio is single-threaded cooperative). When count → 0, the worker that decremented closes the next queue.
- [x] Workers exit naturally on `QueueShutDown`; `finally` block decrements the counter and (only when last) shuts down downstream.
- [x] Smoke test: 4 workers per stage, 100 items, all arrive (sorted to handle out-of-order delivery)
- [x] Test: parallelism observable — `max_in_flight > 1` with `_workers_per_stage=4`
- [x] Test: drain cascade with N>1 doesn't hang; `run()` returns; items not lost
- [x] Test: empty source with 8 workers per stage — all workers exit promptly
- [x] Test: default behavior (no kwarg) → single worker per stage, M1 ordering preserved

Coverage: 97% on `_flow.py`. 50 tests pass in 0.07s.

### M2b — Configure API

Goal: users can specify per-stage scaling/queue and pipeline-wide defaults; the M2a worker pool consumes those settings.

- [x] `Flow.configure(name, *, scaling=None, queue=None, queue_size=None)` — per-stage tuning; supports namespaced names (storage is just a dict keyed by name, so sub-flow paths like `"inner.decode"` work without further changes)
- [x] `Flow.configure_default(*, scaling=None, queue=None, queue_size=None)` — pipeline-wide defaults
- [x] `flow(*stages, default_scaling=None, default_queue=None, default_queue_size=None)` — constructor kwargs equivalent to `configure_default`
- [x] At `run()` time, resolve effective config per stage: per-stage override → pipeline default → built-in default (`FixedScaling(workers=1)`, `fifo_queue`, `maxsize=1`)
- [x] Updated `FixedScaling` — now requires `workers: int >= 1` (raises `ValueError` on 0; pointer to `UtilizationScaling` for scale-to-zero); added `initial_workers()` method
- [x] Added `initial_workers()` method to `UtilizationScaling` (returns `self.min_workers`); added it to the `ScalingStrategy` Protocol so the runtime can query worker counts uniformly
- [x] Removed the M2a-only `_workers_per_stage` hidden kwarg; M2a tests migrated to use `default_scaling=FixedScaling(workers=N)`
- [x] Test: `chain.configure(name, scaling=FixedScaling(workers=4))` results in 4 workers for that stage (proven via the same in-flight barrier as M2a)
- [x] Test: `flow(*, default_scaling=...)` and `chain.configure_default(scaling=...)` apply to all stages
- [x] Test: per-stage override beats pipeline default
- [x] Test: `default_queue=lifo_queue`, `default_queue_size=N`, `configure(name, queue_size=N)` all wire through without crashing
- [x] Test: `FixedScaling(workers=0)` raises
- [x] Test: configuring an unknown stage name silently stores (no error); pipeline still runs

Coverage: 95% on `_flow.py`, 96% overall. 61 tests pass in 0.05s.

### M2c — UtilizationScaling integration

Goal: `UtilizationScaling` (already implemented as a strategy) drives the worker pool dynamically based on `StageSnapshot`.

- [x] Extracted `_FlowRun` class — per-execution state and tasks live in their own object instead of closures inside `_start_and_join`. Sets up M5 (`Flow.stop()` calls `_FlowRun.abort()`), M6 (`PushHandle` holds a `_FlowRun`), M9/M10 (`dump()` reads from it)
- [x] Wired `strategy.on_enqueue()` after every put (source's put to stage 0; worker's put to stage i+1) and `strategy.on_dequeue()` after every get (right after the worker takes an item, before processing)
- [x] Per-stage `_make_snapshot()` builds `StageSnapshot` from runtime tracking (busy/idle/queue_length). Timestamps are placeholder zeros — M9/M10 will fill them when `dump(stats)` lands.
- [x] Two-counter design (`_target` / `_alive`) per DESIGN.md "Worker pool internals"
- [x] `_spawn_worker()` is the sole source of `_alive` increments; `_alive` starts at 0 and is brought up to `_target` by the initial spawn loop
- [x] Polling retirement check at top of worker loop (handles `target > 0` case)
- [x] State-driven completion via `_done_event` (no `gather`/`TaskGroup`); fired when `_source_finished and all(_alive == 0)`
- [x] `_input_drained[i]` flag distinguishes drain-cascade from voluntary retirement (avoids spurious downstream shutdown when workers retire mid-run)
- [x] `_all_workers[i]` and `_idle_workers[i]` task sets per stage — placeholders for M5 abort and the scale-to-zero targeted-cancel
- [x] Test: `UtilizationScaling(min_workers=1, max_workers=8)` grows worker count under load (max_in_flight ≥ 4)
- [x] Regression test: `FixedScaling(workers=4)` still gives exactly 4 concurrent workers after the refactor
- [x] Test: no items lost or duplicated under dynamic scaling

Deferred to M2d / later:
- Targeted cancellation of idle workers (needed for true scale-to-zero with `min_workers=0`)
- Cooldown / sampling integration tests for `UtilizationScaling` (the strategy is already covered by `test_utilizationscaling.py`; integration in the runtime is implicit since strategy decisions are sync)
- Removing workers when load drops (polling handles it lazily, but a test demonstrating shrinkage would be nice)

Coverage: 97% on `_flow.py`, 98% overall. 65 tests pass in 0.05s. `make lint` clean.

### M3 — CM factory transformers

Goal: per-worker resource lifecycle. Each worker enters its own context on spawn, exits on stop.

- [x] CM factory detection (callable with 0 params — function or class — used as-is; framework calls factory() per worker)
- [x] Plain async transformers (1 arg) wrapped at construction with `_wrap_plain_as_factory` so the worker loop has one uniform `async with s.factory() as user_fn:` code path
- [x] `_StageRuntime.factory` replaces `_StageRuntime.fn` — the field carries a normalised CM-factory shape regardless of the original stage form
- [x] `sync_stage(fn)` helper — wraps a sync function with `asyncio.to_thread`, preserves `__name__` via `functools.wraps` so auto-naming works
- [x] Validation messages updated: 1-arg sync function points users to `sync_stage()`; arity errors mention "0 args (CM factory) or 1 arg (transformer)"
- [x] Drain cascade fix: extracted `_maybe_cascade()` helper called from both `source_task` and `worker_task` finally blocks. Necessary because workers can die (raise) before source ends; without this the `input_drained=True` set by source's finally has no worker exit to trigger the cascade.
- [x] Tests (`tests/test_flow_cm.py`): per-worker isolation (4 workers → 4 distinct factory invocations); class-based CM via `cls()`; `__aexit__` runs on normal drain; `__aexit__` runs on transformer exception (both `try/finally` and class-based forms); plain function regression; `sync_stage` happy path; auto-naming preserved through `sync_stage`; sync 1-arg function rejected with `sync_stage()` pointer
- [x] `__init__.py` exports `sync_stage`

Coverage: 97% on `_flow.py`, 98% overall. 74 tests pass in 0.07s. `make lint` clean.

Deferred to later milestones:
- `__aexit__` on `Flow.stop()` — M5 will exercise this path; current design ensures cancellation can only fire in state 3 (`waiting_input`), so CM init/teardown windows are protected
- Routing transformer exceptions to a typed-events handler — M4

### M4 — Error handling

Goal: typed events to a single error handler; handler decides policy by raising vs returning.

- [x] `TransformerError(item, exception, stage)` dataclass — in new `_errors.py` module
- [x] `SourceError(exception)` dataclass
- [x] `Dropped(item, stage, reason)` dataclass + `DropReason` enum (`UPSTREAM_TERMINATED`, `ROUTER_MISS`)
- [x] Wrap each transformer call in try/except (catches `Exception`, NOT `BaseException` — preserves cancellation); route to handler with `TransformerError`
- [x] Catch source generator exceptions (same Exception-only rule); route to handler with `SourceError`
- [x] `default_handler` in `_errors.py`: log `TransformerError` to stderr; re-raise `SourceError`; silent on `Dropped`
- [x] `Flow.set_error_handler(handler)` method
- [x] `flow(*stages, on_error=handler)` constructor kwarg
- [x] `_FlowRun._handle_error()` wraps the handler call (catches handler exceptions); `_abort()` triggers `shutdown(immediate=True)` cascade and stores the exception
- [x] `execute()` re-raises `_abort_exception` after `_done_event` fires
- [x] Handler raises → workers exit via QueueShutDown → `run()` re-raises with the handler's exception
- [x] Handler returns → worker drops the item, continues with next; source treated as exhausted on returning from SourceError
- [x] `Dropped` event types are exported but not yet emitted by the runtime — that lands with M5 (`UPSTREAM_TERMINATED`) and M8 (`ROUTER_MISS`)
- [x] Tests (`tests/test_flow_errors.py` — 11 tests): TransformerError/SourceError fire correctly; handler return continues, raise aborts; default handler behavior; CancelledError propagates (doesn't reach handler); constructor kwarg ≡ method; event types exported

Coverage: 98% on `_flow.py`, 94% on `_errors.py`, 98% overall. 85 tests pass in 0.11s. `make lint` clean.

### M5 — `Last(value)` + `drain()` + `stop()`

Goal: all three termination paths work; upstream drops emit `Dropped` events.

- [ ] `Last(value)` wrapper class
- [ ] `Last` detection in worker loop: forward wrapped value, then trigger upstream drain from this stage
- [ ] Dropped upstream items emit `Dropped(..., UPSTREAM_TERMINATED)` events
- [ ] `Flow.drain()` — graceful shutdown:
  - In bounded mode: call `source.aclose()` on the source generator
  - In unbounded mode: stop emitting `None`s
  - Trigger the drain cascade via `shutdown(immediate=False)` on the first stage's input queue
  - Wait for all in-flight items to reach terminal state (sink or error handler), then return
- [ ] `Flow.stop()` — abort:
  - Call `shutdown(immediate=True)` on every stage's input queue at once — unblocks all `get()` callers immediately
  - Cancel any worker tasks that are mid-`processing` (in user transformer code)
  - Each worker's per-worker CM `__aexit__` runs (resources always released)
  - Return when all workers gone
- [ ] Tests: `Last(value)` sink sees value last; upstream items dropped; `drain()` waits for in-flight; `stop()` is immediate

### M6 — `push()` + `PushHandle`

Goal: push-mode activation via `async with chain.push() as h: await h.send(item)`.

- [ ] `PushHandle` class with `send(item)` and `complete()` methods
- [ ] `Flow.push()` returns `AsyncContextManager[PushHandle]`
- [ ] `__aenter__` of the CM starts workers and yields the handle
- [ ] `__aexit__` calls `handle.complete()` then waits for drain
- [ ] `handle.send(item)` enqueues to first stage's input queue (blocks under backpressure)
- [ ] `handle.complete()` is idempotent; subsequent `send()` raises
- [ ] Tests: push items via handle; complete + drain on `async with` exit; abort via `chain.stop()` from another task; `send()` after `complete()` raises

### M7 — Sub-flow inlining

Goal: a `Flow` used inside another `flow()` is expanded into the parent's pipeline graph.

- [ ] Sub-flow expansion algorithm per DESIGN.md "Inlining algorithm":
  - Pick prefix from `stage(inner, name="...")` or fallback `_subflow_N`
  - Expand sub-stages with `<prefix>.<sub_stage>` names
  - Carry over the sub-flow's per-stage config under prefixed names
  - Recurse for nested sub-flows
- [ ] Configuration merge order: most-specific wins (parent's `configure()` overrides sub-flow's pre-existing config)
- [ ] `stage(fn, name=...)` wrapper supports both functions and `Flow` instances (for explicit sub-flow naming)
- [ ] Tests: composition preserves behavior; standalone vs composed produce identical output; nested sub-flows expand correctly with dotted names; parent override beats sub-flow config

### M8 — Router

Goal: `router(classifier, **arms, default=None)` works as a graph-fragment stage.

- [ ] `Router` class with `classifier`, `arms`, `default` attributes
- [ ] `router(classifier, **arms, default=None)` factory function
- [ ] Arm expansion: each arm becomes a sub-graph in the parent (treated like sub-flow if arm is a `Flow`, or as a single stage if it's a callable)
- [ ] Classifier runs as a single stage that dispatches items to arm input queues
- [ ] All arm outputs feed the same downstream queue
- [ ] If classifier returns unknown label and no `default`: emit `Dropped(..., ROUTER_MISS)` event; item discarded
- [ ] Tests: arm dispatch by label; `default` arm fallback; ROUTER_MISS reported as `Dropped`

### M9 — `dump(mode="structure")`

Goal: introspect the pipeline graph for debugging.

- [ ] Walk the expanded graph (after sub-flow + router expansion)
- [ ] Render: stage names with namespacing (`outer`, `outer.inner.decode`); queue type per stage; scaling strategy (with min/max); router branches; sub-flow boundaries
- [ ] Choose output format (decide: text, JSON, mermaid, or all three — see DESIGN.md open question)
- [ ] Tests: structure for linear flow, sub-flow composition, router

### M10 — `dump(mode="stats")` + worker-state tracking

Goal: live runtime stats for operational visibility.

- [ ] Track per-worker state (`waiting_input` / `processing` / `waiting_output`) per DESIGN.md "Worker states"
- [ ] Per-stage stats: stage name, busy/idle worker counts, queue length, queue open/closed status, recent throughput, time since last item processed
- [ ] Per-stage worker-state breakdown counts
- [ ] Aggregate top-line: total items processed, total errors, drops by reason
- [ ] Optional: scaling strategy state (cooldown remaining, etc.)
- [ ] Tests: stats while flow is running; stats after `stop()`/`drain()` (final state)

## Polish (across milestones)

- [ ] Add a "Troubleshooting" section to README based on real failure modes encountered during implementation and testing — common mistakes (sync code in stages, `flow(items())` instead of `flow(items)`, missing error handler), how to read `dump()` output, what to check when the pipeline stalls. (Deferred from `readme-rewrite.md` because writing this without a runtime to validate against is speculative.)
- [ ] Verify `make lint` passes (ruff) at every milestone
- [ ] Verify `make cov` shows reasonable coverage at every milestone

## Done

(none yet)
