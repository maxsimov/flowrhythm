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

- [ ] `Flow.configure(name, *, scaling=None, queue=None, queue_size=None)` — per-stage tuning; supports namespaced names like `"inner.decode"` (sub-flow names won't be resolvable until M7 but the storage shape should be ready)
- [ ] `Flow.configure_default(*, scaling=None, queue=None, queue_size=None)` — pipeline-wide defaults
- [ ] `flow(*stages, default_scaling=None, default_queue=None, default_queue_size=None)` — constructor kwargs equivalent to `configure_default`
- [ ] At `run()` time, resolve effective config per stage: per-stage override → pipeline default → built-in default (`FixedScaling(1)`, `fifo_queue`, `maxsize=1`)
- [ ] Update `FixedScaling` so it actually targets N workers (currently it returns 0 forever — a stub from the legacy code)
- [ ] Test: `flow(...).configure(name, scaling=FixedScaling(4))` results in 4 workers for that stage
- [ ] Test: `flow(*, default_scaling=FixedScaling(2))` applies to all stages
- [ ] Test: per-stage override beats default
- [ ] Test: configuring an unknown stage name — what happens? (Open question per DESIGN.md; for now: silently store, no error. Resolve later.)

### M2c — UtilizationScaling integration

Goal: `UtilizationScaling` (already implemented as a strategy) drives the worker pool dynamically based on `StageStats`.

- [ ] Wire `ScalingStrategy.on_enqueue` and `on_dequeue` into the runtime — invoke after every enqueue/dequeue with the current `StageStats`; spawn or stop workers per the returned delta
- [ ] Build `StageStats` snapshot from worker tracker (busy/idle counts, queue length, timestamps)
- [ ] Test: `UtilizationScaling(min_workers=1, max_workers=8)` adds workers under sustained load and removes them when idle
- [ ] Test: `min_workers=0` (scale-to-zero) — stage starts with 0, scales up on first item, scales back down when idle
- [ ] Test: cooldown prevents flapping under bursty load

### M3 — CM factory transformers

Goal: per-worker resource lifecycle. Each worker enters its own context on spawn, exits on stop.

- [ ] CM factory detection (callable with 0 params returning `AsyncContextManager`)
- [ ] Per-worker `__aenter__` on spawn; `__aexit__` on stop (including on `stop()` and on unhandled exceptions)
- [ ] Class-based factory support (instantiate as `cls()` per worker)
- [ ] `sync_stage(fn)` helper — wrap a sync function with `asyncio.to_thread` so it can be used as an async stage
- [ ] Tests: per-worker resource isolation (each worker holds its own state); `__aexit__` runs on normal shutdown, on `stop()`, on exception

### M4 — Error handling

Goal: typed events to a single error handler; handler decides policy by raising vs returning.

- [ ] `TransformerError(item, exception, stage)` dataclass
- [ ] `SourceError(exception)` dataclass
- [ ] `Dropped(item, stage, reason)` dataclass + `DropReason` enum (`UPSTREAM_TERMINATED`, `ROUTER_MISS`)
- [ ] Wrap each transformer call in try/except; route to handler with `TransformerError`
- [ ] Catch source generator exceptions; route to handler with `SourceError`
- [ ] Default handler: log `TransformerError` to stderr, re-raise `SourceError`, silent on `Dropped`
- [ ] `Flow.set_error_handler(handler)` method
- [ ] `flow(*stages, on_error=...)` constructor kwarg
- [ ] Handler raises → cancel workers, propagate exception out of `run()`
- [ ] Handler returns → continue (or treat source as exhausted for `SourceError`)
- [ ] Tests: each event type fires correctly; raise-aborts; return-continues; default behaviors match docs

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
