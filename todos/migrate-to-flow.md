# Migrate implementation to `flow()` API

**Status:** planned
**Updated:** 2026-04-26

## Motivation

Current `flowrhythm/_builder.py` is the legacy `Builder` DSL. The README documents the new `flow()` function, `Flow`/`Router`/`PushHandle` design with graph-level sub-flow inlining, typed error events, three drive modes, and `Last(value)` termination. The implementation needs to catch up to the docs.

This is a from-scratch rebuild of the runtime. `_builder.py` will be replaced; existing tests in `tests/test_builder.py` are skipped at module level pending rewrite.

See `DESIGN.md` for the design that this implements.

## Items

### Public API surface

- [ ] Implement `flow(*stages, on_error=None, default_scaling=None, default_queue=None)` function returning a `Flow` instance. Constructor kwargs are equivalent to `set_error_handler` / `configure_default(scaling=...)` / `configure_default(queue=...)` after construction.
- [ ] Implement `router(classifier, **arms, default=None)` returning a `Router` instance
- [ ] Implement `stage(fn, name=...)` wrapper for explicit naming
- [ ] Implement `Last(value)` wrapper for transformer-initiated termination
- [ ] Update `flowrhythm/__init__.py` exports: `flow`, `router`, `stage`, `Last`, `Flow` (typing), `Router` (typing), `PushHandle` (typing), `TransformerError`, `SourceError`, `Dropped`, `DropReason`, scaling classes, queue factories

### Construction-time logic

- [ ] Stage role detection per DESIGN.md "Stage role detection" section — reject async generators with clear `TypeError`, dispatch by signature inspection
- [ ] Auto-derive stage names from function names; numeric suffix on collisions
- [ ] Reject sync functions and sync context managers with error pointing to `asyncio.to_thread` or `sync_stage()` helper
- [ ] Sub-flow expansion — implement the inlining algorithm per DESIGN.md "Composition (sub-flows) → Inlining algorithm": (1) pick prefix from `stage(inner, name="...")` or fallback `_subflow_N`, (2) expand sub-stages with `<prefix>.<sub>` names, (3) carry over the sub-flow's per-stage config under prefixed names, (4) recurse for nested sub-flows. Configuration merge order: most-specific wins (parent's `configure()` overrides sub-flow's pre-existing config).
- [ ] Router arm expansion — each arm becomes a sub-graph in the parent

### Runtime

- [ ] Internal `_start()` / `_join()` (private — not exposed)
- [ ] Per-stage worker pool — spawn N workers based on scaling strategy
- [ ] Per-worker async context manager lifecycle (acquire on spawn, release on stop)
- [ ] Wire `ScalingStrategy.on_enqueue` / `on_dequeue` to actual worker spawn/kill
- [ ] Bounded queue between stages — default `maxsize=1` (per DESIGN.md "Queue size and backpressure"); override via `chain.configure(name, queue_size=N)`
- [ ] EOF/drain cascade — close-style queue per DESIGN.md "EOF / drain cascade"; implement closeable subclass of `asyncio.Queue` (and LIFO/Priority variants) with `close()`/`QueueClosed`. Track per-worker state (`waiting_input` / `processing` / `waiting_output`) for `dump()`.
- [ ] `Last(value)` handling — output `value`, then close upstream queue, drop in-flight upstream items as `Dropped(..., UPSTREAM_TERMINATED)`

### Activation modes

- [ ] `Flow.run(source)` — bounded; consume async generator with one task; close on exhaustion
- [ ] `Flow.run(source)` — reject already-instantiated generators (`isinstance` check); raise `TypeError` with helpful message: "pass the generator function, not the called generator. e.g., chain.run(my_items) not chain.run(my_items())"
- [ ] `Flow.run()` — unbounded; tight loop `await first_queue.put(None)` (per DESIGN.md "Auto-emit rate" — backpressure from `maxsize=1` throttles naturally, no timing logic needed); runs until `stop()`/`drain()`
- [ ] `Flow.run(source)` for CM-factory source — call factory, enter context, iterate inner generator
- [ ] `Flow.push()` — return `AsyncContextManager[PushHandle]`
- [ ] `PushHandle.send(item)` — enqueue to first stage's queue (blocks if full)
- [ ] `PushHandle.complete()` — signal end-of-stream; idempotent
- [ ] `Flow.drain()` — graceful: close source (`source.aclose()` for generators, stop auto-trigger, etc.); wait for all items to reach terminal state
- [ ] `Flow.stop()` — abort: cancel all worker tasks; ensure `__aexit__` runs on each per-worker CM

### Configuration

- [ ] `Flow.configure(name, scaling=..., queue=...)` — per-stage tuning; supports namespaced names like `"inner.decode"`
- [ ] `Flow.configure_default(scaling=..., queue=...)` — pipeline-wide defaults
- [ ] `Flow.set_error_handler(handler)` — replace default handler

### Error handling

- [ ] Implement `TransformerError`, `SourceError`, `Dropped` dataclasses
- [ ] Implement `DropReason` enum (`UPSTREAM_TERMINATED`, `ROUTER_MISS`)
- [ ] Wrap each transformer call in try/except; route to error handler with `TransformerError`
- [ ] Catch source generator exceptions; route to error handler with `SourceError`
- [ ] Default handler: log `TransformerError`, re-raise `SourceError`, silent on `Dropped`
- [ ] If handler raises, propagate out of `run()` after stopping workers
- [ ] If handler returns, continue (for transformer/dropped) or treat source as exhausted (for source error)

### Tests (rewrite against new API)

- [ ] Delete or replace `tests/test_builder.py` (currently skipped)
- [ ] Linear flow: source → transformers → sink
- [ ] CM factory transformer: per-worker context entered/exited
- [ ] Sub-flow composition: stages inlined with namespaced names; sub-stage scaling preserved
- [ ] Router: arm dispatch, default fallback, ROUTER_MISS reports `Dropped`
- [ ] `Last(value)` semantics: value reaches sink last; upstream items reported as `Dropped`
- [ ] Error handler: typed events, raise-aborts vs return-continues
- [ ] Push mode: `chain.push()` + `send()`; `complete()` and `async with` exit
- [ ] `drain()` per-mode (bounded, unbounded, push)
- [ ] `stop()` cancels workers and runs `__aexit__`
- [ ] Reject async generators in `flow()` args with `TypeError`
- [ ] Reject sync functions in `flow()` args with informative error
- [ ] Naming: auto-derive from function name; numeric suffix on collision; `stage(fn, name=...)` overrides

### Polish

- [ ] Implement `sync_stage(fn)` helper that wraps with `asyncio.to_thread`
- [ ] Add a "Troubleshooting" section to README based on real failure modes encountered during implementation and testing — common mistakes (sync code in stages, `flow(items())` instead of `flow(items)`, missing error handler), how to read `dump()` output, what to check when the pipeline stalls. Was deferred from `readme-rewrite.md` because writing this without a runtime to validate against is speculative.
- [ ] Verify `make lint` passes (ruff)
- [ ] Verify `make cov` shows reasonable coverage

## Done

(none yet)
