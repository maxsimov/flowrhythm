# Implement `Flow.dump()`

**Status:** implemented
**Updated:** 2026-04-27

## Motivation

`Flow.dump()` is part of the documented public API but not implemented. It serves two distinct purposes:

1. **Structure mode** — show the pipeline graph for debugging composition, naming, sub-flow inlining
2. **Stats mode** — show live runtime stats (queue lengths, worker counts, utilization) for operational visibility

Both are essential for the developer experience: without `dump()`, users have no way to inspect what their flow actually looks like or what it's doing.

Depends on: `implement-runtime.md` (need the flow graph and stats infrastructure first).

## Items

### API

- [x] Implement `Flow.dump(mode="structure")` — text + mermaid + json formats.
- [x] Implement `Flow.dump(mode="stats")` — text + json formats (mermaid raises ValueError; stats aren't graph-shaped). Live during a run; after run, reads from `Flow._last_stats` snapshot taken in `run()/push()`'s finally block.
- [x] Decide and implement output format(s) — **text + mermaid + json** for structure, **text + json** for stats.

### Structure mode

- [x] Walk the expanded graph (after sub-flow inlining and router expansion) — `_stage_info_list()` derives the structured form from `flow._stages` + `flow._resolve_config()`.
- [x] Show stage names with namespacing — uses the actual stored names which already encode the namespace (sub-flow inlining + `stage(name=)` overrides).
- [x] Show queues between stages with type — each stage's row includes `<factory>(maxsize=N)`.
- [x] Show scaling strategy per stage — uses `repr(strategy)`. Added `__repr__` to `FixedScaling` and `UtilizationScaling`.
- [x] Indicate router branching visually — text format shows `arms: {fast→[3], slow→[4]}`; mermaid uses labeled edges and a diamond node for the classifier.
- [-] Sub-flow boundaries — not separately marked beyond the dotted names. Adding explicit grouping (e.g., subgraph in mermaid) is deferred — dotted names + dump output already make composition visible.

### Stats mode

- [x] Per-stage snapshot: stage name, alive/busy/idle worker counts, queue length, queue drained flag, processed counter, error counter. Built by `_FlowRun.snapshot_stats()`.
- [-] Per-stage worker-state breakdown: only alive/busy/idle (= waiting_input proxy) shipped. `processing` / `waiting_output` distinction deferred — would need instrumenting put boundaries; minor info gain. Today `busy` covers "processing"; `idle` covers everything else (includes both `waiting_input` AND `waiting_output`).
- [x] Aggregate top-line: total transformer errors, source errors, drops by reason. (Items in flight not separately tracked; can be derived from queue lengths.)
- [-] Time since last item processed — deferred. Needs timestamp instrumentation in worker loop.
- [-] Scaling strategy state (cooldown remaining) — deferred. Strategy-specific; would need a snapshot method on `ScalingStrategy` Protocol.

### Tests

- [x] Structure mode for linear flow — covered.
- [x] Structure mode for sub-flow composition — covered.
- [x] Structure mode for router — covered.
- [x] Stats mode while flow is running — `test_dump_stats_active_flag_during_run`.
- [x] Stats mode after `stop()` / `drain()` — covered (most stats tests run after a clean run; the active-flag test covers post-stop).

### Documentation

- [x] Add a worked example to README — both modes, real output shown.
- [ ] Add a "debugging" subsection mentioning `dump()` as the first thing to try (deferred to README polish phase).

## Done

- M9 — `dump(mode="structure")` shipped with text/mermaid/json formats. 20 tests in `tests/test_flow_dump.py`.
- M10 — `dump(mode="stats")` shipped with text/json formats (mermaid raises ValueError). Per-stage `processed_count`/`error_count` counters on `_StageRuntime`; aggregate `source_errors` + `drops_by_reason` on `_FlowRun`. `Flow._last_stats` persists the snapshot after a run finishes. 15 tests in `tests/test_flow_dump_stats.py`. `__repr__` added to `FixedScaling` and `UtilizationScaling` (back in M9).
