# Implement `Flow.dump()`

**Status:** in-progress
**Updated:** 2026-04-27

## Motivation

`Flow.dump()` is part of the documented public API but not implemented. It serves two distinct purposes:

1. **Structure mode** — show the pipeline graph for debugging composition, naming, sub-flow inlining
2. **Stats mode** — show live runtime stats (queue lengths, worker counts, utilization) for operational visibility

Both are essential for the developer experience: without `dump()`, users have no way to inspect what their flow actually looks like or what it's doing.

Depends on: `implement-runtime.md` (need the flow graph and stats infrastructure first).

## Items

### API

- [x] Implement `Flow.dump(mode="structure")` — see `Flow.dump()` in `_flow.py`. Three formats via `format=` kwarg.
- [ ] Implement `Flow.dump(mode="stats")` — currently raises `NotImplementedError`; M10.
- [x] Decide and implement output format(s) — **text + mermaid + json** (settled with user). Sub-flow boundaries are NOT visually marked separately because dotted names already convey the structure (`outer.inner.decode`); router branching IS shown explicitly via arm labels and merge edges.

### Structure mode

- [x] Walk the expanded graph (after sub-flow inlining and router expansion) — `_stage_info_list()` derives the structured form from `flow._stages` + `flow._resolve_config()`.
- [x] Show stage names with namespacing — uses the actual stored names which already encode the namespace (sub-flow inlining + `stage(name=)` overrides).
- [x] Show queues between stages with type — each stage's row includes `<factory>(maxsize=N)`.
- [x] Show scaling strategy per stage — uses `repr(strategy)`. Added `__repr__` to `FixedScaling` and `UtilizationScaling`.
- [x] Indicate router branching visually — text format shows `arms: {fast→[3], slow→[4]}`; mermaid uses labeled edges and a diamond node for the classifier.
- [-] Sub-flow boundaries — not separately marked beyond the dotted names. Adding explicit grouping (e.g., subgraph in mermaid) is deferred — dotted names + dump output already make composition visible.

### Stats mode

- [ ] Per-stage `StageSnapshot` snapshot: stage name, busy/idle worker counts, queue length, queue open/closed status, recent throughput
- [ ] Per-stage worker-state breakdown: count of workers in each of `waiting_input` / `processing` / `waiting_output` (per DESIGN.md "Worker states"). Diagnoses stalls (e.g., "all workers `waiting_output`" → downstream bottleneck)
- [ ] Aggregate top-line: total items in flight, total items processed, total errors, drops by reason
- [ ] Time since last item processed (for spotting stalls)
- [ ] Optional: include scaling strategy state (cooldown remaining, etc.)

### Tests

- [x] Structure mode for linear flow — covered in `tests/test_flow_dump.py`.
- [x] Structure mode for sub-flow composition (verify namespaced names) — covered.
- [x] Structure mode for router (verify arm representation) — covered for all three formats.
- [ ] Stats mode while flow is running (verify counters update)
- [ ] Stats mode after `stop()` / `drain()` (verify final state)

### Documentation

- [x] Add a worked example to README — both modes, real output shown.
- [ ] Add a "debugging" subsection mentioning `dump()` as the first thing to try (deferred to README polish phase).

## Done

- M9 — `dump(mode="structure")` shipped with text/mermaid/json formats. 20 tests in `tests/test_flow_dump.py`. `__repr__` added to scaling strategies. DESIGN open question on dump format resolved (all three).
