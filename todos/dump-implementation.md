# Implement `Flow.dump()`

**Status:** planned
**Updated:** 2026-04-26

## Motivation

`Flow.dump()` is part of the documented public API but not implemented. It serves two distinct purposes:

1. **Structure mode** — show the pipeline graph for debugging composition, naming, sub-flow inlining
2. **Stats mode** — show live runtime stats (queue lengths, worker counts, utilization) for operational visibility

Both are essential for the developer experience: without `dump()`, users have no way to inspect what their flow actually looks like or what it's doing.

Depends on: `migrate-to-flow.md` (need the flow graph and stats infrastructure first).

## Items

### API

- [ ] Implement `Flow.dump(mode="structure")` returning structure representation
- [ ] Implement `Flow.dump(mode="stats")` returning live runtime stats
- [ ] Decide and implement output format(s) — see ROADMAP open question (`JSON`, `mermaid`, plain text, or all three)

### Structure mode

- [ ] Walk the expanded graph (after sub-flow inlining and router expansion)
- [ ] Show stage names with namespacing (`outer`, `outer.inner.decode`, `outer.split.fast`)
- [ ] Show queues between stages with type (FIFO/LIFO/priority)
- [ ] Show scaling strategy per stage (with min/max bounds)
- [ ] Indicate router branching visually
- [ ] Indicate sub-flow boundaries visually (so user can see what was a `Flow` before inlining)

### Stats mode

- [ ] Per-stage `StageStats` snapshot: stage name, busy/idle worker counts, queue length, recent throughput
- [ ] Aggregate top-line: total items in flight, total items processed, total errors, drops by reason
- [ ] Time since last item processed (for spotting stalls)
- [ ] Optional: include scaling strategy state (cooldown remaining, etc.)

### Tests

- [ ] Structure mode for linear flow
- [ ] Structure mode for sub-flow composition (verify namespaced names)
- [ ] Structure mode for router (verify arm representation)
- [ ] Stats mode while flow is running (verify counters update)
- [ ] Stats mode after `stop()` / `drain()` (verify final state)

### Documentation

- [ ] Add a worked example to README — both modes, real output shown
- [ ] Add a "debugging" subsection mentioning `dump()` as the first thing to try

## Done

(none yet)
