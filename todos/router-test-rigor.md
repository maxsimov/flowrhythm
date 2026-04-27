# Router test rigor

**Status:** in-progress
**Updated:** 2026-04-27

## Motivation

After M8 + M8.5 + M8.6 (the high-value test additions), the router has 30+ tests and 95% coverage. But a closer review under a "rigorous bug-hunter" lens reveals categories of bugs current tests can't catch:

- **Conservation isn't rigorously asserted** — most tests check output values, which would still pass if some items were silently duplicated or lost (only `sorted(received) == [...]` catches duplicates incidentally).
- **Topology is verified through behavior, not directly** — never inspect `chain._stages` to assert wiring; a construction bug producing wrong indices but happening to "work" for happy-path inputs would slip past.
- **Multi-stage arm Flows are untested** — existing arm-Flow tests use 2-stage Flows; longer arms with sub-flow inlining inside an arm aren't exercised.
- **No stress under concurrency** — bugs that lose 1-in-1000 items wouldn't surface with handful-of-items tests.
- **Cascade-boundary edge cases** for `Last(value)` from non-arm-end positions in multi-stage arms not exercised.

This plan captures targeted tests to close those gaps, ranked by bug-catching value. None requires runtime changes — these are pure tests.

## Items

### Conservation (catches: lost/duplicated items)

- [x] **Conservation across topology variations** — `tests/test_router_rigor.py`: 4 tests covering linear, single-router, nested-router (with inner merge), sequential-routers. All N=100 items in → N items out, exact ID set match.
- [ ] **Stress conservation** — 5,000 items through a multi-arm router with multi-worker arms (`FixedScaling(workers=4)` per arm). Exact count + uniqueness asserted. Catches subtle dispatch races, queue-close races, backpressure-related drops.
- [x] **No cross-arm contamination via item identity** — `tests/test_router_rigor.py::test_conservation_no_cross_arm_contamination`. 200 items through 3-arm router; each item's `visited_arms` is a singleton, and the dispatch matches the classifier's decision.

### Topology introspection (catches: wiring bugs)

- [x] **Direct assertions on `_stages` after construction** — `tests/test_router_rigor.py`: 3 tests pin down classifier `downstream_stage_idx is None` + correct `cascade_targets`, arm-end `downstream_stage_idx == merge_idx`, merge `pending_inputs == N_arms`, default-arm inclusion, and router-as-last (downstream=None for arm-ends).
- [ ] **Re-indexing stress** — build `flow(s1, s2, sub_flow_with_router, s3)` with the sub-flow at varying offsets; assert all hint indices in the resulting `_stages` are correct absolute indices. Catches re-indexer off-by-ones today's tests would mask because output values still happen to match.

### Multi-stage arm Flows (catches: arm-pipeline bugs)

- [x] **Arm with multi-stage sub-flow** — `tests/test_router_rigor.py::test_multi_stage_arm_flow_path_tracing`. 4-stage arm Flow; breadcrumbs verify the full path.
- [ ] **Multi-stage arm with nested router AND a tail stage** — `flow(router(c1, slow=flow(s1, router(c2, x=x, y=y), tag)))`. Three levels of topology; verify breadcrumbs for every path through the outer-arm `slow`.

### Last cascade bounds (catches: under/over-kill bugs)

- [ ] **Last from arm with cascade bounds verification** — instrument every stage with "I started" and "I exited" timestamps. After Last fires from arm A:
  - Assert sibling arm B's workers exited (recorded exit time)
  - Assert classifier exited
  - Assert merge processed Last value
  - Assert NO sibling-arm items appear in sink AFTER Last value (formal assertion across many items, not single-item)
- [x] **Last from middle of multi-stage arm** — `tests/test_router_rigor.py::test_last_from_middle_of_arm_subflow_is_absolute_last`. **Found a real bug**: cascade kill range was based on the firing stage's immediate `downstream_stage_idx`, which doesn't include sibling arms when the firing stage is in the middle of a multi-stage arm. Fixed by adding `arm_merge_idx` to `_StageRuntime` (the merge of the enclosing arm; outermost wins for nested arms) and using it as the kill boundary. The initiator's own downstream chain is excluded from the kill range so the Last value can still flow through to the merge. I10 invariant updated.
- [ ] **Concurrent Last from multiple arms** — both arms return Last on different items. Race condition: which one wins? Both? Document and lock down behavior with a test (might require a runtime decision: "first Last wins, others ignored" vs current undefined behavior).

### Backpressure (catches: blocking bugs)

- [x] **Slow merge backpressures arms backpressures classifier** — `tests/test_router_rigor.py::test_backpressure_through_router_no_loss`. 50 items through a slow-merge bottleneck with `queue_size=1`; all delivered, no losses.
- [ ] **Slow arm + classifier scaling** — single-worker classifier dispatching to slow arm A. Classifier blocks → can't dispatch to arm B even if arm B is free. With multi-worker classifier, parallelism survives. Verify the behavior matches docs.

### Edge cases (catches: corner-case bugs)

- [ ] **Source with one item through complex router topology** — minimal case through nested-router topology. Catches initialization bugs that need ≥2 items to manifest.
- [ ] **All items go to default** — 100% miss rate against named arms; default arm should process them all.
- [ ] **Arm that's never selected** — classifier never returns label for arm B; arm B's workers should still spawn (per scaling) and exit cleanly when classifier finishes.
- [ ] **`drain()` immediately after first send in push mode + router** — item in flight, drain called; verify exactly one item delivered, run completes.

### Order semantics (catches: order-guarantee bugs)

- [ ] **Single-worker arm preserves order WITHIN arm** — items 0, 2, 4, 6 (all "fast") through single-worker fast arm; assert they arrive in 0, 2, 4, 6 order at sink. Verifies the FIFO guarantee per arm.
- [ ] **Multi-worker arm allows out-of-order WITHIN arm** — explicit observation that with N>1 workers per arm, items can arrive out of order at the merge. Documents the non-guarantee (might just be a no-assert observational test).

### Identity preservation (catches: copy bugs)

- [ ] **Same Python object flows through pipeline** — push a `MutableObj()` instance; capture `id()` at each stage; assert `id()` is the same throughout. The framework should never copy items.

## Priority pick (recommended top 5)

If implementing in stages, do these first — they catch the broadest class of bugs:

1. Conservation across topologies (broadest — loss/duplication across topology shapes)
2. Direct `_stages` introspection (catches construction bugs the runtime can mask)
3. Multi-stage arm Flow with breadcrumbs (covers untested sub-flow-as-arm depth)
4. Last from middle of arm sub-flow (cascade boundary case)
5. Backpressure end-to-end through router (verifies multi-input-source merge actually backpressures)

## Non-goals

- These tests don't change runtime behavior except where a discovered bug forces a fix.
- "Concurrent Last from multiple arms" may surface a runtime question (which Last wins?). Resolution either lands here or spawns its own plan if substantial.
