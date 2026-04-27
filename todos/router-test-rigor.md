# Router test rigor

**Status:** implemented
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

- [x] **Conservation across topology variations** — 4 tests covering linear, single-router, nested-router, sequential-routers. All N=100 items in → N items out, exact ID set match.
- [x] **Stress conservation** — `test_conservation_stress_5k_items_multiworker_arms`: 5,000 items through 3-arm router, `FixedScaling(workers=4)` per arm. All delivered, no duplicates.
- [x] **No cross-arm contamination via item identity** — 200 items through 3-arm router; each item's `visited_arms` is a singleton matching the classifier's decision.

### Topology introspection (catches: wiring bugs)

- [x] **Direct assertions on `_stages` after construction** — 3 tests pin down classifier `downstream_stage_idx is None` + correct `cascade_targets`, arm-end `downstream_stage_idx == merge_idx`, merge `pending_inputs == N_arms`, default-arm inclusion, router-as-last.
- [x] **Re-indexing stress** — `test_reindexing_subflow_with_router_at_varying_offsets`: sub-flow with router placed at offset 0, 1, 2 in the parent; verifies `arm_first_stage_idx` and `merge_stage_idx` are absolute (correctly re-based) indices.

### Multi-stage arm Flows (catches: arm-pipeline bugs)

- [x] **Arm with multi-stage sub-flow** — 4-stage arm Flow; breadcrumbs verify the full path.
- [x] **Multi-stage arm with nested router AND a tail stage** — `test_multi_stage_arm_with_nested_router_and_tail_breadcrumbs`. Three nesting levels (outer router → arm sub-flow → inner router); breadcrumbs verify every path.

### Last cascade bounds (catches: under/over-kill bugs)

- [x] **Last from arm with cascade bounds verification** — `test_last_no_sibling_items_after_last_under_load`: stress version with 15 fast items in flight when Last fires. Asserts FINAL is at sink AND zero items after FINAL.
- [x] **Last from middle of multi-stage arm** — **bug found and fixed**: cascade kill range was based on `downstream_stage_idx`, missing sibling arms when firing stage was mid-arm. Fix: `arm_merge_idx` annotated on each `_StageRuntime`; outermost wins for nested. Initiator's own downstream chain excluded from the kill range so Last value can flow through.
- [x] **Concurrent Last from multiple arms** — `test_concurrent_last_from_multiple_arms_does_not_hang`. Both arms return Last on different items. Pipeline terminates without hang or duplicate; "first wins" emerges from the existing cascade semantics (subsequent Lasts find queues already shut down and exit naturally).

### Backpressure (catches: blocking bugs)

- [x] **Slow merge backpressures arms backpressures classifier** — 50 items through a slow-merge bottleneck with `queue_size=1`; all delivered.
- [ ] **Slow arm + classifier scaling** — Deferred. Hard to deterministically verify the "classifier blocks" timing behavior; the conservation + backpressure tests already exercise the underlying mechanism. Can be revisited if a real bug surfaces.

### Edge cases (catches: corner-case bugs)

- [x] **Source with one item through complex router topology** — `test_source_with_one_item_through_complex_topology`. Single item through nested router; verifies init handles N=1.
- [x] **All items go to default** — `test_all_items_to_default_arm`. 50 items, 100% miss rate; all caught by default arm.
- [x] **Arm that's never selected** — `test_unused_arm_workers_exit_cleanly`. Classifier never picks the second arm; its workers spawn, wait on get(), exit cleanly when the cascade signals drain.
- [x] **`drain()` immediately after first send in push mode + router** — `test_drain_after_first_send_in_push_mode_with_router`. **Bug found and fixed**: in push mode, `_mark_complete()` was triggering `_maybe_cascade(0)` BEFORE `execute()` spawned workers. With `alive==0` everywhere, the cascade prematurely shut down all arm queues — items dispatched by the classifier hit closed queues and were silently dropped. Fix: removed the `_maybe_cascade` call from `_mark_complete`; the cascade now triggers naturally from the classifier worker's `finally`.

### Order semantics (catches: order-guarantee bugs)

- [x] **Single-worker arm preserves order WITHIN arm** — `test_single_worker_arm_preserves_order_within_arm`. Items in fast and slow arms arrive at sink in dispatch order within each arm.
- [x] **Multi-worker arm allows out-of-order WITHIN arm** — `test_multiworker_arm_does_not_guarantee_order`. Observational: 30 items through 4-worker arm with variable delay. All delivered (sorted match), but no specific-order assertion (documents the non-guarantee).

### Identity preservation (catches: copy bugs)

- [x] **Same Python object flows through pipeline** — `test_item_identity_preserved_through_router`. Each item's `id()` captured at every stage it visits is identical — framework never copies items.

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
