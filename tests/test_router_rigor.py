"""Rigorous bug-hunt tests for router topology and message paths.

These tests target categories of bugs the happy-path tests in
`test_flow_router.py` could miss: conservation (lost/duplicated items),
direct topology wiring, multi-stage arm Flows, Last cascade boundaries
inside arm sub-flows, and end-to-end backpressure through routers.

See `todos/router-test-rigor.md` for the full plan.
"""

import asyncio

from flowrhythm import (
    FixedScaling,
    Last,
    flow,
    router,
    stage,
)


# ---------------------------------------------------------------------------
# #1 Conservation across topology variations
# ---------------------------------------------------------------------------


async def test_conservation_linear_chain():
    """N items in → N items out, each unique ID, no duplicates, no losses."""
    received = []
    N = 100

    async def passthrough(item):
        return item

    async def collect(item):
        received.append(item)

    chain = flow(passthrough, passthrough, passthrough, collect)

    async def items():
        for i in range(N):
            yield {"id": i}

    await chain.run(items)

    ids = [item["id"] for item in received]
    assert len(ids) == N
    assert set(ids) == set(range(N))
    assert len(set(ids)) == N  # no duplicates


async def test_conservation_single_router():
    """Conservation through a single router with two arms."""
    received = []
    N = 100

    async def classify(item):
        return "even" if item["id"] % 2 == 0 else "odd"

    async def even(item):
        return item

    async def odd(item):
        return item

    async def collect(item):
        received.append(item)

    chain = flow(router(classify, even=even, odd=odd), collect)

    async def items():
        for i in range(N):
            yield {"id": i}

    await chain.run(items)

    ids = sorted(item["id"] for item in received)
    assert len(ids) == N
    assert ids == list(range(N))


async def test_conservation_nested_router():
    """Conservation through a nested router (inner has its own merge)."""
    received = []
    N = 100

    async def cls_outer(item):
        return "slow" if item["id"] >= 50 else "fast"

    async def fast(item):
        return item

    async def cls_inner(item):
        return "high" if item["id"] >= 75 else "low"

    async def low(item):
        return item

    async def high(item):
        return item

    async def inner_tag(item):
        return item

    async def collect(item):
        received.append(item)

    inner = flow(router(cls_inner, low=low, high=high), inner_tag)
    chain = flow(router(cls_outer, fast=fast, slow=inner), collect)

    async def items():
        for i in range(N):
            yield {"id": i}

    await chain.run(items)

    ids = sorted(item["id"] for item in received)
    assert ids == list(range(N))


async def test_conservation_sequential_routers():
    """Conservation through two routers in series."""
    received = []
    N = 100

    async def cls1(item):
        return "even" if item["id"] % 2 == 0 else "odd"

    async def even(item):
        return item

    async def odd(item):
        return item

    async def cls2(item):
        return "small" if item["id"] < 50 else "big"

    async def small(item):
        return item

    async def big(item):
        return item

    async def collect(item):
        received.append(item)

    chain = flow(
        router(cls1, even=even, odd=odd),
        router(cls2, small=small, big=big),
        collect,
    )

    async def items():
        for i in range(N):
            yield {"id": i}

    await chain.run(items)

    ids = sorted(item["id"] for item in received)
    assert ids == list(range(N))


async def test_conservation_no_cross_arm_contamination():
    """Each item must visit exactly ONE arm at each routing level. With
    items as mutable dicts that record their visited arms, assert that
    every item's `visited_arms` is a singleton — not multiple arms (which
    would indicate dispatch leakage)."""
    received = []
    N = 200

    async def classify(item):
        item["visited_arms"].append("classify_input")
        return "a" if item["id"] % 3 == 0 else ("b" if item["id"] % 3 == 1 else "c")

    async def a(item):
        item["visited_arms"].append("a")
        return item

    async def b(item):
        item["visited_arms"].append("b")
        return item

    async def c(item):
        item["visited_arms"].append("c")
        return item

    async def collect(item):
        received.append(item)

    chain = flow(router(classify, a=a, b=b, c=c), collect)

    async def items():
        for i in range(N):
            yield {"id": i, "visited_arms": []}

    await chain.run(items)

    assert len(received) == N
    for item in received:
        # Trail: ["classify_input", <one of a/b/c>]
        assert len(item["visited_arms"]) == 2
        assert item["visited_arms"][0] == "classify_input"
        assert item["visited_arms"][1] in ("a", "b", "c")
        # Verify the dispatch matched the classifier's decision
        expected = "a" if item["id"] % 3 == 0 else ("b" if item["id"] % 3 == 1 else "c")
        assert item["visited_arms"][1] == expected


# ---------------------------------------------------------------------------
# #2 Topology introspection — assert wiring directly on _FlowRun
# ---------------------------------------------------------------------------


async def test_topology_introspection_single_router():
    """For a known router topology, assert exactly the runtime wiring:
    classifier dispatches via wrapper (downstream=None), arm-ends point
    to merge, merge has pending_inputs == N_arms, classifier has
    cascade_targets covering all arm queues."""
    from flowrhythm._flow import _FlowRun

    async def classify(x):
        return "a"

    async def fn_a(x):
        return x

    async def fn_b(x):
        return x

    async def fn_c(x):
        return x

    async def collect(x):
        pass

    chain = flow(
        stage(router(classify, a=fn_a, b=fn_b, c=fn_c), name="r"),
        collect,
    )

    runner = _FlowRun(chain, source=None)

    # Expected layout (one stage per row):
    #   0: r          (classifier; cascade to [1,2,3]; downstream=None)
    #   1: r.a        (arm-end; downstream=4 (merge))
    #   2: r.b        (arm-end; downstream=4)
    #   3: r.c        (arm-end; downstream=4)
    #   4: collect    (merge; pending_inputs=3; downstream=None — last stage)
    assert chain.stage_names == ["r", "r.a", "r.b", "r.c", "collect"]

    # Classifier
    assert runner._stages[0].downstream_stage_idx is None
    assert sorted(runner._stages[0].cascade_targets) == [1, 2, 3]
    assert runner._stages[0].pending_inputs == 1  # source feeds it

    # Arm-ends all point to merge (idx 4)
    for arm_idx in (1, 2, 3):
        assert runner._stages[arm_idx].downstream_stage_idx == 4
        assert runner._stages[arm_idx].cascade_targets is None
        assert runner._stages[arm_idx].pending_inputs == 1  # classifier feeds it

    # Merge: 3 contributors (the 3 arm-ends), no downstream (last stage)
    assert runner._stages[4].pending_inputs == 3
    assert runner._stages[4].downstream_stage_idx is None
    assert runner._stages[4].cascade_targets is None


async def test_topology_introspection_with_default_arm():
    """Verify that the default arm is included in cascade_targets and
    contributes to merge's pending_inputs."""
    from flowrhythm._flow import _FlowRun

    async def classify(x):
        return "a"

    async def fn_a(x):
        return x

    async def fn_default(x):
        return x

    async def collect(x):
        pass

    chain = flow(
        stage(router(classify, a=fn_a, default=fn_default), name="r"),
        collect,
    )
    runner = _FlowRun(chain, source=None)

    # Layout:
    #   0: r          (classifier; cascade to [1,2])
    #   1: r.a        (arm-end → merge=3)
    #   2: r.default  (arm-end → merge=3)
    #   3: collect    (merge; pending_inputs=2)
    assert chain.stage_names == ["r", "r.a", "r.default", "collect"]
    assert sorted(runner._stages[0].cascade_targets) == [1, 2]
    assert runner._stages[3].pending_inputs == 2


async def test_topology_introspection_router_as_last():
    """Router with no merge — arm-ends have downstream=None (sink)."""
    from flowrhythm._flow import _FlowRun

    async def classify(x):
        return "a"

    async def fn_a(x):
        return x

    async def fn_b(x):
        return x

    chain = flow(stage(router(classify, a=fn_a, b=fn_b), name="r"))
    runner = _FlowRun(chain, source=None)

    # Layout: 0:r (classifier), 1:r.a (arm-end no-merge), 2:r.b (same)
    assert chain.stage_names == ["r", "r.a", "r.b"]
    assert runner._stages[0].downstream_stage_idx is None  # classifier
    assert runner._stages[1].downstream_stage_idx is None  # arm-end → sink
    assert runner._stages[2].downstream_stage_idx is None  # arm-end → sink


# ---------------------------------------------------------------------------
# #3 Multi-stage arm Flow with breadcrumbs
# ---------------------------------------------------------------------------


async def test_multi_stage_arm_flow_path_tracing():
    """An arm that's a 4-stage Flow. Verify items take the full path
    through every arm stage in order."""
    received = []

    async def items():
        for v in [1, 100]:
            yield {"value": v, "trail": []}

    async def cls(item):
        item["trail"].append("cls")
        return "fast" if item["value"] < 10 else "slow"

    async def fast(item):
        item["trail"].append("fast")
        return item

    async def s1(item):
        item["trail"].append("slow.s1")
        return item

    async def s2(item):
        item["trail"].append("slow.s2")
        return item

    async def s3(item):
        item["trail"].append("slow.s3")
        return item

    async def s4(item):
        item["trail"].append("slow.s4")
        return item

    async def collect(item):
        received.append(item)

    slow_arm = flow(s1, s2, s3, s4)
    chain = flow(
        stage(router(cls, fast=fast, slow=slow_arm), name="r"),
        collect,
    )

    await chain.run(items)

    by_value = {item["value"]: item for item in received}
    assert by_value[1]["trail"] == ["cls", "fast"]
    assert by_value[100]["trail"] == [
        "cls",
        "slow.s1",
        "slow.s2",
        "slow.s3",
        "slow.s4",
    ]


# ---------------------------------------------------------------------------
# #4 Last(value) from middle of an arm sub-flow
# ---------------------------------------------------------------------------


async def test_last_from_middle_of_arm_subflow_is_absolute_last():
    """Last(value) returned from the middle of a multi-stage arm Flow
    must still be the absolute last item the sink processes — including
    over items currently being processed by sibling arms.

    Topology:
        items → router(c) ─┬─ slow_arm.s1 → slow_arm.s2 (returns Last) → slow_arm.s3 ─┐
                           └─ fast (slow on purpose) ──────────────────────────────────┴─► collect

    With fast deliberately slower than s3, fast items would normally win
    the race to merge after Last fires from s2 — unless the cascade
    correctly waits for the entire enclosing arm group (including fast)
    to drain before propagating Last value.
    """
    sink_received = []

    async def cls(x):
        return "fast" if x % 2 == 0 else "slow"

    async def fast(x):
        # Deliberately slow so it would lose the race to merge
        # if the Last cascade doesn't wait for it.
        await asyncio.sleep(0.02)
        return f"fast-{x}"

    async def s1(x):
        return x

    async def s2(x):
        # Trigger Last on the first slow item we see (1)
        if x == 1:
            return Last("FINAL")
        return x

    async def s3(x):
        # Fast (no sleep) so it would propagate Last quickly
        return x

    async def collect(x):
        sink_received.append(x)

    slow_arm = flow(s1, s2, s3)
    # Order matters here: by listing `slow` BEFORE `fast`, the slow arm
    # gets stage indices 1..3 and `fast` gets index 4 — outside the kill
    # range computed from s2.downstream_stage_idx (3). If the cascade
    # boundary is wrong, fast keeps processing during the cascade and
    # injects items into the merge AFTER the Last value.
    chain = flow(
        stage(router(cls, slow=slow_arm, fast=fast), name="r"),
        collect,
    )
    chain.configure("collect", scaling=FixedScaling(workers=1))

    async def items():
        # Send slow first (triggers Last), then several fast items
        # already in flight in the fast arm.
        yield 1  # → slow.s1 → slow.s2 returns Last("FINAL")
        for i in (0, 2, 4, 6, 8):
            yield i  # → fast (slow)

    async with asyncio.timeout(3):
        await chain.run(items)

    # FINAL must be present and the absolute last item received at the sink
    assert "FINAL" in sink_received
    assert sink_received[-1] == "FINAL"


# ---------------------------------------------------------------------------
# #5 Backpressure end-to-end through router
# ---------------------------------------------------------------------------


async def test_backpressure_through_router_no_loss():
    """Slow merge backpressures arms backpressures classifier
    backpressures source. With queue_size=1 everywhere and a slow merge
    stage, all items must still be delivered (no drops, no duplicates)
    despite the bottleneck."""
    received = []
    N = 50

    async def cls(item):
        return "a" if item["id"] % 2 == 0 else "b"

    async def fn_a(item):
        return item

    async def fn_b(item):
        return item

    async def slow_merge(item):
        # Slow processing creates backpressure all the way to source
        await asyncio.sleep(0.001)
        received.append(item)

    chain = flow(router(cls, a=fn_a, b=fn_b), slow_merge)

    async def items():
        for i in range(N):
            yield {"id": i}

    async with asyncio.timeout(5):
        await chain.run(items)

    ids = sorted(item["id"] for item in received)
    assert ids == list(range(N))


# ---------------------------------------------------------------------------
# Stress conservation (5,000 items, multi-worker arms)
# ---------------------------------------------------------------------------


async def test_conservation_stress_5k_items_multiworker_arms():
    """5,000 items through a 3-arm router with FixedScaling(workers=4)
    per arm. Catches subtle dispatch races, queue-close races,
    backpressure-related drops that handful-of-items tests would miss."""
    received = []
    N = 5000

    async def cls(item):
        return ["a", "b", "c"][item["id"] % 3]

    async def fn_a(item):
        return item

    async def fn_b(item):
        return item

    async def fn_c(item):
        return item

    async def collect(item):
        received.append(item)

    chain = flow(stage(router(cls, a=fn_a, b=fn_b, c=fn_c), name="r"), collect)
    chain.configure("r.a", scaling=FixedScaling(workers=4))
    chain.configure("r.b", scaling=FixedScaling(workers=4))
    chain.configure("r.c", scaling=FixedScaling(workers=4))

    async def items():
        for i in range(N):
            yield {"id": i}

    async with asyncio.timeout(30):
        await chain.run(items)

    ids = sorted(item["id"] for item in received)
    assert len(ids) == N
    assert ids == list(range(N))


# ---------------------------------------------------------------------------
# Re-indexing stress: sub-flow with router at varying offsets
# ---------------------------------------------------------------------------


async def test_reindexing_subflow_with_router_at_varying_offsets():
    """Sub-flow containing a router placed at offset 0, 1, 2 in the
    parent. Verify all topology hint indices are correct absolute
    indices into the parent (not stale relative indices from the
    sub-flow's own _stages list)."""
    from flowrhythm._flow import _ArmEndHint, _ClassifierHint

    async def cls(item):
        return "x"

    async def x(item):
        return item

    async def trailing(item):
        return item

    async def s1(item):
        return item

    async def s2(item):
        return item

    async def collect(item):
        pass

    # Build inner once. Inner: classifier (0) → arm "x" (1) → trailing (2)
    def make_inner():
        return flow(stage(router(cls, x=x), name="r"), trailing)

    # Offset 0: i.r (0), i.r.x (1), i.trailing (2), collect (3)
    chain0 = flow(stage(make_inner(), name="i"), collect)
    assert chain0.stage_names == ["i.r", "i.r.x", "i.trailing", "collect"]
    cls_hint0 = chain0._stages[0][2]
    assert isinstance(cls_hint0, _ClassifierHint)
    assert cls_hint0.arm_first_stage_idx == {"x": 1}
    arm_hint0 = chain0._stages[1][2]
    assert isinstance(arm_hint0, _ArmEndHint)
    assert arm_hint0.merge_stage_idx == 2

    # Offset 1: s1 (0), i.r (1), i.r.x (2), i.trailing (3), collect (4)
    chain1 = flow(s1, stage(make_inner(), name="i"), collect)
    assert chain1.stage_names == ["s1", "i.r", "i.r.x", "i.trailing", "collect"]
    cls_hint1 = chain1._stages[1][2]
    assert isinstance(cls_hint1, _ClassifierHint)
    assert cls_hint1.arm_first_stage_idx == {"x": 2}
    arm_hint1 = chain1._stages[2][2]
    assert isinstance(arm_hint1, _ArmEndHint)
    assert arm_hint1.merge_stage_idx == 3

    # Offset 2: s1, s2, i.r, i.r.x, i.trailing, collect
    chain2 = flow(s1, s2, stage(make_inner(), name="i"), collect)
    cls_hint2 = chain2._stages[2][2]
    assert isinstance(cls_hint2, _ClassifierHint)
    assert cls_hint2.arm_first_stage_idx == {"x": 3}
    arm_hint2 = chain2._stages[3][2]
    assert isinstance(arm_hint2, _ArmEndHint)
    assert arm_hint2.merge_stage_idx == 4


# ---------------------------------------------------------------------------
# Multi-stage arm with nested router AND tail stage
# ---------------------------------------------------------------------------


async def test_multi_stage_arm_with_nested_router_and_tail_breadcrumbs():
    """An arm Flow that is itself: stage → router → tail. Three nesting
    levels (outer router → arm sub-flow → inner router). Verify
    breadcrumbs through every path."""
    received = []

    async def items():
        for v in [1, 100, 9999]:
            yield {"value": v, "trail": []}

    async def cls_outer(item):
        item["trail"].append("cls_outer")
        return "fast" if item["value"] < 10 else "slow"

    async def fast(item):
        item["trail"].append("fast")
        return item

    async def s1(item):
        item["trail"].append("s1")
        return item

    async def cls_inner(item):
        item["trail"].append("cls_inner")
        return "x" if item["value"] < 1000 else "y"

    async def x_arm(item):
        item["trail"].append("x")
        return item

    async def y_arm(item):
        item["trail"].append("y")
        return item

    async def tag(item):
        item["trail"].append("tag")
        return item

    async def collect(item):
        received.append(item)

    slow_arm = flow(
        s1,
        stage(router(cls_inner, x=x_arm, y=y_arm), name="ri"),
        tag,
    )
    chain = flow(
        stage(router(cls_outer, fast=fast, slow=slow_arm), name="ro"),
        collect,
    )

    await chain.run(items)

    by_value = {item["value"]: item for item in received}
    assert by_value[1]["trail"] == ["cls_outer", "fast"]
    assert by_value[100]["trail"] == [
        "cls_outer",
        "s1",
        "cls_inner",
        "x",
        "tag",
    ]
    assert by_value[9999]["trail"] == [
        "cls_outer",
        "s1",
        "cls_inner",
        "y",
        "tag",
    ]


# ---------------------------------------------------------------------------
# Last cascade bounds — stress version with many sibling items in flight
# ---------------------------------------------------------------------------


async def test_last_no_sibling_items_after_last_under_load():
    """Many sibling-arm items in flight when Last fires. Assert
    (a) Last value at sink, (b) zero items appear after Last."""
    sink_received = []

    async def cls(x):
        return "fast" if x % 2 == 0 else "slow"

    async def fast(x):
        # Slow enough that several are in flight when Last fires
        await asyncio.sleep(0.005)
        return f"fast-{x}"

    async def slow_returns_last(x):
        if x == 99:
            return Last("FINAL")
        return f"slow-{x}"

    async def collect(x):
        sink_received.append(x)

    # Slow first so it gets lower indices and fast is OUTSIDE the immediate
    # downstream chain — exactly the situation the M8 fix addresses.
    chain = flow(
        stage(router(cls, slow=slow_returns_last, fast=fast), name="r"),
        collect,
    )
    chain.configure("collect", scaling=FixedScaling(workers=1))

    async def items():
        # Many fast items in flight, then trigger Last
        for i in range(0, 30, 2):  # 0, 2, 4, ..., 28 — all fast
            yield i
        yield 99  # → slow → Last

    async with asyncio.timeout(5):
        await chain.run(items)

    assert "FINAL" in sink_received
    last_idx = sink_received.index("FINAL")
    after_final = sink_received[last_idx + 1:]
    assert after_final == [], (
        f"items appeared after Last value: {after_final}"
    )


# ---------------------------------------------------------------------------
# Concurrent Last from multiple arms — locks down the "first wins" contract
# ---------------------------------------------------------------------------


async def test_concurrent_last_from_multiple_arms_does_not_hang():
    """Both arms return Last(...) on different items. Pipeline must
    terminate (no deadlock) and not duplicate items. Whichever Last's
    cascade starts first wins — subsequent Lasts are no-ops since the
    queues are already shut down."""
    sink_received = []

    async def cls(x):
        return "a" if x % 2 == 0 else "b"

    async def arm_a(x):
        return Last(f"FINAL-A-{x}")

    async def arm_b(x):
        return Last(f"FINAL-B-{x}")

    async def collect(x):
        sink_received.append(x)

    chain = flow(router(cls, a=arm_a, b=arm_b), collect)

    async def items():
        for i in range(10):
            yield i

    async with asyncio.timeout(2):
        await chain.run(items)

    # Pipeline terminated. The number of received items depends on the
    # race; at minimum the first Last's value should land. No item should
    # appear twice.
    assert len(sink_received) >= 1
    assert len(sink_received) == len(set(sink_received))
    for item in sink_received:
        assert item.startswith("FINAL-")


# ---------------------------------------------------------------------------
# Edge cases
# ---------------------------------------------------------------------------


async def test_source_with_one_item_through_complex_topology():
    """A single item through a nested-router topology — catches
    initialization bugs that need ≥2 items to manifest."""
    received = []

    async def cls_outer(x):
        return "slow"

    async def cls_inner(x):
        return "x"

    async def x(item):
        return item

    async def collect(item):
        received.append(item)

    inner = flow(router(cls_inner, x=x))
    chain = flow(router(cls_outer, slow=inner), collect)

    async def items():
        yield {"id": 42}

    async with asyncio.timeout(2):
        await chain.run(items)
    assert received == [{"id": 42}]


async def test_all_items_to_default_arm():
    """100% miss rate against named arms; default catches every item."""
    matched = []
    defaulted = []

    async def cls(x):
        return "unknown_label"

    async def matched_arm(x):
        matched.append(x)

    async def default_arm(x):
        defaulted.append(x)

    chain = flow(router(cls, matched=matched_arm, default=default_arm))

    async def items():
        for i in range(50):
            yield i

    await chain.run(items)
    assert matched == []
    assert sorted(defaulted) == list(range(50))


async def test_unused_arm_workers_exit_cleanly():
    """An arm that's never picked: its workers spawn, wait on get(),
    and exit cleanly when the cascade signals drain."""
    used = []
    unused = []

    async def cls(x):
        return "used"  # never returns "unused"

    async def used_arm(x):
        used.append(x)

    async def unused_arm(x):
        unused.append(x)

    chain = flow(router(cls, used=used_arm, unused=unused_arm))

    async def items():
        for i in range(5):
            yield i

    async with asyncio.timeout(2):
        await chain.run(items)
    assert sorted(used) == [0, 1, 2, 3, 4]
    assert unused == []


async def test_drain_after_first_send_in_push_mode_with_router():
    """Push mode + router + drain on async-with exit: one item
    delivered, run completes."""
    received = []

    async def cls(x):
        return "a"

    async def fn(x):
        received.append(x)

    chain = flow(router(cls, a=fn))

    async with asyncio.timeout(2):
        async with chain.push() as h:
            await h.send(1)
        # Async-with exit calls complete() + drain
    assert received == [1]


# ---------------------------------------------------------------------------
# Order semantics
# ---------------------------------------------------------------------------


async def test_single_worker_arm_preserves_order_within_arm():
    """With single-worker arms and single-worker downstream, items
    within one arm arrive at sink in dispatch order."""
    received = []

    async def cls(x):
        return "fast" if x % 2 == 0 else "slow"

    async def fast(x):
        return x

    async def slow(x):
        await asyncio.sleep(0.001)
        return x

    async def collect(x):
        received.append(x)

    chain = flow(router(cls, fast=fast, slow=slow), collect)
    chain.configure("collect", scaling=FixedScaling(workers=1))

    async def items():
        for i in range(10):
            yield i

    await chain.run(items)

    # Within fast arm: 0, 2, 4, 6, 8 in order
    fast_items = [x for x in received if x % 2 == 0]
    assert fast_items == [0, 2, 4, 6, 8]
    # Within slow arm: 1, 3, 5, 7, 9 in order
    slow_items = [x for x in received if x % 2 == 1]
    assert slow_items == [1, 3, 5, 7, 9]


async def test_multiworker_arm_does_not_guarantee_order():
    """With N>1 workers in one arm, items may arrive at the merge in
    any order — documented non-guarantee. Verify all delivered, no
    specific order assertion."""
    received = []
    N = 30

    async def cls(x):
        return "a"

    async def slow_a(x):
        await asyncio.sleep(0.001 * (x % 3))  # variable delay → reorder
        return x

    async def collect(x):
        received.append(x)

    chain = flow(stage(router(cls, a=slow_a), name="r"), collect)
    chain.configure("r.a", scaling=FixedScaling(workers=4))

    async def items():
        for i in range(N):
            yield i

    async with asyncio.timeout(5):
        await chain.run(items)

    assert sorted(received) == list(range(N))
    # We deliberately don't assert received == list(range(N)) — the multi-
    # worker arm permits reordering. If you need order, use workers=1.


# ---------------------------------------------------------------------------
# Identity preservation
# ---------------------------------------------------------------------------


async def test_item_identity_preserved_through_router():
    """The framework must never copy items. The id() of the dict object
    should be the same at every stage that sees it."""
    seen = {}

    async def cls(item):
        item.setdefault("ids", []).append(("cls", id(item)))
        return "a"

    async def fn_a(item):
        item["ids"].append(("a", id(item)))
        return item

    async def collect(item):
        item["ids"].append(("collect", id(item)))
        seen[item["uid"]] = item

    chain = flow(router(cls, a=fn_a), collect)

    async def items():
        for i in range(5):
            yield {"uid": i}

    await chain.run(items)

    for uid, item in seen.items():
        ids_per_stage = [obj_id for _, obj_id in item["ids"]]
        assert len(set(ids_per_stage)) == 1, (
            f"item {uid} was copied: stage→id list = {item['ids']}"
        )
