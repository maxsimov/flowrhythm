"""Tests for M8 — router(): branching as a sub-pipeline.

The classifier runs as one stage. Each arm has its own input queue and
worker pool (first stage of the arm). All arm outputs feed a shared
merge queue (the stage right after the router in the parent's pipeline).
"""

from flowrhythm import (
    DropReason,
    Dropped,
    FixedScaling,
    flow,
    router,
    stage,
)


# ---------------------------------------------------------------------------
# Basic routing: items dispatched by label
# ---------------------------------------------------------------------------


async def test_router_dispatches_by_label():
    received_fast = []
    received_slow = []

    async def classify(x):
        return "fast" if x < 5 else "slow"

    async def fast(x):
        received_fast.append(x)

    async def slow(x):
        received_slow.append(x)

    chain = flow(router(classify, fast=fast, slow=slow))

    async def items():
        for i in range(10):
            yield i

    await chain.run(items)
    assert sorted(received_fast) == [0, 1, 2, 3, 4]
    assert sorted(received_slow) == [5, 6, 7, 8, 9]


async def test_router_outputs_flow_to_next_stage():
    """Arm outputs converge into the merge queue (next stage after router)."""
    received = []

    async def classify(x):
        return "a" if x % 2 == 0 else "b"

    async def double(x):
        return x * 2

    async def triple(x):
        return x * 3

    async def collect(x):
        received.append(x)

    chain = flow(
        router(classify, a=double, b=triple),
        collect,
    )

    async def items():
        for i in range(4):
            yield i

    await chain.run(items)
    # Even (0, 2) doubled: 0, 4. Odd (1, 3) tripled: 3, 9.
    assert sorted(received) == [0, 3, 4, 9]


# ---------------------------------------------------------------------------
# Default arm: catch-all for unmatched labels
# ---------------------------------------------------------------------------


async def test_router_default_arm_catches_unknown_labels():
    matched = []
    defaulted = []

    async def classify(x):
        return "match" if x == 0 else "unknown_label"

    async def matched_fn(x):
        matched.append(x)

    async def default_fn(x):
        defaulted.append(x)

    chain = flow(router(classify, match=matched_fn, default=default_fn))

    async def items():
        for i in range(3):
            yield i

    await chain.run(items)
    assert matched == [0]
    assert sorted(defaulted) == [1, 2]


# ---------------------------------------------------------------------------
# ROUTER_MISS: no default + unknown label → Dropped event
# ---------------------------------------------------------------------------


async def test_router_miss_emits_dropped_event():
    drops = []

    async def classify(x):
        return "unknown"

    async def matched(x):
        pass

    async def on_error(event):
        if isinstance(event, Dropped):
            drops.append(event)

    chain = flow(router(classify, match=matched), on_error=on_error)

    async def items():
        for i in range(3):
            yield i

    await chain.run(items)
    assert len(drops) == 3
    for d in drops:
        assert d.reason is DropReason.ROUTER_MISS


# ---------------------------------------------------------------------------
# Flow as arm: sub-flow inlining inside a router arm
# ---------------------------------------------------------------------------


async def test_router_with_flow_arm():
    """An arm can be a Flow; its stages are inlined under the arm's label."""
    received = []

    async def classify(x):
        return "heavy" if x > 5 else "light"

    async def light(x):
        return x * 2

    async def decode(x):
        return x + 100

    async def heavy_post(x):
        return x * 10

    async def collect(x):
        received.append(x)

    heavy_path = flow(decode, heavy_post)

    chain = flow(
        router(classify, light=light, heavy=heavy_path),
        collect,
    )

    async def items():
        yield 1  # light: 1 * 2 = 2
        yield 7  # heavy: (7 + 100) * 10 = 1070

    await chain.run(items)
    assert sorted(received) == [2, 1070]


# ---------------------------------------------------------------------------
# Naming: explicit via stage(); fallback _router_N
# ---------------------------------------------------------------------------


async def test_router_explicit_name_via_stage():
    async def classify(x):
        return "a"

    async def a(x):
        pass

    chain = flow(stage(router(classify, a=a), name="my_router"))
    assert "my_router" in chain.stage_names
    assert "my_router.a" in chain.stage_names


async def test_router_fallback_name_uses_router_index():
    async def classify(x):
        return "a"

    async def a(x):
        pass

    chain = flow(router(classify, a=a))
    assert "_router_0" in chain.stage_names
    assert "_router_0.a" in chain.stage_names


async def test_two_unwrapped_routers_get_distinct_indices():
    async def cls1(x):
        return "a"

    async def cls2(x):
        return "b"

    async def a(x):
        pass

    async def b(x):
        pass

    async def collect(x):
        pass

    r1 = router(cls1, a=a)
    r2 = router(cls2, b=b)
    chain = flow(r1, r2, collect)
    assert "_router_0" in chain.stage_names
    assert "_router_0.a" in chain.stage_names
    assert "_router_1" in chain.stage_names
    assert "_router_1.b" in chain.stage_names


# ---------------------------------------------------------------------------
# Configuration: per-arm scaling works
# ---------------------------------------------------------------------------


async def test_router_arm_can_be_configured():
    async def classify(x):
        return "a"

    async def a(x):
        return x

    async def collect(x):
        pass

    chain = flow(stage(router(classify, a=a), name="r"), collect)
    chain.configure("r.a", scaling=FixedScaling(workers=4))

    cfg = chain._resolve_config("r.a")
    assert cfg["scaling"].workers == 4


# ---------------------------------------------------------------------------
# Validation: router() factory rejects bad inputs
# ---------------------------------------------------------------------------


def test_router_rejects_no_arms():
    async def classify(x):
        return "a"

    import pytest

    with pytest.raises(TypeError, match="at least one arm"):
        router(classify)


def test_router_rejects_sync_classifier():
    def classify(x):
        return "a"

    async def a(x):
        pass

    import pytest

    with pytest.raises(TypeError, match="async"):
        router(classify, a=a)


# ---------------------------------------------------------------------------
# Sub-flow containing a router used as a sub-flow (re-indexer test)
# ---------------------------------------------------------------------------


async def test_subflow_containing_router_inlined_as_subflow():
    """A Flow with a router inside, used as a sub-flow (NOT as a router arm).
    The re-indexer must shift the inner router's topology indices."""
    received = []

    async def classify(x):
        return "even" if x % 2 == 0 else "odd"

    async def even(x):
        return x * 10

    async def odd(x):
        return x + 1000

    async def collect_inside(x):
        return x  # passthrough so output reaches outer

    async def collect_outside(x):
        received.append(x)

    inner = flow(router(classify, even=even, odd=odd), collect_inside)
    # Outer has a leading stage so the sub-flow's base_offset > 0
    async def passthrough(x):
        return x

    outer = flow(passthrough, stage(inner, name="x"), collect_outside)

    async def items():
        yield 2
        yield 5

    await outer.run(items)
    # 2 (even) → 2*10 = 20, then collect_inside passes through → 20
    # 5 (odd) → 5+1000 = 1005, then collect_inside passes through → 1005
    assert sorted(received) == [20, 1005]


async def test_router_arm_with_nested_router_no_inner_merge():
    """A Flow used as a router arm CAN itself contain a router. Case 1: the
    inner router is the only/last thing in the inner Flow, so the inner
    arm-ends become the outer arm-ends (multiple terminals)."""
    received = []

    async def cls_outer(x):
        # 'big' (>= 100) → goes to slow (which has its own router)
        # otherwise → fast
        return "slow" if x >= 100 else "fast"

    async def cls_inner(x):
        # within slow: 'huge' (>= 1000) → huge_path; else → big_path
        return "huge" if x >= 1000 else "big"

    async def fast(x):
        return x  # passthrough

    async def big_path(x):
        return x + 10000

    async def huge_path(x):
        return x + 100000

    async def collect(x):
        received.append(x)

    inner = flow(router(cls_inner, big=big_path, huge=huge_path))
    chain = flow(router(cls_outer, fast=fast, slow=inner), collect)

    async def items():
        yield 1  # fast → 1
        yield 100  # slow → big → 100 + 10000 = 10100
        yield 9999  # slow → huge → 9999 + 100000 = 109999

    await chain.run(items)
    assert sorted(received) == [1, 10100, 109999]


async def test_router_arm_with_nested_router_inner_has_merge():
    """Case 2: the inner router has its own merge stage inside the arm Flow.
    Items pass through inner classifier → inner arm → inner merge → outer
    arm-end → outer merge."""
    received = []

    async def cls_outer(x):
        return "main"

    async def cls_inner(x):
        return "even" if x % 2 == 0 else "odd"

    async def even(x):
        return x * 2

    async def odd(x):
        return x + 1

    async def inner_collect(x):
        return ("inner", x)  # tag at inner merge

    async def collect(x):
        received.append(x)

    inner = flow(router(cls_inner, even=even, odd=odd), inner_collect)
    chain = flow(router(cls_outer, main=inner), collect)

    async def items():
        yield 2  # even → 4 → ("inner", 4)
        yield 5  # odd → 6 → ("inner", 6)

    await chain.run(items)
    assert sorted(received) == [("inner", 4), ("inner", 6)]


# ---------------------------------------------------------------------------
# Path tracing: explicit per-item breadcrumb through complex topology
# ---------------------------------------------------------------------------


async def test_router_path_tracing_through_nested_topology():
    """Verify the EXACT path each item takes through a complex DAG.

    Output-value assertions only prove an item reached the sink with the
    right transformation; they don't prove HOW it got there. This test
    instruments every stage to append its name to a per-item breadcrumb,
    so we can pin down: classifier saw the item, exactly one arm
    processed it, no leakage to sibling arms, no duplicate visits, and
    the order of visits matches the topology.

    Topology (outer router with sub-flow arm; inner router has its own
    merge stage):

        items → preprocess → router_outer ─┬─ fast ───────────────────────► collect
                                           └─ slow:                        ▲
                                              router_inner ─┬─ big ─┐      │
                                                            └─ huge ┘      │
                                                                  → tag_inner
    """
    received: list[dict] = []

    async def items():
        for v in [1, 100, 9999]:
            yield {"id": v, "value": v, "trail": []}

    async def preprocess(item):
        item["trail"].append("preprocess")
        return item

    async def cls_outer(item):
        item["trail"].append("cls_outer")
        return "slow" if item["value"] >= 100 else "fast"

    async def fast(item):
        item["trail"].append("fast")
        return item

    async def cls_inner(item):
        item["trail"].append("cls_inner")
        return "huge" if item["value"] >= 1000 else "big"

    async def big(item):
        item["trail"].append("big")
        return item

    async def huge(item):
        item["trail"].append("huge")
        return item

    async def tag_inner(item):
        item["trail"].append("tag_inner")
        return item

    async def collect(item):
        item["trail"].append("collect")
        received.append(item)

    inner = flow(
        stage(router(cls_inner, big=big, huge=huge), name="r_inner"),
        tag_inner,
    )
    chain = flow(
        preprocess,
        stage(router(cls_outer, fast=fast, slow=inner), name="r_outer"),
        collect,
    )

    await chain.run(items)

    # Every item arrived at the sink, exactly once
    assert len(received) == 3
    by_id = {item["id"]: item for item in received}
    assert set(by_id.keys()) == {1, 100, 9999}

    # EXACT path per item — proves both positive (visited expected stages
    # in expected order) and negative (no extra visits) properties.
    assert by_id[1]["trail"] == [
        "preprocess",
        "cls_outer",
        "fast",
        "collect",
    ]
    assert by_id[100]["trail"] == [
        "preprocess",
        "cls_outer",
        "cls_inner",
        "big",
        "tag_inner",
        "collect",
    ]
    assert by_id[9999]["trail"] == [
        "preprocess",
        "cls_outer",
        "cls_inner",
        "huge",
        "tag_inner",
        "collect",
    ]

    # Negative assertions — items must NOT have visited sibling-arm stages.
    # (Already implied by the exact-trail asserts above, but stating them
    # explicitly documents the topology contract.)
    assert "fast" not in by_id[100]["trail"]
    assert "fast" not in by_id[9999]["trail"]
    assert "cls_inner" not in by_id[1]["trail"]
    assert "huge" not in by_id[100]["trail"]
    assert "big" not in by_id[9999]["trail"]


# ---------------------------------------------------------------------------
# Topology completeness
# ---------------------------------------------------------------------------


async def test_router_as_last_stage_arm_outputs_drop_silently():
    """When the router is the last thing in the parent flow, arm outputs
    have nowhere to go (merge_stage_idx = -1 → downstream = None). The
    pipeline must still drain cleanly."""
    received = []

    async def classify(x):
        return "a" if x % 2 == 0 else "b"

    async def fn_a(x):
        received.append(("a", x))
        return x  # return value is dropped (no downstream)

    async def fn_b(x):
        received.append(("b", x))
        return x

    chain = flow(router(classify, a=fn_a, b=fn_b))

    async def items():
        for i in range(6):
            yield i

    import asyncio
    async with asyncio.timeout(2):
        await chain.run(items)

    # Arm side-effects fired for every item; outputs were silently dropped
    assert sorted(received) == [
        ("a", 0), ("a", 2), ("a", 4),
        ("b", 1), ("b", 3), ("b", 5),
    ]


async def test_sequential_routers_chain_correctly():
    """Two routers in series: r1's merge IS r2's classifier input.
    Verifies arm-end → next-router-classifier wiring."""
    received: list[dict] = []

    async def items():
        for v in range(4):
            yield {"value": v, "trail": []}

    async def cls1(item):
        item["trail"].append("cls1")
        return "even" if item["value"] % 2 == 0 else "odd"

    async def even_arm(item):
        item["trail"].append("even_arm")
        return item

    async def odd_arm(item):
        item["trail"].append("odd_arm")
        return item

    async def cls2(item):
        item["trail"].append("cls2")
        return "small" if item["value"] < 2 else "big"

    async def small(item):
        item["trail"].append("small")
        return item

    async def big(item):
        item["trail"].append("big")
        return item

    async def collect(item):
        received.append(item)

    chain = flow(
        stage(router(cls1, even=even_arm, odd=odd_arm), name="r1"),
        stage(router(cls2, small=small, big=big), name="r2"),
        collect,
    )
    await chain.run(items)

    assert len(received) == 4
    by_value = {item["value"]: item for item in received}
    assert by_value[0]["trail"] == ["cls1", "even_arm", "cls2", "small"]
    assert by_value[1]["trail"] == ["cls1", "odd_arm", "cls2", "small"]
    assert by_value[2]["trail"] == ["cls1", "even_arm", "cls2", "big"]
    assert by_value[3]["trail"] == ["cls1", "odd_arm", "cls2", "big"]


async def test_router_arm_can_be_cm_factory():
    """A 0-arg callable returning AsyncContextManager works as an arm."""
    from contextlib import asynccontextmanager

    received = []
    aenter_count = 0
    aexit_count = 0

    @asynccontextmanager
    async def with_resource():
        nonlocal aenter_count, aexit_count
        aenter_count += 1
        try:
            async def fn(x):
                received.append(x)
                return x

            yield fn
        finally:
            aexit_count += 1

    async def classify(x):
        return "go"

    chain = flow(router(classify, go=with_resource))

    async def items():
        for i in range(3):
            yield i

    await chain.run(items)
    assert sorted(received) == [0, 1, 2]
    # CM factory was entered (at least once — exact count depends on workers)
    assert aenter_count >= 1
    assert aexit_count == aenter_count


# ---------------------------------------------------------------------------
# Error handling under router
# ---------------------------------------------------------------------------


async def test_classifier_raise_routes_through_handler_with_router_name():
    """Classifier exception → TransformerError with stage = router's name."""
    from flowrhythm import TransformerError

    events = []

    async def on_error(event):
        events.append(event)

    async def boom_classify(x):
        raise RuntimeError(f"classifier failed on {x}")

    async def fn_a(x):
        pass

    chain = flow(
        stage(router(boom_classify, a=fn_a), name="my_router"),
        on_error=on_error,
    )

    async def items():
        for i in range(3):
            yield i

    await chain.run(items)
    assert len(events) == 3
    for e in events:
        assert isinstance(e, TransformerError)
        assert e.stage == "my_router"
        assert isinstance(e.exception, RuntimeError)


async def test_arm_raise_routes_through_handler_with_arm_name():
    """Arm exception → TransformerError with stage = '<router>.<arm_label>'."""
    from flowrhythm import TransformerError

    events = []
    handled_by_ok = []

    async def on_error(event):
        events.append(event)

    async def classify(x):
        return "fail" if x % 2 == 0 else "ok"

    async def fail_arm(x):
        raise RuntimeError(f"arm boom: {x}")

    async def ok_arm(x):
        handled_by_ok.append(x)

    chain = flow(
        stage(router(classify, fail=fail_arm, ok=ok_arm), name="r"),
        on_error=on_error,
    )

    async def items():
        for i in range(4):
            yield i

    await chain.run(items)

    # Items 0, 2 went to fail (raised); items 1, 3 went to ok (succeeded)
    assert sorted(handled_by_ok) == [1, 3]
    assert len(events) == 2
    for e in events:
        assert isinstance(e, TransformerError)
        assert e.stage == "r.fail"
        assert isinstance(e.exception, RuntimeError)
    assert sorted(e.item for e in events) == [0, 2]


# ---------------------------------------------------------------------------
# Termination through router topology
# ---------------------------------------------------------------------------


async def test_drain_while_items_in_arms_completes_cleanly():
    """drain() shuts down classifier's queue; cascade flows through all arm
    queues; merge counts down as each arm finishes; pipeline drains.
    Exercises the multi-stage cascade through a router."""
    import asyncio

    received = []
    started = asyncio.Event()
    received_lock = asyncio.Lock()

    async def classify(x):
        return "a" if x % 2 == 0 else "b"

    async def slow_arm(x):
        async with received_lock:
            received.append(x)
            if len(received) >= 3:
                started.set()
        return x

    async def collect(x):
        pass

    chain = flow(router(classify, a=slow_arm, b=slow_arm), collect)

    async def items():
        for i in range(1_000_000):
            yield i

    run_task = asyncio.create_task(chain.run(items))

    async with asyncio.timeout(2):
        await started.wait()
        await chain.drain()
        await run_task

    # Drain stopped the source; some items processed but far from 1M
    assert len(received) >= 3
    assert len(received) < 1000


async def test_stop_runs_arm_worker_cleanup():
    """stop() cancels every arm worker; their CM __aexit__ must still run.
    Verifies M5's resource-safety guarantee survives router topology."""
    from contextlib import asynccontextmanager
    import asyncio

    aenter_count = 0
    aexit_count = 0
    started = asyncio.Event()
    counts_lock = asyncio.Lock()

    @asynccontextmanager
    async def factory():
        nonlocal aenter_count, aexit_count
        async with counts_lock:
            aenter_count += 1
        started.set()
        try:
            async def fn(x):
                await asyncio.sleep(60)  # never finishes; stop cancels

            yield fn
        finally:
            async with counts_lock:
                aexit_count += 1

    async def classify(x):
        return "go"

    chain = flow(router(classify, go=factory))

    async def items():
        for i in range(1_000_000):
            yield i

    run_task = asyncio.create_task(chain.run(items))

    async with asyncio.timeout(2):
        await started.wait()
        await chain.stop()
        await run_task

    assert aenter_count >= 1
    assert aexit_count == aenter_count


# ---------------------------------------------------------------------------
# Public API exports
# ---------------------------------------------------------------------------


def test_router_is_exported():
    from flowrhythm import Router, router

    assert router is not None
    assert Router is not None
