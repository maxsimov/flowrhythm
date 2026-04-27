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


async def test_router_arm_with_nested_router_raises():
    """Using a Flow that contains a router as a router arm is not yet
    supported (M8 limitation); raises NotImplementedError with a clear hint."""
    import pytest

    async def cls_outer(x):
        return "a"

    async def cls_inner(x):
        return "x"

    async def x(x):
        pass

    inner_with_router = flow(router(cls_inner, x=x))

    with pytest.raises(NotImplementedError, match="nested"):
        flow(router(cls_outer, a=inner_with_router))


# ---------------------------------------------------------------------------
# Public API exports
# ---------------------------------------------------------------------------


def test_router_is_exported():
    from flowrhythm import Router, router

    assert router is not None
    assert Router is not None
