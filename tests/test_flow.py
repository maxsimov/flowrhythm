"""Tests for M1 — the minimum linear flow runtime.

Covers: flow(*stages) construction, validation, auto-naming, and
Flow.run(source) for plain async functions with single-worker stages.
"""

import pytest

from flowrhythm import Flow, flow


# ---------------------------------------------------------------------------
# Smoke test — the M1 milestone goal
# ---------------------------------------------------------------------------


async def test_linear_flow_processes_items_in_order():
    received = []

    async def double(x):
        return x * 2

    async def add_one(x):
        return x + 1

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(100):
            yield i

    chain = flow(double, add_one, collect)
    await chain.run(items)

    expected = [(i * 2) + 1 for i in range(100)]
    assert received == expected


# ---------------------------------------------------------------------------
# Construction returns a Flow instance
# ---------------------------------------------------------------------------


async def test_flow_returns_Flow_instance():
    async def fn(x):
        return x

    chain = flow(fn, fn)
    assert isinstance(chain, Flow)


async def test_flow_requires_at_least_one_stage():
    with pytest.raises(TypeError, match="at least one stage"):
        flow()


# ---------------------------------------------------------------------------
# Validation: reject async generators in flow() args
# ---------------------------------------------------------------------------


async def test_flow_rejects_async_generator_at_any_position():
    async def fn(x):
        return x

    async def items():
        yield 1

    with pytest.raises(TypeError, match="async generator"):
        flow(items, fn)

    with pytest.raises(TypeError, match="async generator"):
        flow(fn, items)


# ---------------------------------------------------------------------------
# Validation: reject sync functions
# ---------------------------------------------------------------------------


async def test_flow_rejects_sync_function():
    def sync_fn(x):
        return x

    async def fn(x):
        return x

    with pytest.raises(TypeError, match="async"):
        flow(sync_fn, fn)


async def test_flow_rejects_non_callable():
    async def fn(x):
        return x

    with pytest.raises(TypeError, match="callable"):
        flow("not a function", fn)


async def test_flow_rejects_wrong_arity():
    async def two_args(x, y):
        return x

    async def fn(x):
        return x

    with pytest.raises(TypeError, match="0 args.*or 1 arg"):
        flow(two_args, fn)


# ---------------------------------------------------------------------------
# Auto-naming: stage names from function names; numeric suffix on collision
# ---------------------------------------------------------------------------


async def test_auto_naming_uses_function_name():
    async def parse(x):
        return x

    async def write(x):
        return x

    chain = flow(parse, write)
    assert chain.stage_names == ["parse", "write"]


async def test_collision_gets_numeric_suffix():
    async def normalize(x):
        return x

    async def sink(x):
        return x

    chain = flow(normalize, normalize, normalize, sink)
    assert chain.stage_names == ["normalize", "normalize_2", "normalize_3", "sink"]


# ---------------------------------------------------------------------------
# Flow.run() validation
# ---------------------------------------------------------------------------


async def test_run_rejects_called_generator():
    async def fn(x):
        return x

    async def items():
        yield 1

    chain = flow(fn)
    with pytest.raises(TypeError, match="generator function"):
        await chain.run(items())  # ← user passed items() instead of items


async def test_run_rejects_non_generator_function():
    async def fn(x):
        return x

    async def not_a_generator():
        return [1, 2, 3]  # plain async fn, not async gen

    chain = flow(fn)
    with pytest.raises(TypeError, match="async generator"):
        await chain.run(not_a_generator)


# ---------------------------------------------------------------------------
# End-to-end behavior — single stage, two stages, empty source, etc.
# ---------------------------------------------------------------------------


async def test_single_stage_acts_as_sink():
    received = []

    async def collect(x):
        received.append(x)

    async def items():
        yield 1
        yield 2
        yield 3

    chain = flow(collect)
    await chain.run(items)

    assert received == [1, 2, 3]


async def test_two_stage_flow():
    received = []

    async def square(x):
        return x * x

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(5):
            yield i

    chain = flow(square, collect)
    await chain.run(items)

    assert received == [0, 1, 4, 9, 16]


async def test_empty_source_returns_immediately():
    received = []

    async def collect(x):
        received.append(x)

    async def items():
        return
        yield  # unreachable, makes it an async generator

    chain = flow(collect)
    await chain.run(items)

    assert received == []
