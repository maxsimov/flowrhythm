"""Tests for M7 — sub-flow inlining + ``stage()`` helper."""

import pytest

from flowrhythm import FixedScaling, flow, stage


# ---------------------------------------------------------------------------
# stage() helper — explicit naming for plain functions
# ---------------------------------------------------------------------------


async def test_stage_renames_function():
    async def normalize(x):
        return x

    chain = flow(stage(normalize, name="parse"))
    assert chain.stage_names == ["parse"]


async def test_stage_with_two_renamed_copies_of_same_function():
    async def fn(x):
        return x

    chain = flow(stage(fn, name="a"), stage(fn, name="b"))
    assert chain.stage_names == ["a", "b"]


async def test_stage_rejects_empty_name():
    async def fn(x):
        return x

    with pytest.raises(TypeError, match="non-empty string"):
        stage(fn, name="")


# ---------------------------------------------------------------------------
# Basic sub-flow inlining
# ---------------------------------------------------------------------------


async def test_subflow_inlined_into_parent_processes_items():
    received = []

    async def parse(x):
        return x.strip()

    async def upper(x):
        return x.upper()

    async def collect(x):
        received.append(x)

    inner = flow(parse, upper)
    outer = flow(stage(inner, name="ingest"), collect)

    async def items():
        yield "  hello  "
        yield "  world  "

    await outer.run(items)
    assert sorted(received) == ["HELLO", "WORLD"]


async def test_subflow_stage_names_are_prefixed():
    async def a(x):
        return x

    async def b(x):
        return x

    async def c(x):
        pass

    inner = flow(a, b)
    outer = flow(stage(inner, name="ing"), c)

    assert outer.stage_names == ["ing.a", "ing.b", "c"]


# ---------------------------------------------------------------------------
# Standalone vs composed: explicit configs preserved
# ---------------------------------------------------------------------------


async def test_subflow_standalone_vs_composed_same_output():
    async def double(x):
        return x * 2

    async def add_one(x):
        return x + 1

    standalone_received = []
    composed_received = []

    async def collect_a(x):
        standalone_received.append(x)

    async def collect_b(x):
        composed_received.append(x)

    async def items():
        for i in range(5):
            yield i

    inner_standalone = flow(double, add_one, collect_a)
    await inner_standalone.run(items)

    inner_for_compose = flow(double, add_one)
    outer = flow(stage(inner_for_compose, name="x"), collect_b)
    await outer.run(items)

    assert standalone_received == composed_received


# ---------------------------------------------------------------------------
# Fallback naming: _subflow_N when no stage(name=...)
# ---------------------------------------------------------------------------


async def test_subflow_fallback_naming_uses_subflow_index():
    async def a(x):
        pass

    async def b(x):
        pass

    inner = flow(a)
    outer = flow(inner, b)
    assert outer.stage_names == ["_subflow_0.a", "b"]


async def test_two_unwrapped_subflows_get_distinct_indices():
    async def a(x):
        pass

    async def b(x):
        pass

    async def c(x):
        pass

    s1 = flow(a)
    s2 = flow(b)
    outer = flow(s1, s2, c)
    assert outer.stage_names == ["_subflow_0.a", "_subflow_1.b", "c"]


# ---------------------------------------------------------------------------
# Configuration: per-stage and defaults carried over; parent overrides
# ---------------------------------------------------------------------------


async def test_subflow_per_stage_config_carries_over():
    async def parse(x):
        return x

    async def collect(x):
        pass

    inner = flow(parse)
    inner.configure("parse", scaling=FixedScaling(workers=4))

    outer = flow(stage(inner, name="x"), collect)

    cfg = outer._resolve_config("x.parse")
    assert isinstance(cfg["scaling"], FixedScaling)
    assert cfg["scaling"].workers == 4


async def test_subflow_default_baked_into_per_stage():
    """Sub-flow's defaults become explicit per-stage configs at inlining."""

    async def parse(x):
        return x

    async def validate(x):
        return x

    async def collect(x):
        pass

    inner = flow(parse, validate, default_scaling=FixedScaling(workers=4))
    outer = flow(stage(inner, name="x"), collect)

    assert outer._resolve_config("x.parse")["scaling"].workers == 4
    assert outer._resolve_config("x.validate")["scaling"].workers == 4
    # Parent stage falls back to built-in default
    assert outer._resolve_config("collect")["scaling"].workers == 1


async def test_subflow_default_does_not_leak_to_parent_stages():
    """Sub-flow's default does NOT become parent's default — autonomy."""

    async def parse(x):
        return x

    async def collect(x):
        pass

    inner = flow(parse, default_scaling=FixedScaling(workers=8))
    outer = flow(stage(inner, name="x"), collect)

    # `collect` is the parent's stage. Parent has no default. Built-in wins.
    assert outer._resolve_config("collect")["scaling"].workers == 1


async def test_parent_per_stage_overrides_subflow():
    async def parse(x):
        return x

    async def collect(x):
        pass

    inner = flow(parse)
    inner.configure("parse", scaling=FixedScaling(workers=4))

    outer = flow(stage(inner, name="x"), collect)
    outer.configure("x.parse", scaling=FixedScaling(workers=8))

    assert outer._resolve_config("x.parse")["scaling"].workers == 8


async def test_parent_default_applies_to_substage_when_subflow_set_nothing():
    """When sub-flow has no per-stage config and no default for a key, the
    parent's default fills in. This is the natural fall-through; the
    'autonomy' rule only bakes what the sub-flow actually set."""

    async def parse(x):
        return x

    async def collect(x):
        pass

    inner = flow(parse)  # no config at all
    outer = flow(
        stage(inner, name="x"), collect, default_scaling=FixedScaling(workers=5)
    )

    assert outer._resolve_config("x.parse")["scaling"].workers == 5


# ---------------------------------------------------------------------------
# Nested sub-flows: dotted names compose
# ---------------------------------------------------------------------------


async def test_nested_subflows_dotted_names_compose():
    async def a(x):
        pass

    deep = flow(a)
    middle = flow(stage(deep, name="d"))
    outer = flow(stage(middle, name="m"))

    assert outer.stage_names == ["m.d.a"]


async def test_nested_subflows_per_stage_config_propagates():
    async def parse(x):
        return x

    deep = flow(parse)
    deep.configure("parse", scaling=FixedScaling(workers=3))

    middle = flow(stage(deep, name="d"))
    outer = flow(stage(middle, name="m"))

    assert outer._resolve_config("m.d.parse")["scaling"].workers == 3


# ---------------------------------------------------------------------------
# Carve-out: sub-flow's on_error is discarded; parent's wins
# ---------------------------------------------------------------------------


async def test_subflow_on_error_is_discarded_parent_wins():
    """When composed, only the parent's error handler sees errors."""
    parent_events = []
    inner_events = []

    async def parent_handler(event):
        parent_events.append(event)

    async def inner_handler(event):
        inner_events.append(event)

    async def boom(x):
        raise RuntimeError("boom")

    async def collect(x):
        pass

    inner = flow(boom, on_error=inner_handler)
    outer = flow(stage(inner, name="x"), collect, on_error=parent_handler)

    async def items():
        yield 1

    await outer.run(items)

    # Parent received the TransformerError; inner's handler never fired
    assert len(parent_events) == 1
    assert inner_events == []
