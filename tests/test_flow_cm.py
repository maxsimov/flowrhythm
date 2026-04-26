"""Tests for M3 — CM factory transformers and the sync_stage helper.

CM factories give each worker its own per-worker resource lifecycle:
__aenter__ runs once when the worker spawns, the yielded callable handles
each item, __aexit__ runs when the worker stops.
"""

import asyncio
from contextlib import asynccontextmanager

import pytest

from flowrhythm import FixedScaling, flow, sync_stage


# ---------------------------------------------------------------------------
# CM factory: per-worker isolation
# ---------------------------------------------------------------------------


async def test_each_worker_gets_its_own_cm_instance():
    """4 workers → factory called 4 times → 4 distinct yielded callables."""
    instances = []
    instances_lock = asyncio.Lock()

    @asynccontextmanager
    async def factory():
        marker = object()
        async with instances_lock:
            instances.append(marker)

        async def fn(x):
            return (marker, x)

        yield fn

    received = []
    received_lock = asyncio.Lock()

    async def collect(pair):
        async with received_lock:
            received.append(pair)

    async def items():
        for i in range(20):
            yield i

    chain = flow(factory, collect, default_scaling=FixedScaling(workers=4))
    await chain.run(items)

    # Exactly 4 factory invocations — one per worker
    assert len(instances) == 4
    # Every result references one of those 4 instances
    seen_markers = {marker for marker, _ in received}
    assert seen_markers <= set(instances)
    # All items processed
    assert sorted(item for _, item in received) == list(range(20))


# ---------------------------------------------------------------------------
# Class-based CM factory: cls() per worker
# ---------------------------------------------------------------------------


async def test_class_based_cm_factory_instantiated_per_worker():
    """Class with __aenter__/__aexit__ and no-arg __init__ works as factory."""
    instances = []

    class Doubler:
        def __init__(self):
            instances.append(self)

        async def __aenter__(self):
            async def fn(x):
                return x * 2

            return fn

        async def __aexit__(self, *exc):
            pass

    received = []
    received_lock = asyncio.Lock()

    async def collect(x):
        async with received_lock:
            received.append(x)

    async def items():
        for i in range(15):
            yield i

    chain = flow(Doubler, collect, default_scaling=FixedScaling(workers=3))
    await chain.run(items)

    assert len(instances) == 3
    assert sorted(received) == sorted(i * 2 for i in range(15))


# ---------------------------------------------------------------------------
# __aexit__ runs reliably
# ---------------------------------------------------------------------------


async def test_aexit_runs_on_normal_drain():
    """All worker CMs exit cleanly when the source exhausts."""
    enter_count = 0
    exit_count = 0
    counts_lock = asyncio.Lock()

    @asynccontextmanager
    async def factory():
        nonlocal enter_count, exit_count
        async with counts_lock:
            enter_count += 1

        async def fn(x):
            return x

        yield fn
        async with counts_lock:
            exit_count += 1

    async def sink(x):
        pass

    async def items():
        for i in range(10):
            yield i

    chain = flow(factory, sink, default_scaling=FixedScaling(workers=4))
    await chain.run(items)

    assert enter_count == 4
    assert exit_count == 4


async def test_aexit_runs_when_transformer_raises():
    """__aexit__ must run even if the user's transformer raises (so resources
    aren't leaked). The exception propagates as a worker death; the rest of
    the pipeline still drains via the cascade.

    Two equivalent CM patterns are tested below — class-based (explicit
    __aexit__) and @asynccontextmanager-with-try/finally (the user pattern
    that mirrors the cleanup intent). Both observe `cleanup_count == 2`
    (one per worker) after the run.

    Note: M4 will route exceptions through the typed-events error handler.
    For now, the worker dies but its CM cleanup is honored.
    """
    cleanup_count = 0
    counts_lock = asyncio.Lock()

    @asynccontextmanager
    async def factory():
        nonlocal cleanup_count

        async def fn(x):
            raise RuntimeError("boom")

        try:
            yield fn
        finally:
            # try/finally guarantees this runs even when the framework
            # throws the user's exception back into the generator at `yield`
            async with counts_lock:
                cleanup_count += 1

    async def sink(x):
        pass

    async def items():
        for i in range(3):
            yield i

    chain = flow(factory, sink, default_scaling=FixedScaling(workers=2))
    # M2c doesn't surface exceptions; asyncio logs them. Pipeline still drains.
    await chain.run(items)

    # Both workers' cleanup ran despite RuntimeError in the transformer
    assert cleanup_count == 2


async def test_class_based_aexit_runs_on_exception():
    """Class-based CM: __aexit__ is unconditionally called by the framework
    on exception (no user try/finally needed)."""
    exit_count = 0
    exit_count_lock = asyncio.Lock()

    class Factory:
        async def __aenter__(self):
            async def fn(x):
                raise RuntimeError("boom")

            return fn

        async def __aexit__(self, *exc):
            nonlocal exit_count
            async with exit_count_lock:
                exit_count += 1

    async def sink(x):
        pass

    async def items():
        for i in range(3):
            yield i

    chain = flow(Factory, sink, default_scaling=FixedScaling(workers=2))
    await chain.run(items)

    assert exit_count == 2


# ---------------------------------------------------------------------------
# Plain function regression — still works after CM normalization
# ---------------------------------------------------------------------------


async def test_plain_function_still_works():
    """Plain async transformers (1 arg) work the same as before M3."""
    received = []

    async def double(x):
        return x * 2

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(10):
            yield i

    chain = flow(double, collect)
    await chain.run(items)

    assert received == [i * 2 for i in range(10)]


# ---------------------------------------------------------------------------
# sync_stage helper
# ---------------------------------------------------------------------------


async def test_sync_stage_wraps_sync_function():
    """sync_stage(fn) wraps a sync function to run via asyncio.to_thread."""
    received = []

    def sync_double(x):  # NOT async
        return x * 2

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(5):
            yield i

    chain = flow(sync_stage(sync_double), collect)
    await chain.run(items)

    assert received == [i * 2 for i in range(5)]


async def test_sync_stage_preserves_function_name_for_auto_naming():
    """sync_stage preserves __name__ so auto-naming uses the original."""

    def parse(x):
        return x

    async def sink(x):
        pass

    chain = flow(sync_stage(parse), sink)
    assert chain.stage_names == ["parse", "sink"]


# ---------------------------------------------------------------------------
# Validation: sync 1-arg function without sync_stage gets a clear error
# ---------------------------------------------------------------------------


async def test_sync_function_rejected_with_helpful_message():
    """Bare sync function (1 arg) is rejected; user is pointed to sync_stage."""

    def sync_fn(x):
        return x

    async def sink(x):
        pass

    with pytest.raises(TypeError, match="sync_stage"):
        flow(sync_fn, sink)
