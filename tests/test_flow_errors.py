"""Tests for M4 — typed-event error handling.

Errors and drops are reported to a single pipeline-level error handler as
typed events. The handler decides policy: return → continue, raise → abort.
"""

import pytest

from flowrhythm import (
    Dropped,
    DropReason,
    SourceError,
    TransformerError,
    flow,
)


# ---------------------------------------------------------------------------
# TransformerError fires when a transformer raises
# ---------------------------------------------------------------------------


async def test_transformer_error_event_fires():
    events = []

    async def boom(x):
        raise RuntimeError(f"failed on {x}")

    async def sink(x):
        pass

    async def on_error(event):
        events.append(event)

    async def items():
        for i in range(3):
            yield i

    chain = flow(boom, sink, on_error=on_error)
    await chain.run(items)

    assert len(events) == 3
    for e in events:
        assert isinstance(e, TransformerError)
        assert e.stage == "boom"
        assert isinstance(e.exception, RuntimeError)
    # All three items received as the failing input
    assert sorted(e.item for e in events) == [0, 1, 2]


# ---------------------------------------------------------------------------
# Handler returns normally → pipeline continues; failed items are dropped
# but later items still flow
# ---------------------------------------------------------------------------


async def test_handler_returns_continues_pipeline():
    received = []

    async def maybe_boom(x):
        if x == 1:
            raise RuntimeError("only x==1 fails")
        return x * 10

    async def collect(x):
        received.append(x)

    async def on_error(event):
        pass  # silently drop

    async def items():
        for i in range(5):
            yield i

    chain = flow(maybe_boom, collect, on_error=on_error)
    await chain.run(items)

    # x==1 failed; others succeeded
    assert sorted(received) == [0, 20, 30, 40]


# ---------------------------------------------------------------------------
# Handler raises → run() re-raises with the handler's exception
# ---------------------------------------------------------------------------


async def test_handler_raise_aborts_run_with_handler_exception():
    async def boom(x):
        raise RuntimeError("transformer failed")

    async def sink(x):
        pass

    class HandlerAbort(Exception):
        pass

    async def on_error(event):
        raise HandlerAbort("decided to abort")

    async def items():
        for i in range(10):
            yield i

    chain = flow(boom, sink, on_error=on_error)
    with pytest.raises(HandlerAbort, match="decided to abort"):
        await chain.run(items)


async def test_handler_can_re_raise_original_exception():
    async def boom(x):
        raise RuntimeError("original")

    async def sink(x):
        pass

    async def on_error(event):
        if isinstance(event, TransformerError):
            raise event.exception

    async def items():
        yield 1

    chain = flow(boom, sink, on_error=on_error)
    with pytest.raises(RuntimeError, match="original"):
        await chain.run(items)


# ---------------------------------------------------------------------------
# SourceError fires when source raises
# ---------------------------------------------------------------------------


async def test_source_error_event_fires():
    events = []

    async def fn(x):
        return x

    async def on_error(event):
        events.append(event)
        # don't re-raise — drain gracefully

    async def items():
        yield 0
        yield 1
        raise RuntimeError("source died")

    chain = flow(fn, on_error=on_error)
    await chain.run(items)

    assert len(events) == 1
    assert isinstance(events[0], SourceError)
    assert isinstance(events[0].exception, RuntimeError)
    assert "source died" in str(events[0].exception)


# ---------------------------------------------------------------------------
# Default handler behavior (no handler set)
# ---------------------------------------------------------------------------


async def test_default_handler_logs_transformer_error_and_continues(capsys):
    received = []

    async def maybe_boom(x):
        if x == 1:
            raise RuntimeError("boom")
        return x

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(5):
            yield i

    chain = flow(maybe_boom, collect)  # no on_error → default handler
    await chain.run(items)

    captured = capsys.readouterr()
    # Default handler logs to stderr
    assert "boom" in captured.err
    # Pipeline still runs
    assert sorted(received) == [0, 2, 3, 4]


async def test_default_handler_re_raises_source_error():
    async def fn(x):
        return x

    async def items():
        yield 0
        raise RuntimeError("source died")

    chain = flow(fn)  # default handler re-raises SourceError
    with pytest.raises(RuntimeError, match="source died"):
        await chain.run(items)


# ---------------------------------------------------------------------------
# CancelledError is NOT swallowed by error handling
# ---------------------------------------------------------------------------


async def test_cancellation_is_not_routed_through_error_handler():
    """asyncio.CancelledError must propagate; never reaches the error handler.

    If the handler caught CancelledError as a Transformer/SourceError, it
    would silently swallow cooperative cancellation. We verify by cancelling
    the run task and confirming CancelledError propagates.
    """
    import asyncio

    handler_calls = []

    async def slow(x):
        await asyncio.sleep(60)  # would block for a long time, but we cancel
        return x

    async def sink(x):
        pass

    async def on_error(event):
        handler_calls.append(event)

    async def items():
        for i in range(3):
            yield i

    chain = flow(slow, sink, on_error=on_error)
    run_task = asyncio.create_task(chain.run(items))
    await asyncio.sleep(0)  # let run start
    run_task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await run_task

    # Handler must NOT have received CancelledError as an event
    for event in handler_calls:
        if isinstance(event, TransformerError):
            assert not isinstance(event.exception, asyncio.CancelledError)
        if isinstance(event, SourceError):
            assert not isinstance(event.exception, asyncio.CancelledError)


# ---------------------------------------------------------------------------
# Constructor on_error= equivalent to set_error_handler()
# ---------------------------------------------------------------------------


async def test_on_error_constructor_kwarg_equivalent_to_method():
    events_a = []
    events_b = []

    async def boom(x):
        raise RuntimeError("boom")

    async def sink(x):
        pass

    async def handler_a(event):
        events_a.append(event)

    async def handler_b(event):
        events_b.append(event)

    async def items():
        yield 1

    # Constructor form
    chain_a = flow(boom, sink, on_error=handler_a)
    await chain_a.run(items)

    # Method form
    chain_b = flow(boom, sink)
    chain_b.set_error_handler(handler_b)
    await chain_b.run(items)

    assert len(events_a) == 1
    assert len(events_b) == 1
    assert isinstance(events_a[0], TransformerError)
    assert isinstance(events_b[0], TransformerError)


# ---------------------------------------------------------------------------
# Public API: event types are importable
# ---------------------------------------------------------------------------


async def test_event_types_exported():
    # Just verify the symbols are importable from the package
    assert TransformerError is not None
    assert SourceError is not None
    assert Dropped is not None
    assert DropReason.UPSTREAM_TERMINATED is not None
    assert DropReason.ROUTER_MISS is not None


def test_dropped_dataclass_constructable():
    # Sync test — pure dataclass construction
    d = Dropped(item=42, stage="foo", reason=DropReason.UPSTREAM_TERMINATED)
    assert d.item == 42
    assert d.stage == "foo"
    assert d.reason is DropReason.UPSTREAM_TERMINATED
