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
# Handler raise → logged, pipeline continues (observer-only contract).
# See DESIGN.md "Error handler is observer-only".
# ---------------------------------------------------------------------------


async def test_handler_raise_does_not_abort_pipeline(capsys):
    """Handler is an observer; raising is a handler bug, not a flow-control
    signal. The exception is logged; the pipeline continues; subsequent
    items are still processed."""
    received = []

    async def maybe_boom(x):
        if x == 1:
            raise RuntimeError("transformer failed on x==1")
        return x * 10

    async def collect(x):
        received.append(x)

    class HandlerAbort(Exception):
        pass

    async def on_error(event):
        raise HandlerAbort("handler bug — would have wanted to abort")

    async def items():
        for i in range(5):
            yield i

    chain = flow(maybe_boom, collect, on_error=on_error)
    # Pipeline runs to completion; HandlerAbort does NOT propagate
    await chain.run(items)

    # Failed item dropped (the one where transformer raised); others delivered
    assert sorted(received) == [0, 20, 30, 40]
    # Handler's exception was logged to stderr
    captured = capsys.readouterr()
    assert "HandlerAbort" in captured.err
    assert "handler bug" in captured.err


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


async def test_default_handler_logs_source_error_and_drains(capsys):
    """Default handler is an observer: source errors are logged but do not
    abort. The source is treated as exhausted; the pipeline drains normally."""
    received = []

    async def fn(x):
        return x

    async def collect(x):
        received.append(x)

    async def items():
        yield 100
        yield 200
        raise RuntimeError("source died after 2 items")

    chain = flow(fn, collect)  # default handler — observer-only
    await chain.run(items)  # does NOT raise

    captured = capsys.readouterr()
    assert "source failed" in captured.err
    assert "source died" in captured.err
    # Items already in flight finished
    assert sorted(received) == [100, 200]


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
