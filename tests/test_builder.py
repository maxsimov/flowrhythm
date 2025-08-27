from contextlib import asynccontextmanager
from typing import Hashable

import pytest

from flowrhythm._builder import Backend, Builder, BuilderError
from flowrhythm._types import Branch, Sink, Transformer


@pytest.fixture
def dummy_backend():
    class DummyBackend:
        def set_producer(self, name, handler):
            pass

        def set_sink(self, name, handler):
            pass

        def add_transform(self, name, handler, target):
            pass

        def add_branch(self, name, handler, arms):
            pass

    return DummyBackend()


@pytest.fixture
def mock_backend():
    calls = []

    class MockBackend:
        def set_producer(self, name, handler):
            calls.append(("set_producer", name, handler))

        def set_sink(self, name, handler):
            calls.append(("set_sink", name, handler))

        def add_transform(self, name, handler, target):
            calls.append(("add_transform", name, handler, target))

        def add_branch(self, name, handler, arms):
            calls.append(("add_branch", name, handler, arms))

    return MockBackend(), calls


# --- Handler Factories: both function and context manager styles ---


async def transformer_fn(x):
    return x


@asynccontextmanager
async def transformer_ctx():
    async def fn(x):
        return x

    yield fn


async def sink_fn(x):
    pass


@asynccontextmanager
async def sink_ctx():
    async def fn(x):
        pass

    yield fn


async def branch_fn(x):
    return "arm-x"


@asynccontextmanager
async def branch_ctx():
    async def fn(x):
        return "arm-x"

    yield fn


# --- Tests: Core Builder Usage ---


@pytest.mark.parametrize(
    "prod,trans,snk,branchh",
    [
        (transformer_fn, transformer_fn, sink_fn, branch_fn),
        (transformer_ctx, transformer_ctx, sink_ctx, branch_ctx),
    ],
)
def test_simple_chain(prod, trans, snk, branchh, mock_backend):
    backend, calls = mock_backend
    b = Builder()
    b.producer("prod", prod)
    b.then("a", trans)
    b.then("b", trans)
    b.sink("s", snk)
    b.apply(backend)
    assert calls[0][0] == "set_producer"
    assert calls[-1][0] == "set_sink"
    assert len([c for c in calls if c[0] == "add_transform"]) == 2


@pytest.mark.parametrize("branchh,snk", [(branch_fn, sink_fn), (branch_ctx, sink_ctx)])
def test_branch_with_arms_then_sink(branchh, snk, mock_backend):
    backend, calls = mock_backend
    b = Builder()
    b.producer("P", transformer_fn)
    b.branch("B", branchh).arm("x").then("X", transformer_fn).arm("y").jump("S").arm(
        "z"
    ).end().then("A", transformer_fn).sink("S", snk)
    b.apply(backend)
    # Look up the add_branch call
    branch_call = next(c for c in calls if c[0] == "add_branch" and c[1] == "B")
    arms = dict(branch_call[3])
    # "x" → "X", "y" → "S", "z" → "A"
    assert arms["x"] == "X"
    assert arms["y"] == "S"
    assert arms["z"] == "A"


@pytest.mark.parametrize("branchh", [branch_fn, branch_ctx])
def test_nested_branches(branchh, mock_backend):
    backend, calls = mock_backend
    b = Builder()
    b.producer("P", transformer_fn)
    b.branch("A", branchh).arm("x").then("X", transformer_fn).arm("y").branch(
        "B", branchh
    ).arm("z").then("Z", transformer_fn).end().end().sink("S", sink_fn)
    b.apply(backend)
    assert any(c[0] == "add_branch" and c[1] == "A" for c in calls)
    assert any(c[0] == "add_branch" and c[1] == "B" for c in calls)


def test_jump_fn_and_ctx(mock_backend):
    backend, calls = mock_backend
    b = Builder()
    b.producer("P", transformer_fn)
    b.branch("B", branch_fn).arm("x").then("X", transformer_fn).arm("y").then(
        "Y", transformer_fn
    ).arm("z").jump("X").end().sink("S", sink_fn)
    b.apply(backend)
    assert any(
        c[0] == "add_branch" and any(a[0] == "z" and a[1] == "X" for a in c[3])
        for c in calls
    )


# --- Tests: Error Paths ---


def test_duplicate_producer():
    b = Builder()
    b.producer("prod", transformer_fn)
    with pytest.raises(BuilderError):
        b.producer("prod2", transformer_fn)


def test_duplicate_node():
    b = Builder()
    b.producer("prod", transformer_fn)
    b.then("a", transformer_fn)
    with pytest.raises(BuilderError):
        b.then("a", transformer_fn)


def test_duplicate_arm():
    b = Builder()
    b.producer("prod", transformer_fn)
    b.branch("B", branch_fn).arm("x")
    with pytest.raises(BuilderError):
        b.arm("x")


def test_duplicate_sink():
    b = Builder()
    b.producer("prod", transformer_fn)
    b.sink("s1", sink_fn)
    with pytest.raises(BuilderError):
        b.sink("s2", sink_fn)


def test_jump_to_undefined(dummy_backend):
    b = Builder()
    b.producer("prod", transformer_fn)
    b.branch("B", branch_fn).arm("x").jump("no_such_node")
    with pytest.raises(BuilderError):
        b.apply(dummy_backend)


def test_branch_arm_collision():
    b = Builder()
    b.producer("prod", transformer_fn)
    with pytest.raises(BuilderError):
        b.branch("B", branch_fn).arm("B")


def test_branch_end_without_active():
    b = Builder()
    with pytest.raises(BuilderError):
        b.end()


def test_arm_without_branch():
    b = Builder()
    b.producer("prod", transformer_fn)
    with pytest.raises(BuilderError):
        b.arm("x")


def test_unresolved_fallthrough_arm(dummy_backend):
    b = Builder()
    b.producer("prod", transformer_fn)
    b.branch("B", branch_fn).arm("x")
    with pytest.raises(BuilderError):
        b.apply(dummy_backend)


@pytest.mark.parametrize("snk", [sink_fn, sink_ctx])
def test_fallthrough_to_sink(snk, mock_backend):
    backend, calls = mock_backend
    b = Builder()
    b.producer("P", transformer_fn)
    b.branch("B", branch_fn).arm("x").end().sink("S", snk)
    b.apply(backend)
    assert any(
        c[0] == "add_branch"
        and c[1] == "B"
        and any(a[0] == "x" and a[1] == "S" for a in c[3])
        for c in calls
    )


def test_apply_resets_builder(mock_backend):
    backend, calls = mock_backend
    b = Builder()
    b.producer("prod", transformer_fn)
    b.then("a", transformer_fn)
    b.sink("s", sink_fn)
    b.apply(backend)
    b.producer("prod2", transformer_fn)
    b.sink("s2", sink_fn)
    b.apply(backend)
    assert any(call[:2] == ("set_producer", "prod2") for call in calls)


# --- Tests for context managers as handlers, async runtime checks ---



@pytest.mark.asyncio
async def test_asynccontextmanager_transformer():
    @asynccontextmanager
    async def my_ctx():
        async def fn(x):
            return x

        yield fn

    async with my_ctx() as fn:
        assert await fn(123) == 123


@pytest.mark.asyncio
async def test_asynccontextmanager_sink():
    @asynccontextmanager
    async def my_ctx():
        async def fn(x):
            return None

        yield fn

    async with my_ctx() as fn:
        assert await fn("foo") is None


@pytest.mark.asyncio
async def test_asynccontextmanager_branch():
    @asynccontextmanager
    async def my_ctx():
        async def fn(x):
            return "abc"

        yield fn

    async with my_ctx() as fn:
        assert await fn(None) == "abc"
