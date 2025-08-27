# test_builder_contract.py

import pytest
from contextlib import asynccontextmanager

from flowrhythm._builder import Builder, BuilderError
from flowrhythm._types import Branch, Sink, Transformer

# --- Backend helpers ---


class RecordingBackend:
    def __init__(self):
        self.calls = []

    def set_producer(self, name, handler):
        self.calls.append(("set_producer", name, handler))

    def set_sink(self, name, handler):
        self.calls.append(("set_sink", name, handler))

    def add_transform(self, name, handler, target):
        self.calls.append(("add_transform", name, handler, target))

    def add_branch(self, name, handler, arms):
        self.calls.append(("add_branch", name, handler, arms))


class VerificationBackend:
    def __init__(self, expected_calls):
        self.calls = expected_calls
        self.idx = 0

    def _assert_call(self, *args):
        got = self.calls[self.idx]
        assert got[0] == args[0], f"Call #{self.idx}: method {got[0]} != {args[0]}"
        for i in range(1, len(args)):
            if isinstance(args[i], list):  # For arms
                assert [x[0] for x in got[i]] == [x[0] for x in args[i]], (
                    f"Call #{self.idx}: arm names {got[i]} != {args[i]}"
                )
                assert [x[1] for x in got[i]] == [x[1] for x in args[i]], (
                    f"Call #{self.idx}: arm targets {got[i]} != {args[i]}"
                )
            else:
                assert got[i] is args[i], (
                    f"Call #{self.idx}: arg {i} {got[i]!r} is not {args[i]!r}"
                )
        self.idx += 1

    def set_producer(self, name, handler):
        self._assert_call("set_producer", name, handler)

    def set_sink(self, name, handler):
        self._assert_call("set_sink", name, handler)

    def add_transform(self, name, handler, target):
        self._assert_call("add_transform", name, handler, target)

    def add_branch(self, name, handler, arms):
        self._assert_call("add_branch", name, handler, arms)

    def check_all_called(self):
        assert self.idx == len(self.calls), (
            f"Not all calls verified: {self.idx} < {len(self.calls)}"
        )


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


# --- Parametric Contract Tests ---


@pytest.mark.parametrize(
    "prod,trans,snk",
    [
        (transformer_fn, transformer_fn, sink_fn),
        (transformer_ctx, transformer_ctx, sink_ctx),
    ],
)
def test_simple_chain_param(prod, trans, snk):
    # For context managers, get concrete function objects
    async def make_handler(h):
        if hasattr(h, "__aenter__"):
            async with h() as fn:
                return fn
        return h

    import asyncio

    prod_fn = asyncio.run(make_handler(prod))
    trans_fn_a = asyncio.run(make_handler(trans))
    trans_fn_b = asyncio.run(make_handler(trans))
    sink = asyncio.run(make_handler(snk))

    rec = RecordingBackend()
    b = Builder()
    b.producer("prod", prod_fn)
    b.then("a", trans_fn_a)
    b.then("b", trans_fn_b)
    b.sink("s", sink)
    b.apply(rec)

    ver = VerificationBackend(rec.calls)
    ver.set_producer("prod", prod_fn)
    ver.add_transform("a", trans_fn_a, "a")
    ver.add_transform("b", trans_fn_b, "b")
    ver.set_sink("s", sink)
    ver.check_all_called()


@pytest.mark.parametrize(
    "branchh,snk",
    [
        (branch_fn, sink_fn),
        (branch_ctx, sink_ctx),
    ],
)
def test_branch_with_arms_then_sink_param(branchh, snk):
    import asyncio

    async def get_handler(h):
        if hasattr(h, "__aenter__"):
            async with h() as fn:
                return fn
        return h

    transformer = asyncio.run(get_handler(transformer_fn))
    branch = asyncio.run(get_handler(branchh))
    sink = asyncio.run(get_handler(snk))

    rec = RecordingBackend()
    b = Builder()
    b.producer("P", transformer)
    b.branch("B", branchh).arm("x").then("X", transformer).arm("y").jump("S").arm(
        "z"
    ).end().then("A", transformer).sink("S", sink)
    b.apply(rec)

    arms = next(c for c in rec.calls if c[0] == "add_branch" and c[1] == "B")[3]

    ver = VerificationBackend(rec.calls)
    ver.set_producer("P", transformer)
    ver.add_branch("B", branch, arms)
    ver.add_transform("X", transformer, "X")
    ver.add_transform("A", transformer, "A")
    ver.set_sink("S", sink)
    ver.check_all_called()


@pytest.mark.parametrize("branchh", [branch_fn, branch_ctx])
def test_nested_branches_param(branchh):
    import asyncio

    async def get_handler(h):
        if hasattr(h, "__aenter__"):
            async with h() as fn:
                return fn
        return h

    transformer = asyncio.run(get_handler(transformer_fn))
    branchA = asyncio.run(get_handler(branchh))
    branchB = asyncio.run(get_handler(branchh))
    sink = asyncio.run(get_handler(sink_fn))

    rec = RecordingBackend()
    b = Builder()
    b.producer("P", transformer)
    b.branch("A", branchh).arm("x").then("X", transformer).arm("y").branch(
        "B", branchh
    ).arm("z").then("Z", transformer).end().end().sink("S", sink)
    b.apply(rec)

    arms_A = next(c for c in rec.calls if c[0] == "add_branch" and c[1] == "A")[3]
    arms_B = next(c for c in rec.calls if c[0] == "add_branch" and c[1] == "B")[3]

    ver = VerificationBackend(rec.calls)
    ver.set_producer("P", transformer)
    ver.add_branch("A", branchA, arms_A)
    ver.add_transform("X", transformer, "X")
    ver.add_branch("B", branchB, arms_B)
    ver.add_transform("Z", transformer, "Z")
    ver.set_sink("S", sink)
    ver.check_all_called()


def test_jump_fn_and_ctx():
    async def transformer_fn(x):
        return x

    async def branch_fn(x):
        return "arm-x"

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    b = Builder()
    b.producer("P", transformer_fn)
    b.branch("B", branch_fn).arm("x").then("X", transformer_fn).arm("y").then(
        "Y", transformer_fn
    ).arm("z").jump("X").end().sink("S", sink_fn)
    b.apply(rec)

    arms = next(c for c in rec.calls if c[0] == "add_branch" and c[1] == "B")[3]

    ver = VerificationBackend(rec.calls)
    ver.set_producer("P", transformer_fn)
    ver.add_branch("B", branch_fn, arms)
    ver.add_transform("X", transformer_fn, "X")
    ver.add_transform("Y", transformer_fn, "Y")
    ver.set_sink("S", sink_fn)
    ver.check_all_called()


@pytest.mark.parametrize("snk", [sink_fn, sink_ctx])
def test_fallthrough_to_sink_param(snk):
    import asyncio

    async def get_handler(h):
        if hasattr(h, "__aenter__"):
            async with h() as fn:
                return fn
        return h

    transformer = asyncio.run(get_handler(transformer_fn))
    branch = asyncio.run(get_handler(branch_fn))
    sink = asyncio.run(get_handler(snk))

    rec = RecordingBackend()
    b = Builder()
    b.producer("P", transformer)
    b.branch("B", branch_fn).arm("x").end().sink("S", snk)
    b.apply(rec)

    arms = next(c for c in rec.calls if c[0] == "add_branch" and c[1] == "B")[3]

    ver = VerificationBackend(rec.calls)
    ver.set_producer("P", transformer)
    ver.add_branch("B", branch, arms)
    ver.set_sink("S", sink)
    ver.check_all_called()


def test_apply_resets_builder():
    async def transformer_fn(x):
        return x

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    b = Builder()
    b.producer("prod", transformer_fn)
    b.then("a", transformer_fn)
    b.sink("s", sink_fn)
    b.apply(rec)
    b.producer("prod2", transformer_fn)
    b.sink("s2", sink_fn)
    b.apply(rec)
    assert any(call[:2] == ("set_producer", "prod2") for call in rec.calls)


# --- Error path tests (unchanged from above, dual-backend not required) ---


def test_duplicate_producer():
    async def transformer_fn(x):
        return x

    b = Builder()
    b.producer("prod", transformer_fn)
    with pytest.raises(BuilderError):
        b.producer("prod2", transformer_fn)


def test_duplicate_node():
    async def transformer_fn(x):
        return x

    b = Builder()
    b.producer("prod", transformer_fn)
    b.then("a", transformer_fn)
    with pytest.raises(BuilderError):
        b.then("a", transformer_fn)


def test_duplicate_arm():
    async def transformer_fn(x):
        return x

    async def branch_fn(x):
        return "arm-x"

    b = Builder()
    b.producer("prod", transformer_fn)
    b.branch("B", branch_fn).arm("x")
    with pytest.raises(BuilderError):
        b.arm("x")


def test_duplicate_sink():
    async def transformer_fn(x):
        return x

    async def sink_fn(x):
        pass

    b = Builder()
    b.producer("prod", transformer_fn)
    b.sink("s1", sink_fn)
    with pytest.raises(BuilderError):
        b.sink("s2", sink_fn)


def test_jump_to_undefined():
    async def transformer_fn(x):
        return x

    async def branch_fn(x):
        return "arm-x"

    b = Builder()
    b.producer("prod", transformer_fn)
    b.branch("B", branch_fn).arm("x").jump("no_such_node")
    rec = RecordingBackend()
    with pytest.raises(BuilderError):
        b.apply(rec)


def test_branch_arm_collision():
    async def transformer_fn(x):
        return x

    async def branch_fn(x):
        return "arm-x"

    b = Builder()
    b.producer("prod", transformer_fn)
    with pytest.raises(BuilderError):
        b.branch("B", branch_fn).arm("B")


def test_branch_end_without_active():
    b = Builder()
    with pytest.raises(BuilderError):
        b.end()


def test_arm_without_branch():
    async def transformer_fn(x):
        return x

    b = Builder()
    b.producer("prod", transformer_fn)
    with pytest.raises(BuilderError):
        b.arm("x")


def test_unresolved_fallthrough_arm():
    async def transformer_fn(x):
        return x

    async def branch_fn(x):
        return "arm-x"

    b = Builder()
    b.producer("prod", transformer_fn)
    b.branch("B", branch_fn).arm("x")
    rec = RecordingBackend()
    with pytest.raises(BuilderError):
        b.apply(rec)


# --- Async handler tests, function contract (not record/verify, just handler semantics) ---


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
