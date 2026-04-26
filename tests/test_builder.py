import pytest

pytest.skip(
    "Builder DSL is being replaced by flow() function. "
    "Tests will be rewritten against the new API. See ROADMAP.md.",
    allow_module_level=True,
)

from flowrhythm._builder import Builder  # noqa: E402


def make_hashable(obj):
    if isinstance(obj, list):
        return tuple(make_hashable(x) for x in obj)
    elif isinstance(obj, dict):
        return tuple(sorted((k, make_hashable(v)) for k, v in obj.items()))
    elif isinstance(obj, tuple):
        return tuple(make_hashable(x) for x in obj)
    return obj


class RecordingBackend:
    def __init__(self):
        self.calls = []
        self.defined = set()

    def set_sink(self, name, handler):
        assert name not in self.defined, f"Duplicate sink name: {name}"
        self.defined.add(name)
        self.calls.append(make_hashable(("set_sink", name, handler)))

    def add_transform(self, name, handler, target):
        assert name not in self.defined, f"Duplicate transform name: {name}"
        assert target in self.defined, (
            f"Target {target} not defined before transform {name}"
        )
        self.defined.add(name)
        self.calls.append(make_hashable(("add_transform", name, handler, target)))

    def add_branch(self, name, handler, arms):
        assert name not in self.defined, f"Duplicate branch name: {name}"
        self.defined.add(name)
        arm_names = set()
        for arm_name, target in arms:
            assert arm_name not in arm_names, (
                f"Duplicate arm name '{arm_name}' in branch '{name}'"
            )
            assert target in self.defined, (
                f"Target {target} not defined before arm {arm_name} in branch '{name}'"
            )
            arm_names.add(arm_name)
        self.calls.append(make_hashable(("add_branch", name, handler, arms)))

    def set_producer(self, name, handler, targets):
        assert name not in self.defined, f"Duplicate producer name: {name}"
        for t in targets:
            assert t in self.defined, f"Producer target {t} not defined"
        self.defined.add(name)
        self.calls.append(make_hashable(("set_producer", name, handler, targets)))


class VerificationBackend:
    def __init__(self, expected_calls):
        self.expected = list(expected_calls)
        self._recorded_calls = expected_calls

    def _pop_expected(self, *args):
        h = make_hashable(args)
        try:
            self.expected.remove(h)
        except ValueError:
            print("\n==== Verification FAILED ====")
            print("Missing call:")
            print(f"  {args}")
            print("Remaining expected calls:")
            for e in self.expected:
                print(f"  {e}")
            print("Full recorded calls:")
            for c in self._recorded_calls:
                print(f"  {c}")
            print("==== End debug ====")
            raise AssertionError(f"Call {args} not found in expected calls!")

    def set_sink(self, name, handler):
        self._pop_expected("set_sink", name, handler)

    def add_transform(self, name, handler, target):
        self._pop_expected("add_transform", name, handler, target)

    def add_branch(self, name, handler, arms):
        self._pop_expected("add_branch", name, handler, tuple(arms))

    def set_producer(self, name, handler, targets):
        self._pop_expected("set_producer", name, handler, targets)

    def check_all_called(self):
        if self.expected:
            print("\n==== Verification FAILED ====")
            print("Not all expected calls matched. Leftover:")
            for e in self.expected:
                print(f"  {e}")
            print("Full recorded calls:")
            for c in self._recorded_calls:
                print(f"  {c}")
            print("==== End debug ====")
            raise AssertionError(f"Expected calls not matched: {self.expected}")


# --- TESTS ---


@pytest.mark.asyncio
async def test_linear_chain():
    # Graph:
    #   prod -> a -> b -> s
    async def prod_fn(x):
        return f"P-{x}"

    async def fn_a(x):
        return f"A-{x}"

    async def fn_b(x):
        return f"B-{x}"

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    (
        Builder()
        .producer("prod", prod_fn)
        .then("a", fn_a)
        .then("b", fn_b)
        .sink("s", sink_fn)
        .apply(rec)
    )
    ver = VerificationBackend(rec.calls)
    ver.set_sink("s", sink_fn)
    ver.add_transform("b", fn_b, "s")
    ver.add_transform("a", fn_a, "b")
    ver.set_producer("prod", prod_fn, ["a"])
    ver.check_all_called()


@pytest.mark.asyncio
async def test_branch_two_arms():
    # Graph:
    #   prod
    #     |
    #  branch1[handler: router(x)]
    #   /     \
    #  A       B
    #  |       |
    #  a       b
    #   \     /
    #   join
    #    |
    #    s
    async def prod_fn(x):
        return f"P-{x}"

    async def branch_handler(x):
        return "A" if x else "B"

    async def fn_a(x):
        return f"A-{x}"

    async def fn_b(x):
        return f"B-{x}"

    async def join_fn(x):
        return f"J-{x}"

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    (
        Builder()
        .producer("prod", prod_fn)
        .branch("branch1", branch_handler)
        .arm("A")
        .then("a", fn_a)
        .arm("B")
        .then("b", fn_b)
        .end()
        .then("join", join_fn)
        .sink("s", sink_fn)
        .apply(rec)
    )
    ver = VerificationBackend(rec.calls)
    ver.set_sink("s", sink_fn)
    ver.add_transform("join", join_fn, "s")
    ver.add_transform("a", fn_a, "join")
    ver.add_transform("b", fn_b, "join")
    ver.add_branch("branch1", branch_handler, [("A", "a"), ("B", "b")])
    ver.set_producer("prod", prod_fn, ["branch1"])
    ver.check_all_called()


@pytest.mark.asyncio
async def test_nested_branch():
    # Graph:
    # prod
    #  |
    # branch1 [outer_handler(x)]
    #  |      \
    #  X     branch2 [inner_handler(x)]
    #  |      /     \
    # x1   Y1      Y2
    #  |    |       |
    # join y1      y2
    #  |    \      /
    #  |   branch1
    #  |    /
    #  join
    #   |
    #   s
    async def prod_fn(x):
        return f"P-{x}"

    async def outer_handler(x):
        return "X" if x else "Y"

    async def inner_handler(x):
        return "Y1" if x else "Y2"

    async def fn_x1(x):
        return f"X1-{x}"

    async def fn_y1(x):
        return f"Y1-{x}"

    async def fn_y2(x):
        return f"Y2-{x}"

    async def join_fn(x):
        return f"J-{x}"

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    (
        Builder()
        .producer("prod", prod_fn)
        .branch("branch1", outer_handler)
        .arm("X")
        .then("x1", fn_x1)
        .arm("Y")
        .branch("branch2", inner_handler)
        .arm("Y1")
        .then("y1", fn_y1)
        .arm("Y2")
        .then("y2", fn_y2)
        .end()
        .end()
        .then("join", join_fn)
        .sink("s", sink_fn)
        .apply(rec)
    )
    ver = VerificationBackend(rec.calls)
    ver.set_sink("s", sink_fn)
    ver.add_transform("join", join_fn, "s")
    ver.add_transform("x1", fn_x1, "join")
    # The targets for y1/y2 are 'branch1' as emitted by the builder, not 'join'
    ver.add_transform("y2", fn_y2, "branch1")
    ver.add_transform("y1", fn_y1, "branch1")
    ver.add_branch("branch2", inner_handler, [("Y1", "y1"), ("Y2", "y2")])
    ver.add_branch("branch1", outer_handler, [("X", "x1"), ("Y", "branch2")])
    ver.set_producer("prod", prod_fn, ["branch1"])
    ver.check_all_called()


@pytest.mark.asyncio
async def test_arm_jump():
    # Graph:
    # prod
    #  |
    # branch1 [handler]
    #  /    \
    # A      B
    # |     (jump to join)
    # a
    # |
    # join
    #  |
    #  s
    async def prod_fn(x):
        return f"P-{x}"

    async def branch_handler(x):
        return "A" if x else "B"

    async def fn_a(x):
        return f"A-{x}"

    async def join_fn(x):
        return f"J-{x}"

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    (
        Builder()
        .producer("prod", prod_fn)
        .branch("branch1", branch_handler)
        .arm("A")
        .then("a", fn_a)
        .arm("B")
        .jump("join")
        .end()
        .then("join", join_fn)
        .sink("s", sink_fn)
        .apply(rec)
    )
    ver = VerificationBackend(rec.calls)
    ver.set_sink("s", sink_fn)
    ver.add_transform("join", join_fn, "s")
    ver.add_transform("a", fn_a, "join")
    ver.add_branch("branch1", branch_handler, [("A", "a"), ("B", "join")])
    ver.set_producer("prod", prod_fn, ["branch1"])
    ver.check_all_called()


@pytest.mark.asyncio
async def test_nested_branch_with_jump():
    # Graph:
    # prod
    #  |
    # branch1 [outer_handler(x)]
    #  |      \
    #  A     branch2 [inner_handler(x)]
    #  |     /    \
    #  a   X     (jump to join)
    #  |   |
    #  |   x
    #  |   |
    #  |  join
    #  |   /
    #  join
    #   |
    #   s
    async def prod_fn(x):
        return f"P-{x}"

    async def outer_handler(x):
        return "A" if x else "B"

    async def inner_handler(x):
        return "X" if x else "Y"

    async def fn_a(x):
        return f"A-{x}"

    async def fn_x(x):
        return f"X-{x}"

    async def join_fn(x):
        return f"J-{x}"

    async def sink_fn(x):
        pass

    rec = RecordingBackend()
    (
        Builder()
        .producer("prod", prod_fn)
        .branch("branch1", outer_handler)
        .arm("A")
        .then("a", fn_a)
        .arm("B")
        .branch("branch2", inner_handler)
        .arm("X")
        .then("x", fn_x)
        .arm("Y")
        .jump("join")
        .end()
        .end()
        .then("join", join_fn)
        .sink("s", sink_fn)
        .apply(rec)
    )
    ver = VerificationBackend(rec.calls)
    ver.set_sink("s", sink_fn)
    ver.add_transform("join", join_fn, "s")
    ver.add_transform("x", fn_x, "join")
    ver.add_branch("branch2", inner_handler, [("X", "x"), ("Y", "join")])
    ver.add_transform("a", fn_a, "join")
    ver.add_branch("branch1", outer_handler, [("A", "a"), ("B", "branch2")])
    ver.set_producer("prod", prod_fn, ["branch1"])
    ver.check_all_called()
