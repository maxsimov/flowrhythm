"""Tests for router naming improvements (plan: router-naming.md).

Precedence (most specific wins):
  1. stage(router(...), name="X")           → "X"
  2. router(..., name="Y")                  → "Y"
  3. classifier.__name__ (if not <lambda>)  → classifier's own name
  4. fallback                               → "_router_N"
"""

import pytest

from flowrhythm import flow, router, stage


# ---------------------------------------------------------------------------
# Auto-derive from classifier
# ---------------------------------------------------------------------------


async def test_router_auto_derives_name_from_classifier():
    async def classify_videos(x):
        return "a"

    async def fn_a(x):
        pass

    chain = flow(router(classify_videos, a=fn_a))
    assert "classify_videos" in chain.stage_names
    assert "classify_videos.a" in chain.stage_names


async def test_router_lambda_classifier_falls_back_to_indexed_name():
    """A lambda has __name__ == '<lambda>' which is ugly. Use the
    indexed fallback."""
    import asyncio

    async def fn_a(x):
        pass

    async_lambda = lambda x: asyncio.sleep(0, result="a")  # noqa: E731
    # Need a coroutine function, so wrap:
    async def lambda_classifier(x):
        return "a"

    lambda_classifier.__name__ = "<lambda>"  # simulate lambda-derived

    chain = flow(router(lambda_classifier, a=fn_a))
    assert "_router_0" in chain.stage_names
    assert "_router_0.a" in chain.stage_names


# ---------------------------------------------------------------------------
# Explicit name= kwarg on router()
# ---------------------------------------------------------------------------


async def test_router_explicit_name_kwarg():
    async def classify(x):
        return "a"

    async def fn_a(x):
        pass

    chain = flow(router(classify, name="dispatch", a=fn_a))
    assert "dispatch" in chain.stage_names
    assert "dispatch.a" in chain.stage_names
    # Classifier name NOT used because explicit name= won
    assert "classify" not in chain.stage_names


async def test_router_name_kwarg_with_default():
    """`name` and `default` are both reserved kwargs. Both work together."""

    async def classify(x):
        return "x"

    async def fn_a(x):
        pass

    async def fn_default(x):
        pass

    chain = flow(router(classify, name="r", a=fn_a, default=fn_default))
    assert "r" in chain.stage_names
    assert "r.a" in chain.stage_names
    assert "r.default" in chain.stage_names


# ---------------------------------------------------------------------------
# Precedence
# ---------------------------------------------------------------------------


async def test_stage_name_beats_router_name_beats_classifier_name():
    """Most-specific wins: stage() > router(name=) > classifier."""

    async def classify_things(x):
        return "a"

    async def fn_a(x):
        pass

    # All three forms in one place
    chain = flow(
        stage(
            router(classify_things, name="from_router_kwarg", a=fn_a),
            name="from_stage_wrapper",
        )
    )
    # stage(name=) wins
    assert "from_stage_wrapper" in chain.stage_names
    assert "from_stage_wrapper.a" in chain.stage_names
    # The router-name-kwarg form is shadowed
    assert "from_router_kwarg" not in chain.stage_names
    assert "classify_things" not in chain.stage_names


async def test_router_name_kwarg_beats_classifier_name():
    async def classify_things(x):
        return "a"

    async def fn_a(x):
        pass

    chain = flow(router(classify_things, name="explicit", a=fn_a))
    assert "explicit" in chain.stage_names
    assert "classify_things" not in chain.stage_names


# ---------------------------------------------------------------------------
# Collisions: two routers with the same classifier name → suffix
# ---------------------------------------------------------------------------


async def test_two_routers_with_same_classifier_name_get_collision_suffix():
    async def classify(x):
        return "a"

    async def fn_a(x):
        pass

    async def fn_b(x):
        pass

    async def collect(x):
        pass

    # Two routers, both auto-derive to "classify"; second gets _2 suffix
    chain = flow(
        router(classify, a=fn_a),
        router(classify, b=fn_b),
        collect,
    )
    names = chain.stage_names
    assert "classify" in names
    assert "classify_2" in names
    assert "classify.a" in names
    assert "classify_2.b" in names


async def test_two_unnamed_routers_with_lambda_classifier_get_indexed():
    """Both routers fall back to _router_N; counter ensures distinct names."""

    async def cls1(x):
        return "a"

    async def cls2(x):
        return "b"

    cls1.__name__ = "<lambda>"
    cls2.__name__ = "<lambda>"

    async def fn_a(x):
        pass

    async def fn_b(x):
        pass

    async def collect(x):
        pass

    chain = flow(router(cls1, a=fn_a), router(cls2, b=fn_b), collect)
    assert "_router_0" in chain.stage_names
    assert "_router_1" in chain.stage_names


# ---------------------------------------------------------------------------
# Validation: empty name=
# ---------------------------------------------------------------------------


def test_router_rejects_empty_name():
    async def classify(x):
        return "a"

    async def fn_a(x):
        pass

    with pytest.raises(TypeError, match="name"):
        router(classify, name="", a=fn_a)


# ---------------------------------------------------------------------------
# Sanity: routing still works with auto-derived name
# ---------------------------------------------------------------------------


async def test_routing_works_with_auto_derived_name():
    received = []

    async def my_classifier(x):
        return "even" if x % 2 == 0 else "odd"

    async def even(x):
        received.append(("even", x))

    async def odd(x):
        received.append(("odd", x))

    chain = flow(router(my_classifier, even=even, odd=odd))

    async def items():
        for i in range(4):
            yield i

    await chain.run(items)
    assert ("even", 0) in received
    assert ("even", 2) in received
    assert ("odd", 1) in received
    assert ("odd", 3) in received
