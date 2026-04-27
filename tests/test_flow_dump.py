"""Tests for M9 — `Flow.dump(mode="structure")`.

`dump()` is pure introspection — walks the expanded `_stages` list and
renders it. Three formats: text (debugging), mermaid (documentation),
JSON (programmatic).
"""

import json

from flowrhythm import (
    FixedScaling,
    UtilizationScaling,
    flow,
    priority_queue,
    router,
    stage,
)


# ---------------------------------------------------------------------------
# Default + format kwarg
# ---------------------------------------------------------------------------


async def test_dump_default_returns_text():
    async def fn(x):
        return x

    chain = flow(fn)
    out = chain.dump()
    assert isinstance(out, str)
    assert "fn" in out


async def test_dump_format_text_explicit():
    async def fn(x):
        return x

    chain = flow(fn)
    out = chain.dump(format="text")
    assert "fn" in out


async def test_dump_format_mermaid_starts_with_flowchart():
    async def fn(x):
        return x

    chain = flow(fn)
    out = chain.dump(format="mermaid")
    assert out.startswith("flowchart LR")
    assert "fn" in out


async def test_dump_format_json_is_valid_json():
    async def fn(x):
        return x

    chain = flow(fn)
    out = chain.dump(format="json")
    data = json.loads(out)
    assert "stages" in data
    assert isinstance(data["stages"], list)


async def test_dump_unknown_format_raises():
    import pytest

    async def fn(x):
        return x

    chain = flow(fn)
    with pytest.raises(ValueError, match="format"):
        chain.dump(format="xml")


async def test_dump_unknown_mode_raises():
    import pytest

    async def fn(x):
        return x

    chain = flow(fn)
    with pytest.raises(ValueError, match="mode"):
        chain.dump(mode="bogus")


# ---------------------------------------------------------------------------
# Text format — content checks
# ---------------------------------------------------------------------------


async def test_dump_text_linear_chain_lists_all_stages():
    async def parse(x):
        return x

    async def validate(x):
        return x

    async def collect(x):
        pass

    chain = flow(parse, validate, collect)
    out = chain.dump(format="text")
    assert "parse" in out
    assert "validate" in out
    assert "collect" in out
    # Stages numbered
    assert "[0]" in out
    assert "[1]" in out
    assert "[2]" in out


async def test_dump_text_shows_scaling_and_queue():
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure("fn", scaling=FixedScaling(workers=4), queue=priority_queue, queue_size=10)
    out = chain.dump(format="text")
    assert "FixedScaling" in out
    assert "workers=4" in out
    assert "priority_queue" in out
    assert "10" in out


async def test_dump_text_router_shows_arms():
    async def cls(x):
        return "a"

    async def fn_a(x):
        return x

    async def fn_b(x):
        return x

    chain = flow(stage(router(cls, a=fn_a, b=fn_b), name="r"))
    out = chain.dump(format="text")
    # Router/classifier indicator
    assert "router" in out.lower() or "classifier" in out.lower()
    assert "r.a" in out
    assert "r.b" in out
    # Arms mapping shown
    assert "a" in out and "b" in out


async def test_dump_text_router_shows_default_arm():
    async def cls(x):
        return "x"

    async def fn_a(x):
        return x

    async def fn_default(x):
        return x

    chain = flow(stage(router(cls, a=fn_a, default=fn_default), name="r"))
    out = chain.dump(format="text")
    assert "default" in out


async def test_dump_text_subflow_uses_dotted_names():
    async def parse(x):
        return x

    async def validate(x):
        return x

    async def collect(x):
        pass

    inner = flow(parse, validate)
    outer = flow(stage(inner, name="ingest"), collect)
    out = outer.dump(format="text")
    assert "ingest.parse" in out
    assert "ingest.validate" in out


async def test_dump_text_arm_end_marked_with_merge_target():
    async def cls(x):
        return "a"

    async def fn_a(x):
        return x

    async def collect(x):
        pass

    chain = flow(stage(router(cls, a=fn_a), name="r"), collect)
    out = chain.dump(format="text")
    # Arm-end stage should indicate it's an arm-end and where it merges
    assert "arm-end" in out.lower() or "merge" in out.lower()


# ---------------------------------------------------------------------------
# Mermaid format — content checks
# ---------------------------------------------------------------------------


async def test_dump_mermaid_linear_chain_has_arrows():
    async def a(x):
        return x

    async def b(x):
        return x

    async def c(x):
        return x

    chain = flow(a, b, c)
    out = chain.dump(format="mermaid")
    assert "flowchart LR" in out
    # Three nodes + 2 edges
    assert "a" in out and "b" in out and "c" in out
    assert "-->" in out


async def test_dump_mermaid_router_shows_labeled_arms():
    async def cls(x):
        return "fast"

    async def fast(x):
        return x

    async def slow(x):
        return x

    async def collect(x):
        pass

    chain = flow(stage(router(cls, fast=fast, slow=slow), name="r"), collect)
    out = chain.dump(format="mermaid")
    # Mermaid edge with label: source -->|label| target
    assert "|fast|" in out
    assert "|slow|" in out


async def test_dump_mermaid_arm_ends_converge_on_merge():
    async def cls(x):
        return "a"

    async def fn_a(x):
        return x

    async def fn_b(x):
        return x

    async def collect(x):
        pass

    chain = flow(stage(router(cls, a=fn_a, b=fn_b), name="r"), collect)
    out = chain.dump(format="mermaid")
    # Both arm-ends should have an edge to the same downstream stage
    # (collect). Count "--> s4" or similar.
    lines = [line.strip() for line in out.splitlines() if "-->" in line]
    # 2 arm dispatch edges + 2 arm-to-merge edges = 4 edges
    assert len(lines) >= 4


# ---------------------------------------------------------------------------
# JSON format — schema checks
# ---------------------------------------------------------------------------


async def test_dump_json_linear_chain_schema():
    async def parse(x):
        return x

    async def collect(x):
        pass

    chain = flow(parse, collect)
    data = json.loads(chain.dump(format="json"))

    assert "stages" in data
    assert len(data["stages"]) == 2

    s0 = data["stages"][0]
    assert s0["index"] == 0
    assert s0["name"] == "parse"
    assert s0["kind"] == "transformer"
    assert s0["downstream"] == 1
    assert "scaling" in s0
    assert "queue" in s0

    s1 = data["stages"][1]
    assert s1["index"] == 1
    assert s1["name"] == "collect"
    assert s1["downstream"] is None  # last stage = sink


async def test_dump_json_router_schema():
    async def cls(x):
        return "a"

    async def fn_a(x):
        return x

    async def fn_b(x):
        return x

    async def collect(x):
        pass

    chain = flow(stage(router(cls, a=fn_a, b=fn_b), name="r"), collect)
    data = json.loads(chain.dump(format="json"))

    # Find the classifier and arm-ends
    by_name = {s["name"]: s for s in data["stages"]}
    cls_stage = by_name["r"]
    assert cls_stage["kind"] == "classifier"
    assert cls_stage["arms"] == {"a": 1, "b": 2}
    assert cls_stage["default"] is None
    assert cls_stage["downstream"] is None

    a_stage = by_name["r.a"]
    assert a_stage["kind"] == "arm-end"
    assert a_stage["merge"] == 3  # collect's index
    assert a_stage["downstream"] == 3


async def test_dump_json_router_with_default():
    async def cls(x):
        return "x"

    async def fn_a(x):
        return x

    async def fn_default(x):
        return x

    chain = flow(stage(router(cls, a=fn_a, default=fn_default), name="r"))
    data = json.loads(chain.dump(format="json"))

    by_name = {s["name"]: s for s in data["stages"]}
    cls_stage = by_name["r"]
    assert cls_stage["arms"] == {"a": 1}
    assert cls_stage["default"] == 2  # "default" is at index 2


async def test_dump_json_includes_scaling_repr():
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure("fn", scaling=UtilizationScaling(min_workers=1, max_workers=8))
    data = json.loads(chain.dump(format="json"))
    s0 = data["stages"][0]
    assert "Utilization" in s0["scaling"]


# ---------------------------------------------------------------------------
# Stats mode — not implemented in M9; should raise
# ---------------------------------------------------------------------------


async def test_dump_stats_mode_not_yet_implemented():
    import pytest

    async def fn(x):
        return x

    chain = flow(fn)
    with pytest.raises(NotImplementedError, match="stats"):
        chain.dump(mode="stats")
