"""Tests for M10 — `Flow.dump(mode="stats")`.

Stats mode shows live runtime stats: per-stage worker counts, queue
lengths, processed counters, errors, and aggregate event counts (drops
by reason, source errors). Two formats: text + json. Mermaid is not
supported for stats (stats aren't graph-shaped).
"""

import asyncio
import json

import pytest

from flowrhythm import Last, flow, router, stage


# ---------------------------------------------------------------------------
# Format handling
# ---------------------------------------------------------------------------


async def test_dump_stats_default_format_is_text_after_run():
    received = []

    async def fn(x):
        received.append(x)

    chain = flow(fn)

    async def items():
        for i in range(5):
            yield i

    await chain.run(items)
    out = chain.dump(mode="stats")
    assert isinstance(out, str)
    # Text format mentions stage and counters
    assert "fn" in out
    assert "5" in out  # processed count


async def test_dump_stats_json_format_after_run():
    async def fn(x):
        return x

    chain = flow(fn)

    async def items():
        for i in range(3):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    assert "active" in data
    assert "stages" in data
    assert "events" in data


async def test_dump_stats_mermaid_not_supported():
    async def fn(x):
        return x

    chain = flow(fn)
    with pytest.raises(ValueError, match="mermaid"):
        chain.dump(mode="stats", format="mermaid")


async def test_dump_stats_before_any_run_returns_empty_message():
    async def fn(x):
        return x

    chain = flow(fn)
    out = chain.dump(mode="stats")
    assert "no run" in out.lower() or "never" in out.lower()


# ---------------------------------------------------------------------------
# Per-stage counters
# ---------------------------------------------------------------------------


async def test_dump_stats_processed_count_per_stage():
    async def parse(x):
        return x * 2

    async def collect(x):
        pass

    chain = flow(parse, collect)

    async def items():
        for i in range(7):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))

    by_name = {s["name"]: s for s in data["stages"]}
    assert by_name["parse"]["processed"] == 7
    assert by_name["collect"]["processed"] == 7


async def test_dump_stats_error_count_per_stage():
    async def boom(x):
        if x % 2 == 0:
            raise RuntimeError(f"boom on {x}")
        return x

    async def collect(x):
        pass

    async def on_error(event):
        pass  # silently drop

    chain = flow(boom, collect, on_error=on_error)

    async def items():
        for i in range(6):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))

    by_name = {s["name"]: s for s in data["stages"]}
    # 3 even items raised; 3 odd items processed by boom
    assert by_name["boom"]["errors"] == 3
    assert by_name["boom"]["processed"] == 3
    # Only the 3 successful items reached collect
    assert by_name["collect"]["processed"] == 3


# ---------------------------------------------------------------------------
# Aggregate event counts
# ---------------------------------------------------------------------------


async def test_dump_stats_aggregate_transformer_errors():
    async def boom(x):
        raise RuntimeError("boom")

    async def collect(x):
        pass

    async def on_error(event):
        pass

    chain = flow(boom, collect, on_error=on_error)

    async def items():
        for i in range(4):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    # Aggregate: 4 errors total
    # The aggregate transformer_errors should match sum of stage errors
    assert data["events"]["transformer_errors"] == 4


async def test_dump_stats_aggregate_drops_by_reason_router_miss():
    async def cls(x):
        return "unknown"

    async def matched(x):
        pass

    async def on_error(event):
        pass

    chain = flow(router(cls, match=matched), on_error=on_error)

    async def items():
        for i in range(5):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    drops = data["events"]["drops"]
    assert drops.get("ROUTER_MISS", 0) == 5


async def test_dump_stats_aggregate_drops_by_reason_upstream_terminated():
    async def maybe_last(x):
        if x == 2:
            return Last("FINAL")
        return x

    async def collect(x):
        pass

    async def on_error(event):
        pass

    chain = flow(maybe_last, collect, on_error=on_error)

    async def items():
        for i in range(20):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    # Some source items were dropped after Last; >= 1 expected
    drops = data["events"]["drops"]
    assert drops.get("UPSTREAM_TERMINATED", 0) >= 1


async def test_dump_stats_aggregate_source_errors():
    async def fn(x):
        return x

    async def on_error(event):
        pass

    chain = flow(fn, on_error=on_error)

    async def items():
        yield 1
        yield 2
        raise RuntimeError("source died")

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    assert data["events"]["source_errors"] == 1


# ---------------------------------------------------------------------------
# Stats during a live run
# ---------------------------------------------------------------------------


async def test_dump_stats_active_flag_during_run():
    async def slow(x):
        await asyncio.sleep(60)

    chain = flow(slow)

    async def items():
        for i in range(1_000_000):
            yield i

    started = asyncio.Event()

    async def waiter():
        started.set()
        await asyncio.sleep(60)

    run_task = asyncio.create_task(chain.run(items))
    await asyncio.sleep(0.05)  # let some items enter the pipeline

    # Active during run
    data = json.loads(chain.dump(mode="stats", format="json"))
    assert data["active"] is True

    await chain.stop()
    await run_task

    # Inactive after stop, but stats still readable
    data = json.loads(chain.dump(mode="stats", format="json"))
    assert data["active"] is False


# ---------------------------------------------------------------------------
# Per-stage info: alive/busy/idle/queue_length presence
# ---------------------------------------------------------------------------


async def test_dump_stats_per_stage_fields_present():
    async def fn(x):
        return x

    chain = flow(fn)

    async def items():
        yield 1

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    s0 = data["stages"][0]
    assert "alive" in s0
    assert "busy" in s0
    assert "idle" in s0
    assert "queue_length" in s0
    assert "queue_drained" in s0
    assert "processed" in s0
    assert "errors" in s0


async def test_dump_stats_router_arms_have_per_stage_stats():
    async def cls(x):
        return "a" if x % 2 == 0 else "b"

    async def fn_a(x):
        return x

    async def fn_b(x):
        return x

    async def collect(x):
        pass

    chain = flow(stage(router(cls, a=fn_a, b=fn_b), name="r"), collect)

    async def items():
        for i in range(10):
            yield i

    await chain.run(items)
    data = json.loads(chain.dump(mode="stats", format="json"))
    by_name = {s["name"]: s for s in data["stages"]}
    # Each arm processed half
    assert by_name["r.a"]["processed"] == 5
    assert by_name["r.b"]["processed"] == 5
    assert by_name["collect"]["processed"] == 10


# ---------------------------------------------------------------------------
# Text format readability
# ---------------------------------------------------------------------------


async def test_dump_stats_text_includes_event_breakdown():
    async def cls(x):
        return "unknown"

    async def matched(x):
        pass

    async def on_error(event):
        pass

    chain = flow(router(cls, match=matched), on_error=on_error)

    async def items():
        for i in range(3):
            yield i

    await chain.run(items)
    out = chain.dump(mode="stats")  # text format
    assert "ROUTER_MISS" in out
    assert "drops" in out.lower()


async def test_dump_stats_text_after_clean_run_shows_processed_counts():
    async def fn(x):
        return x

    chain = flow(fn)

    async def items():
        for i in range(10):
            yield i

    await chain.run(items)
    out = chain.dump(mode="stats")
    assert "fn" in out
    assert "10" in out  # processed count
