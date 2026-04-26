"""Tests for M2b — the Configure API.

Per-stage configuration via `Flow.configure(name, scaling=, queue=, queue_size=)`,
pipeline-wide defaults via `Flow.configure_default(...)`, and the equivalent
constructor kwargs on `flow(*, default_scaling=, default_queue=, default_queue_size=)`.

These tests focus on the **contract of the configure API** — does it store
correctly, does resolution pick the right value? They use white-box checks
of `_resolve_config()` instead of running barrier-style end-to-end tests,
which would just re-prove the multi-worker pool that's already covered in
`tests/test_flow_multiworker.py::test_workers_run_concurrently`.
"""

import pytest

from flowrhythm import FixedScaling, fifo_queue, flow, lifo_queue, priority_queue


# ---------------------------------------------------------------------------
# configure() — per-stage scaling, queue, queue_size are stored and resolved
# ---------------------------------------------------------------------------


async def test_configure_stores_and_resolves_per_stage_scaling():
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure("fn", scaling=FixedScaling(workers=4))

    assert chain._resolve_config("fn")["scaling"].workers == 4


async def test_configure_stores_and_resolves_queue_size():
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure("fn", queue_size=10)

    assert chain._resolve_config("fn")["queue_size"] == 10


@pytest.mark.parametrize("factory", [fifo_queue, lifo_queue, priority_queue])
async def test_configure_stores_and_resolves_queue_factory(factory):
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure("fn", queue=factory)

    assert chain._resolve_config("fn")["queue"] is factory


# ---------------------------------------------------------------------------
# configure_default() — pipeline-wide defaults applied to all stages
# ---------------------------------------------------------------------------


async def test_configure_default_applies_to_all_stages():
    async def a(x):
        return x

    async def b(x):
        return x

    chain = flow(a, b)
    chain.configure_default(scaling=FixedScaling(workers=4))

    assert chain._resolve_config("a")["scaling"].workers == 4
    assert chain._resolve_config("b")["scaling"].workers == 4


# ---------------------------------------------------------------------------
# Resolution order: per-stage override → pipeline default → built-in default
# ---------------------------------------------------------------------------


async def test_per_stage_overrides_default():
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure_default(scaling=FixedScaling(workers=2))
    chain.configure("fn", scaling=FixedScaling(workers=4))

    assert chain._resolve_config("fn")["scaling"].workers == 4


async def test_no_config_falls_back_to_built_in_defaults():
    """No configure or configure_default → FixedScaling(1), fifo_queue, maxsize=1."""
    async def fn(x):
        return x

    chain = flow(fn)
    cfg = chain._resolve_config("fn")
    assert isinstance(cfg["scaling"], FixedScaling)
    assert cfg["scaling"].workers == 1
    assert cfg["queue"] is fifo_queue
    assert cfg["queue_size"] == 1


# ---------------------------------------------------------------------------
# Constructor kwargs are equivalent to configure_default()
# ---------------------------------------------------------------------------


async def test_constructor_kwargs_set_defaults():
    async def fn(x):
        return x

    chain = flow(
        fn,
        default_scaling=FixedScaling(workers=4),
        default_queue=lifo_queue,
        default_queue_size=10,
    )
    cfg = chain._resolve_config("fn")
    assert cfg["scaling"].workers == 4
    assert cfg["queue"] is lifo_queue
    assert cfg["queue_size"] == 10


# ---------------------------------------------------------------------------
# Unknown stage names are silently stored (DESIGN.md open question)
# ---------------------------------------------------------------------------


async def test_configure_unknown_stage_name_silently_stored():
    """Per DESIGN.md open question: unknown name is silently stored for now.

    Will likely be revisited (warn? error?). Lock current behavior so we know
    if it changes.
    """
    async def fn(x):
        return x

    chain = flow(fn)
    chain.configure("does_not_exist", scaling=FixedScaling(workers=4))

    # Pipeline still resolves correctly for the real stage.
    assert chain._resolve_config("fn")["scaling"].workers == 1

    # Flow still runs normally.
    async def items():
        yield 1

    await chain.run(items)


# ---------------------------------------------------------------------------
# FixedScaling validation
# ---------------------------------------------------------------------------


async def test_fixed_scaling_requires_at_least_one_worker():
    """FixedScaling(workers=N) requires N >= 1 per DESIGN.md.

    For scale-to-zero, use UtilizationScaling(min_workers=0).
    """
    with pytest.raises(ValueError, match="workers >= 1"):
        FixedScaling(workers=0)

    with pytest.raises(ValueError, match="workers >= 1"):
        FixedScaling(workers=-1)


# ---------------------------------------------------------------------------
# Light integration: configure paths don't crash a real run()
# ---------------------------------------------------------------------------


async def test_configured_flow_runs_to_completion():
    """Smoke test: a flow with configure_default + per-stage configure runs end-to-end."""
    received = []

    async def double(x):
        return x * 2

    async def collect(x):
        received.append(x)

    async def items():
        for i in range(20):
            yield i

    chain = flow(double, collect, default_queue_size=5)
    chain.configure("double", scaling=FixedScaling(workers=2))
    chain.configure("collect", queue=fifo_queue, queue_size=2)

    await chain.run(items)

    assert sorted(received) == sorted(i * 2 for i in range(20))
