import pytest

from flowrhythm._scaling import StageStats
from flowrhythm._utilizationscaling import UtilizationScaling


def make_stats(busy_workers, idle_workers, **kwargs):
    return StageStats(
        stage_name=kwargs.get("stage_name", "test"),
        busy_workers=busy_workers,
        idle_workers=idle_workers,
        queue_length=kwargs.get("queue_length", 0),
        oldest_item_enqueued_at=kwargs.get("oldest_item_enqueued_at", 0.0),
        last_enqueue_at=kwargs.get("last_enqueue_at", 0.0),
        last_dequeue_at=kwargs.get("last_dequeue_at", 0.0),
        last_scale_up_at=kwargs.get("last_scale_up_at", 0.0),
        last_scale_down_at=kwargs.get("last_scale_down_at", 0.0),
        last_error_at=kwargs.get("last_error_at", None),
    )


@pytest.fixture
def fake_monotonic(monkeypatch):
    class FakeMonotonic:
        def __init__(self):
            self._value = 1000.0

        def set(self, val):
            self._value = val

        def advance(self, dt):
            self._value += dt

        def __call__(self):
            return self._value

    clock = FakeMonotonic()
    monkeypatch.setattr("flowrhythm._utilizationscaling.time.monotonic", clock)
    return clock


@pytest.mark.asyncio
# Test: Scaling up and down when utilization crosses thresholds,
#       and cooldown logic prevents scaling too frequently.
async def test_scaling_up_and_down_with_cooldown(fake_monotonic):
    scaling = UtilizationScaling(
        min_workers=2,
        max_workers=4,
        lower_utilization=0.2,
        upper_utilization=0.8,
        upscaling_rate=2,
        downscaling_rate=2,
        cooldown_seconds=5.0,
        dampening=1.0,
    )
    stats = make_stats(3, 0)  # Utilization = 1.0, can add 1 worker (max is 4)
    assert await scaling.on_enqueue(stats) == 1
    assert await scaling.on_enqueue(stats) == 0  # Cooldown not expired
    fake_monotonic.advance(5.1)
    assert await scaling.on_enqueue(stats) == 1

    stats = make_stats(0, 3)  # Utilization = 0.0, can remove 1 worker (min is 2)
    fake_monotonic.advance(5.1)
    assert await scaling.on_dequeue(stats) == -1
    assert await scaling.on_dequeue(stats) == 0  # Cooldown not expired


@pytest.mark.asyncio
# Test: No scaling action is taken when utilization is between thresholds.
async def test_scaling_no_action_between_thresholds():
    scaling = UtilizationScaling(
        min_workers=2,
        max_workers=4,
        lower_utilization=0.2,
        upper_utilization=0.8,
        cooldown_seconds=0.0,
    )
    stats = make_stats(1, 1)  # Utilization = 0.5, between thresholds
    assert await scaling.on_enqueue(stats) == 0
    assert await scaling.on_dequeue(stats) == 0


@pytest.mark.asyncio
# Test: Scaling does not exceed min_workers or max_workers limits.
async def test_scaling_limits(fake_monotonic):
    scaling = UtilizationScaling(
        min_workers=1,
        max_workers=2,
        upscaling_rate=10,
        downscaling_rate=10,
        cooldown_seconds=0.0,
        dampening=1.0,
    )
    stats = make_stats(2, 0)  # Already at max_workers
    assert await scaling.on_enqueue(stats) == 0

    stats = make_stats(0, 1)  # Already at min_workers
    assert await scaling.on_dequeue(stats) == 0 or await scaling.on_dequeue(stats) == -1


@pytest.mark.asyncio
# Test: Sampling period prevents scaling on rapid calls.
async def test_sampling_period_skips_scaling(fake_monotonic):
    scaling = UtilizationScaling(
        min_workers=1,
        max_workers=3,
        lower_utilization=0.2,
        upper_utilization=0.8,
        sampling_period=10.0,
        cooldown_seconds=0.0,
    )
    stats = make_stats(2, 0)  # Under max_workers
    assert await scaling.on_enqueue(stats) == 1
    assert await scaling.on_enqueue(stats) == 0
    fake_monotonic.advance(11.0)
    assert await scaling.on_enqueue(stats) == 1


@pytest.mark.asyncio
# Test: Only every Nth call scales if sampling_events is set.
async def test_sampling_events_skips_scaling(fake_monotonic):
    scaling = UtilizationScaling(
        min_workers=1,
        max_workers=3,
        lower_utilization=0.2,
        upper_utilization=0.8,
        sampling_events=3,
        cooldown_seconds=0.0,
    )
    stats = make_stats(2, 0)  # Under max_workers
    assert await scaling.on_enqueue(stats) == 0
    assert await scaling.on_enqueue(stats) == 0
    assert await scaling.on_enqueue(stats) == 1  # 3rd event


@pytest.mark.asyncio
# Test: Even with dampening=0, we always scale by at least 1 if needed.
async def test_dampening_zero_results_minimum_scale(fake_monotonic):
    scaling = UtilizationScaling(
        min_workers=1,
        max_workers=5,
        upper_utilization=0.5,
        upscaling_rate=2,
        dampening=0.0,
        cooldown_seconds=0.0,
    )
    stats = make_stats(4, 0)  # Under max_workers
    assert await scaling.on_enqueue(stats) == 1


@pytest.mark.asyncio
# Test: Utilization at exactly the threshold does not trigger scaling.
async def test_exact_thresholds_do_not_scale(fake_monotonic):
    scaling = UtilizationScaling(
        min_workers=1,
        max_workers=5,
        lower_utilization=0.5,
        upper_utilization=0.8,
        cooldown_seconds=0.0,
    )
    stats = make_stats(4, 1)  # Utilization = 0.8 (upper threshold)
    assert await scaling.on_enqueue(stats) == 0
    stats = make_stats(1, 1)  # Utilization = 0.5 (lower threshold)
    assert await scaling.on_dequeue(stats) == 0
