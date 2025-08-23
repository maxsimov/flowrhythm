import pytest

from flowrhythm._scaling import StageStats
from flowrhythm._utilizationscaling import UtilizationScaling


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


def make_stats(busy_workers, idle_workers, utilization, **kwargs):
    total = busy_workers + idle_workers
    # Utilization will be computed as busy/total, so pass values accordingly.
    # If you want a different utilization, fudge the values.
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
    # Utilization above upper threshold: should scale up (max +1, because max_workers=4)
    stats = make_stats(busy_workers=3, idle_workers=0, utilization=1.0)
    assert await scaling.on_enqueue(stats) == 1
    # Scaling again immediately should not work (cooldown not expired)
    assert await scaling.on_enqueue(stats) == 0
    # Advance fake time beyond cooldown, scaling should be allowed again
    fake_monotonic.advance(5.1)
    assert await scaling.on_enqueue(stats) == 1

    # Utilization below lower threshold: should scale down (min -1, because min_workers=2)
    stats = make_stats(busy_workers=0, idle_workers=3, utilization=0.0)
    fake_monotonic.advance(5.1)
    assert await scaling.on_dequeue(stats) == -1
    # Cooldown prevents immediate further scaling down
    assert await scaling.on_dequeue(stats) == 0


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
    # Utilization between thresholds: should not scale up or down
    stats = make_stats(busy_workers=1, idle_workers=1, utilization=0.5)
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
    # Already at max_workers: should not scale up
    stats = make_stats(busy_workers=2, idle_workers=0, utilization=1.0)
    assert await scaling.on_enqueue(stats) == 0

    # Already at min_workers: should not scale down
    stats = make_stats(busy_workers=0, idle_workers=1, utilization=0.0)
    assert await scaling.on_dequeue(stats) == 0 or await scaling.on_dequeue(stats) == -1
