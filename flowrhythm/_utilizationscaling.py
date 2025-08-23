import time
from typing import Optional

from ._scaling import StageStats


class UtilizationScaling:
    def __init__(
        self,
        min_workers: int = 1,
        max_workers: int = 8,
        lower_utilization: float = 0.2,
        upper_utilization: float = 0.8,
        upscaling_rate: int = 1,
        downscaling_rate: int = 1,
        cooldown_seconds: float = 5.0,
        dampening: float = 0.5,
        sampling_period: Optional[float] = None,  # seconds
        sampling_events: Optional[int] = None,  # events
    ):
        self.min_workers = min_workers
        self.max_workers = max_workers
        self.lower_utilization = lower_utilization
        self.upper_utilization = upper_utilization
        self.upscaling_rate = upscaling_rate
        self.downscaling_rate = downscaling_rate
        self.cooldown_seconds = cooldown_seconds
        self.dampening = dampening
        self.sampling_period = sampling_period
        self.sampling_events = sampling_events

        self._last_scaled_at = 0.0
        self._last_sampled_at = 0.0
        self._event_count = 0

    def _should_sample(self):
        now = time.monotonic()
        if self.sampling_events is not None:
            self._event_count += 1
            if self._event_count < self.sampling_events:
                return False
            self._event_count = 0
        if self.sampling_period is not None:
            if now - self._last_sampled_at < self.sampling_period:
                return False
            self._last_sampled_at = now
        return True

    def _should_cooldown(self):
        now = time.monotonic()
        if now - self._last_scaled_at < self.cooldown_seconds:
            return False
        return True

    def _record_scale(self):
        self._last_scaled_at = time.monotonic()

    async def on_enqueue(self, stats: StageStats) -> int:
        return await self._scale(stats)

    async def on_dequeue(self, stats: StageStats) -> int:
        return await self._scale(stats)

    async def _scale(self, stats: StageStats) -> int:
        # Sampling
        if (
            self.sampling_events is not None or self.sampling_period is not None
        ) and not self._should_sample():
            return 0

        # Cooldown
        if not self._should_cooldown():
            return 0

        workers = stats.total_workers
        util = stats.utilization

        if util > self.upper_utilization and workers < self.max_workers:
            n_add = min(self.upscaling_rate, self.max_workers - workers)
            n_add = int(n_add * self.dampening) or 1
            self._record_scale()
            return n_add

        if util < self.lower_utilization and workers > self.min_workers:
            n_remove = min(self.downscaling_rate, workers - self.min_workers)
            n_remove = int(n_remove * self.dampening) or 1
            self._record_scale()
            return -n_remove

        return 0
