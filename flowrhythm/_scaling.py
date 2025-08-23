from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass
class StageStats:
    stage_name: str
    busy_workers: int
    idle_workers: int
    queue_length: int
    oldest_item_enqueued_at: float
    last_enqueue_at: float
    last_dequeue_at: float
    last_scale_up_at: float
    last_scale_down_at: float
    last_error_at: Optional[float]

    @property
    def total_workers(self) -> int:
        return self.busy_workers + self.idle_workers

    @property
    def utilization(self) -> float:
        total = self.total_workers
        return (self.busy_workers / total) if total else 0.0


class ScalingStrategy(Protocol):
    async def on_enqueue(self, stats: StageStats) -> int: ...
    async def on_dequeue(self, stats: StageStats) -> int: ...


class FixedScaling:
    async def on_enqueue(self, _) -> int:
        return 0  # never scale (stub)

    async def on_dequeue(self, _) -> int:
        return 0  # never scale (stub)
