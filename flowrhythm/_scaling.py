"""Scaling strategies for stage worker pools.

A `ScalingStrategy` decides how many workers a stage runs. The runtime
queries `initial_workers()` at startup and (in M2c) calls `on_enqueue()` /
`on_dequeue()` on every item event to adjust the worker count dynamically.

Built-in strategies:
- `FixedScaling(workers=N)` — constant N workers; never scales.
- `UtilizationScaling(...)` — adjusts based on busy/idle ratio (see `_utilizationscaling.py`).
"""

from dataclasses import dataclass
from typing import Optional, Protocol


@dataclass
class StageSnapshot:
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
    """Protocol for stage worker-pool sizing strategies.

    All methods are **synchronous** by design. Scaling decisions are called
    on every item event (potentially millions/sec); awaiting external
    services here would block the hot path. If a strategy needs external
    state, refresh it in a background task and read it synchronously here.
    """

    def initial_workers(self) -> int:
        """Number of workers to spawn when the stage starts."""

    def on_enqueue(self, stats: StageSnapshot) -> int:
        """Return delta to apply on item-enqueue events. Positive = add, negative = remove."""

    def on_dequeue(self, stats: StageSnapshot) -> int:
        """Return delta to apply on item-dequeue events. Positive = add, negative = remove."""


class FixedScaling:
    """Constant N workers per stage. Never scales after startup.

    Use `UtilizationScaling` if you want dynamic scaling (including scale-to-zero).
    """

    def __init__(self, workers: int) -> None:
        if workers < 1:
            raise ValueError(
                f"FixedScaling requires workers >= 1 (got {workers}); "
                "use UtilizationScaling(min_workers=0) for scale-to-zero"
            )
        self.workers = workers

    def initial_workers(self) -> int:
        return self.workers

    def on_enqueue(self, stats: StageSnapshot) -> int:
        return 0  # never scales

    def on_dequeue(self, stats: StageSnapshot) -> int:
        return 0  # never scales
