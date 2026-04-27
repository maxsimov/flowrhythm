"""flowrhythm — asyncio-native stream processing pipelines.

This package is mid-rebuild. The public API documented in `README.md` is the
target; only a few primitives are wired up so far. See `todos/migrate-to-flow.md`
for the implementation plan.
"""

from ._errors import (
    Dropped,
    DropReason,
    SourceError,
    TransformerError,
)
from ._flow import Flow, Last, PushHandle, flow, stage, sync_stage
from ._queue import (
    AsyncQueueFactory,
    AsyncQueueInterface,
    fifo_queue,
    lifo_queue,
    priority_queue,
)
from ._scaling import (
    FixedScaling,
    ScalingStrategy,
    StageSnapshot,
)
from ._utilizationscaling import UtilizationScaling

__all__ = [
    "AsyncQueueFactory",
    "AsyncQueueInterface",
    "DropReason",
    "Dropped",
    "FixedScaling",
    "Flow",
    "Last",
    "PushHandle",
    "ScalingStrategy",
    "SourceError",
    "StageSnapshot",
    "TransformerError",
    "UtilizationScaling",
    "fifo_queue",
    "flow",
    "lifo_queue",
    "priority_queue",
    "stage",
    "sync_stage",
]
