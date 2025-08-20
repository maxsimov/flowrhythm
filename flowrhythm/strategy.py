import asyncio
import enum
import logging

from .capacity import UtilizationCapacity
from .job import _Job, _JobState

logger = logging.getLogger(__name__)


class _UtilizationStrategy:
    def __init__(self):
        pass

    def name(self):
        return "UtilizationStrategy"

    def default_capacity(self):
        return UtilizationCapacity()

    async def __call__(self, job: _Job):
        last_ts = job.timestamp()
        while True:
            assert job._cap is UtilizationCapacity
            await asyncio.sleep(job._cap.cooldown)
            job._log.debug("cooldown finished")
            lower_threshold_time = None
            upper_threshold_time = None
            while True:
                await asyncio.sleep(job._cap.sampling)
                async with job._state_cond:
                    if job._state != _JobState.NORMAL:
                        job._log.debug("scaling has been stopped due to last work item")
                        return

                threshold = job.get_current_utilization()
                ts = job.timestamp()

                if ts - last_ts >= 2.0:
                    last_ts = ts
                    job._log.debug(
                        "threshold=%.2f workers=%d", threshold, len(job._workers)
                    )

                if threshold < job._cap.lower_threshold:
                    upper_threshold_time = None
                    lower_threshold_time = lower_threshold_time or ts
                    if (ts - lower_threshold_time) < job._cap.dampening:
                        continue
                    lower_threshold_time = None
                    if await job._scale_down():
                        break
                    continue
                lower_threshold_time = None
                if threshold > job._cap.upper_threshold:
                    upper_threshold_time = upper_threshold_time or ts
                    if (ts - upper_threshold_time) < job._cap.dampening:
                        continue
                    upper_threshold_time = None
                    if await job._scale_up():
                        break
                    continue
                upper_threshold_time = None


def _strategy_factory(strategy):
    if strategy == Strategy.UTILIZATION:
        return _UtilizationStrategy()
    raise NotImplementedError()


class Strategy(enum.Enum):
    UTILIZATION = "Utilization based strategey"
