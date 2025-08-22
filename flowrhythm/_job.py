import asyncio
import logging
from enum import Enum, auto
from typing import Optional

from ._capacity import UtilizationCapacity
from flowrhythm.flow import Processor

from ._logging import LogAdapter
from ._worker import Worker

logger = logging.getLogger(__name__)


class JobState(Enum):
    NORMAL = auto()
    EOW_RECEIVED = auto()
    EOW_GENERATED = auto()


class Job:
    NORMAL = 1
    ERROR = 2
    _input: Optional[asyncio.Queue]
    _error_job: Optional["Job"]

    def __init__(self, flow, processorFactory, cap: UtilizationCapacity):
        self._flow = flow
        self._processorFactory = processorFactory
        self._processor: Optional[Processor] = None
        self._cap: UtilizationCapacity = cap
        self._name: Optional[str] = None
        self._type = Job.NORMAL
        self._input = None
        self._output = None
        self._parent = None
        self._next = None
        self._error_job = None
        self._workers = set()
        self._free_workers_id = set()
        self._idle_workers = set()
        self._blocked_workers = set()
        self._started = asyncio.Event()
        self._state = JobState.NORMAL
        self._state_cond = asyncio.Condition()
        self._control_task = None
        self._target_workers = 0
        self._last_work_item = None

    async def start(self):
        self._log = LogAdapter(logger, {"classname": f"_Job({self._name})"})
        self._control_task = asyncio.create_task(self._control_worker())
        await self._started.wait()

    async def stop(self):
        if self._control_task is None:
            return
        self._control_task.cancel()
        while self._control_task:
            await asyncio.sleep(0)

    def get_current_utilization(self):
        n = len(self._workers)
        if n == 0:
            return 1
        idle = len(self._blocked_workers) + len(self._idle_workers)
        a = n - idle
        if a < 0:
            a = 0
        return a / n

    async def _control_worker(self):
        try:
            self._log.debug("entering _control_worker()")
            assert self._cap is not None
            for i in range(self._cap.max_workers):
                self._free_workers_id.add(i)
            async with self._flow._processor_context as processor:
                self._processor = processor
                self._started.set()
                try:
                    initial_workers = min(
                        self._cap.max_workers, self._cap.initial_workers
                    )
                    for i in range(initial_workers):
                        self._add_worker()
                    self._target_workers = len(self._workers)
                    self._log.debug(
                        "_control_worker() started, initial=%d",
                        self._cap.initial_workers,
                    )
                    await self._flow._strategy(self)
                    while True:
                        await asyncio.sleep(10)
                finally:
                    self._log.debug(
                        "_control_worker() stopping, workers=%d", len(self._workers)
                    )
                    for worker in self._workers:
                        worker.cancel()
                    while self._workers:
                        await asyncio.sleep(0)
                    self._processor = None
                    self._log.debug("_control_worker() stopped")
        except Exception:
            self._log.exception("_control_worker exception!")
            raise
        finally:
            self._control_task = None
            self._log.debug("leaving _control_worker()")

    def _add_worker(self):
        assert self._processor is not None
        i = self._free_workers_id.pop()
        worker = Worker(f"{self._name}{i:02d}", i, self, self._processor)
        self._workers.add(worker)
        worker._task = asyncio.create_task(worker.main())
        worker._task.add_done_callback(worker._main_completed)

    def _remove_worker(self, worker):
        if worker not in self._workers:
            return
        self._free_workers_id.add(worker._id)
        self._workers.discard(worker)
        worker._task = None
        if not self._workers:

            async def wrapper():
                async with self._flow._flow_cond:
                    self._flow._flow_cond.notify_all()

            asyncio.create_task(wrapper())

    async def _scale_up(self):
        self._target_workers += self._cap.upscaling_rate
        if self._target_workers > self._cap.max_workers:
            self._target_workers = self._cap.max_workers

        if self._target_workers <= len(self._workers):
            return False

        original_workers = len(self._workers)
        while len(self._workers) < self._target_workers:
            self._add_worker()
        self._log.debug("scaled up from %d to %d", original_workers, len(self._workers))
        return True

    async def _scale_down(self):
        self._target_workers -= self._cap.downscaling_rate
        if self._target_workers < self._cap.min_workers:
            self._target_workers = self._cap.min_workers

        if self._target_workers >= len(self._workers):
            return False

        original_workers = len(self._workers)
        while self._idle_workers and self._target_workers < len(self._workers):
            worker = self._idle_workers.pop()
            worker.cancel()
            await asyncio.sleep(0)
        self._log.debug(
            "scaled down from %d to %d", original_workers, len(self._workers)
        )
        return True

    def timestamp(self):
        return asyncio.get_event_loop().time()
