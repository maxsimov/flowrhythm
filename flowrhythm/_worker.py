import asyncio
import logging
from contextlib import contextmanager
from typing import Optional

from ._exceptions import LastWorkItem, RouteToErrorQueue, StopProcessing
from ._job import JobState
from ._logging import LogAdapter

logger = logging.getLogger(__name__)


class _StopWorker(Exception):
    def __init__(self, msg):
        super().__init__(msg)


@contextmanager
def set_manager(s, obj):
    try:
        s.add(obj)  # Add object to set on entering the context
        yield s  # Yield the set for use in the context block
    finally:
        s.discard(obj)


class Worker:
    def __init__(self, name, i, job, processor):
        self._name = name
        self._id = i
        self._job = job
        self._task: Optional[asyncio.Task] = None
        self._log = LogAdapter(logger, {"classname": f"_Worker({self._name})"})
        self._processor = processor

    def cancel(self):
        if self._task:
            self._task.cancel()

    async def main(self):
        try:
            self._log.debug("entering main()")
            try:
                while True:
                    async with self._job._state_cond:
                        if self._job._state != JobState.NORMAL:
                            if len(self._job._workers) != 1:
                                raise _StopWorker("Job is closed -> stopping worker")
                            if self._job._state == JobState.EOW_RECEIVED:
                                try:
                                    self._job._state_cond.release()
                                    w = await self._processor(self._job._last_work_item)
                                    if isinstance(w, LastWorkItem):
                                        self._job._last_work_item = w
                                except Exception:
                                    pass
                                finally:
                                    await self._job._state_cond.acquire()
                                self._job._state = JobState.EOW_GENERATED

                            try:
                                self._job._state_cond.release()
                                await self._output(self._job._last_work_item)
                            finally:
                                await self._job._state_cond.acquire()
                            raise _StopWorker(
                                "Error while processing last element -> stopping worker"
                            )

                    work = await self.get_work()

                    rx_last = isinstance(work, LastWorkItem)
                    if rx_last:
                        await self._main_rx_last(work)

                    try:
                        next_work = await self._processor(work)
                    except StopProcessing:
                        await self._main_processor_stop_processing(work)
                        continue
                    except RouteToErrorQueue:
                        await self._main_processor_route_to_error(work)
                        continue
                    except Exception:
                        await self._main_processor_error(work)
                        continue

                    if rx_last or isinstance(next_work, LastWorkItem):
                        await self._main_gen_last(next_work)

                    await self._output(next_work)
            except _StopWorker:
                self._log.debug("main() stopping...")
            except Exception:
                self._log.exception(
                    "main() unhandled exception while trying to create processor"
                )
                return

        finally:
            self._log.debug("leaving main()")
            self._job._remove_worker(self)

    async def _main_rx_last(self, work):
        async with self._job._state_cond:
            if self._job._state == JobState.NORMAL:
                self._job._state = JobState.EOW_RECEIVED
                self._job._state_cond.notify_all()
                self._log.debug("main() received last work item")
                if self._job._last_work_item is None:
                    self._job._last_work_item = work
                self._job._state_cond.release()
                await self._stop_idle_workers()
                await self._job._state_cond.acquire()
        if len(self._job._workers) != 1:
            raise _StopWorker("Stopping workers")

    async def _main_gen_last(self, work):
        async with self._job._state_cond:
            self._job._state = JobState.EOW_GENERATED
            self._job._state_cond.notify_all()
            self._log.debug("main() generated last work item")
            self._job._last_work_item = work
            self._job._state_cond.release()
            await self._stop_idle_workers()
            await self._job._state_cond.acquire()
        if len(self._job._workers) != 1:
            raise _StopWorker("Last element -> stopping workers")

    async def _main_processor_stop_processing(self, work):
        self._log.debug("main() stop processing work {%s}", str(work))

    async def _main_processor_route_to_error(self, work):
        if self._job._error_job:
            self._log.debug("main() re-routing to error queue")
            await self._job._error_job._input.put(work)
        else:
            self._log.error("main() discarding work item!!!")

    async def _main_processor_error(self, work):
        self._log.exception("main() unhandled exception in processor!")
        await self._main_processor_route_to_error(work)

    def _main_completed(self, _):
        self._log.debug("_main_completed()")
        self._job._remove_worker(self)  # in case if task is destored before it started

    async def _output(self, msg):
        if self._job._output is None:
            return
        with set_manager(self._job._blocked_workers, self):
            await self._job._output.put(msg)

    async def get_work(self):
        async with self._job._state_cond:
            if self._job._state != JobState.NORMAL:
                if len(self._job._workers) != 1:
                    raise _StopWorker("Stop workers")

        if self._job._input is None:
            await asyncio.sleep(0)
            return None
        with set_manager(self._job._idle_workers, self):
            work = await self._job._input.get()
        return work

    async def _stop_idle_workers(self):
        if len(self._job._idle_workers) == 0:
            return
        self._log.debug("main() stopping %d idle workers", len(self._job._idle_workers))
        for idle_worker in self._job._idle_workers:
            idle_worker.cancel()
        while self._job._idle_workers:
            await asyncio.sleep(0)
