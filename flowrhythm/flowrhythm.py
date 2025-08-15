#! /usr/bin/env python3

import asyncio
import enum
import logging
import functools
from enum import Enum, auto
from contextlib import contextmanager

"""
 factory:
  if factory

 async def receiver(workitem) -> next item

"""


class Strategy(enum.Enum):
    UTILIZATION = "Utilization based strategey"


logger = logging.getLogger(__name__)


class LastWorkItem:
    pass


class WorkItemException(Exception):
    pass


class StopProcessing(WorkItemException):
    def __init__(msg):
        super().__init__(msg)


class RouteToErrorQueue(WorkItemException):
    def __init__(msg):
        super().__init__(msg)


class Statistics:
    def __init__(self):
        self.processed = 0
        self.successful = 0
        self.errors = 0


class Capacity:
    def __init__(self):
        self.initial_workers = 3
        self.min_workers = 1
        self.max_workers = 5
        self.queue_length = 10


class UtilizationCapacity(Capacity):
    def __init__(self):
        super().__init__()
        self.lower_threshold = 0.5
        self.upper_threshold = 0.7
        self.cooldown = 15  # amount of time we don't perform scaling actions
        self.sampling = 5  # sampling rate
        self.dampening = 10  # amount of time that threshold can be breached
        self.upscaling_rate = 2
        self.downscaling_rate = 1

    def __str__(self):
        lt = self.lower_threshold
        ut = self.upper_threshold
        return (
            f"{{ thresholds=({lt:.2f}/{ut:.2f})"
            f" cooldown={self.cooldown} sampling={self.sampling}"
            f" dampening={self.dampening} }}"
        )


class Flow:
    def __init__(self, strategy=Strategy.UTILIZATION):
        self._strategy = _strategy_factory(strategy)
        self._jobid = 1
        self._jobs = []
        self._error_job = None
        self._started = asyncio.Event()
        self._end_seen = asyncio.Event()
        self._stats = Statistics()
        self._flow_cond = asyncio.Condition()
        self._log = _LogAdapter(logger, {"classname": "Flow"})

    def add(self, processor_context, job_context=None, capacity=None, name=None):
        job = self._create_job(processor_context, job_context, capacity, name)
        if self._jobs:
            prev = self._jobs[-1]
            prev._output = asyncio.Queue(prev._cap.queue_length)
            job._input = prev._output
            prev._next = job
            job._parent = prev
            job._error_job = self._error_job
        self._jobs.append(job)
        self._log.debug('Job "%s" added, cap: %s', job._name, str(job._cap))

    def error(self, processor_context, job_context=None, capacity=None, name="Error"):
        ejob = self._create_job(processor_context, job_context, capacity, name)
        ejob._type = _Job.ERROR
        self._error_job = ejob
        ejob._input = asyncio.Queue(ejob._cap.queue_length)
        for job in self._jobs:
            job._error_job = ejob
        self._log.debug('Error job "%s" added, cap: %s', job._name, str(ejob._cap))

    async def start(self):
        if self._started.is_set():
            return
        self._log.debug(
            "starting: strategy=%s jobs=%d", self._strategy.name(), len(self._jobs)
        )

        try:
            for job in self._jobs:
                await job.start()
            if self._error_job:
                await self._error_job.start()
            self._started.set()
        except Exception:
            self._log.exception("unhandled exception in start()")
            await self._stop()
            raise

    async def stop(self):
        if not self._started.is_set():
            return
        await self._stop()
        self._started.clear()
        self._log.debug(
            "stopped: strategy=%s jobs=%d", self._strategy.name(), len(self._jobs)
        )

    async def run(self):
        try:
            self._log.debug("run() starting (%d) jobs ...", len(self._jobs))
            await self.start()
            async with self._flow_cond:
                while True:
                    all_done = True
                    for job in self._jobs:
                        if job._workers:
                            all_done = False
                            break
                    if all_done:
                        break
                    else:
                        await self._flow_cond.wait()
            self._log.debug("run() no active workers are running, exiting")
        finally:
            await self.stop()

    async def _stop(self):
        for job in self._jobs:
            await job.stop()
        if self._error_job:
            await self._error_job.stop()

    def _create_job(self, processor_context, job_context, cap, name):
        if not callable(processor_context) and not (
            hasattr(processor_context, "__aenter__")
            and hasattr(processor_context, "__aexit__")
        ):
            raise ValueError(
                "processor context must be async context manager or callable"
            )

        decorator = (
            processor_context
            if isinstance(processor_context, _UtilizationDecorator)
            else None
        )

        if decorator is not None:
            if cap is None:
                cap = decorator.capacity
            if name is None:
                name = decorator.name

        if name is None:
            name = f"Job{self._jobid:02d}"
            self._jobid += 1
        job = _Job()
        job._processor_context = processor_context
        job._job_context = job_context or _StubContextManager()
        job._cap = cap or self._strategy.default_capacity()
        job._name = name
        job._flow = self
        return job


class _JobState(Enum):
    NORMAL = auto()
    EOW_RECEIVED = auto()
    EOW_GENERATED = auto()


class _Job:
    NORMAL = 1
    ERROR = 2

    def __init__(self):
        self._processor_context = None
        self._job_context = None
        self._flow = None
        self._cap = None
        self._name = None
        self._type = _Job.NORMAL
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
        self._state = _JobState.NORMAL
        self._state_cond = asyncio.Condition()
        self._control_task = None
        self._target_workers = 0
        self._last_work_item = None

    async def start(self):
        self._log = _LogAdapter(logger, {"classname": f"_Job({self._name})"})
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
            for i in range(self._cap.max_workers):
                self._free_workers_id.add(i)
            async with self._job_context:
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
                    self._log.debug("_control_worker() stopped")
        except Exception:
            self._log.exception("_control_worker exception!")
            raise
        finally:
            self._control_task = None
            self._log.debug("leaving _control_worker()")

    def _add_worker(self):
        i = self._free_workers_id.pop()
        worker = _Worker(f"{self._name}{i:02d}", i, self)
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


class _StopWorker(Exception):
    def __init__(msg):
        super().__init__(msg)


class _Worker:
    def __init__(self, name, i, job):
        self._name = name
        self._id = i
        self._job = job
        self._task = None
        self._log = _LogAdapter(logger, {"classname": f"_Worker({self._name})"})

    def cancel(self):
        if self._task:
            self._task.cancel()

    async def main(self):
        try:
            self._log.debug("entering main()")

            try:
                if _is_async_cman(self._job._processor_context):
                    factory = self._job._processor_context
                else:
                    factory = _StubContextManager(self._job._processor_context)
                async with factory as processor:
                    while True:
                        async with self._job._state_cond:
                            if self._job._state != _JobState.NORMAL:
                                if len(self._job._workers) != 1:
                                    raise _StopWorker()
                                if self._job._state == _JobState.EOW_RECEIVED:
                                    try:
                                        self._job._state_cond.release()
                                        w = await processor(self._job._last_work_item)
                                        if isinstance(w, LastWorkItem):
                                            self._job._last_work_item = w
                                    except Exception:
                                        pass
                                    finally:
                                        await self._job._state_cond.acquire()
                                    self._job._state = _JobState.EOW_GENERATED

                                try:
                                    self._job._state_cond.release()
                                    await self._output(self._job._last_work_item)
                                finally:
                                    await self._job._state_cond.acquire()
                                raise _StopWorker()

                        work = await self.get_work()

                        rx_last = isinstance(work, LastWorkItem)
                        if rx_last:
                            await self._main_rx_last(work)

                        try:
                            next_work = await processor(work)
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
            self._log.debug("leving main()")
            self._job._remove_worker(self)

    async def _main_rx_last(self, work):
        async with self._job._state_cond:
            if self._job._state == _JobState.NORMAL:
                self._job._state = _JobState.EOW_RECEIVED
                self._job._state_cond.notify_all()
                self._log.debug("main() received last work item")
                if self._job._last_work_item is None:
                    self._job._last_work_item = work
                self._job._state_cond.release()
                await self._stop_idle_workers()
                await self._job._state_cond.acquire()
        if len(self._job._workers) != 1:
            raise _StopWorker()

    async def _main_gen_last(self, work):
        async with self._job._state_cond:
            self._job._state = _JobState.EOW_GENERATED
            self._job._state_cond.notify_all()
            self._log.debug("main() generated last work item")
            self._job._last_work_item = work
            self._job._state_cond.release()
            await self._stop_idle_workers()
            await self._job._state_cond.acquire()
        if len(self._job._workers) != 1:
            raise _StopWorker()

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

    def _main_completed(self, task):
        self._log.debug("_main_completed()")
        self._job._remove_worker(self)  # in case if task is destored before it started

    async def _output(self, msg):
        if self._job._output is None:
            return
        with set_manager(self._job._blocked_workers, self):
            await self._job._output.put(msg)

    async def get_work(self):
        async with self._job._state_cond:
            if self._job._state != _JobState.NORMAL:
                if len(self._job._workers) != 1:
                    raise _StopWorker()

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


class _UtilizationDecorator:
    def __init__(self, func, cap=UtilizationCapacity()):
        self.capacity = cap
        self.func = func
        self.name = None
        self.obj = None
        # functools.update_wrapper(self, func)

    def __call__(self, *args, **kwargs):
        if self.obj:
            margs = (self.obj,) + args
        else:
            margs = args

        return self.func(*margs, **kwargs)

    def __get__(self, obj, objtype):
        self.obj = obj
        return self


def job_name(name):
    def factory(func):
        decorator = (
            func
            if isinstance(func, _UtilizationDecorator)
            else _UtilizationDecorator(func)
        )
        decorator.name = name
        return decorator

    return factory


def job_capacity(cap):
    def factory(func):
        decorator = (
            func
            if isinstance(func, _UtilizationDecorator)
            else _UtilizationDecorator(func, cap)
        )
        return decorator

    return factory


def workers(min_workers, max_workers=5, initial_workers=1):
    def factory(func):
        decorator = (
            func
            if isinstance(func, _UtilizationDecorator)
            else _UtilizationDecorator(func)
        )
        decorator.capacity.min_workers = min_workers
        decorator.capacity.max_workers = max_workers
        decorator.capacity.initial_workers = initial_workers
        return decorator

    return factory


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


class _LogAdapter(logging.LoggerAdapter):
    def process(self, msg, kwargs):
        return "%s: %s" % (self.extra["classname"], msg), kwargs


def _is_async_cman(obj):
    return hasattr(obj, "__aenter__") and hasattr(obj, "__aexit__")


@contextmanager
def set_manager(s, obj):
    try:
        s.add(obj)  # Add object to set on entering the context
        yield s  # Yield the set for use in the context block
    finally:
        s.discard(obj)


class _StubContextManager:
    def __init__(self, body=None):
        self._body = body

    async def __aenter__(self):
        return self._body or self

    async def __aexit__(self, exc_type, exc_value, traceback):
        pass
