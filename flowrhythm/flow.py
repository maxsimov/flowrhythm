#! /usr/bin/env python3
import asyncio
import logging
from contextlib import asynccontextmanager
from typing import Any, AsyncContextManager, Awaitable, Callable

from .capacity import Statistics
from .decorators import _UtilizationDecorator
from .job import _Job
from .logging import _LogAdapter
from .strategy import Strategy, _strategy_factory

"""
 factory:
  if factory

 async def receiver(workitem) -> next item

"""

logger = logging.getLogger(__name__)


Processor = Callable[[Any], Awaitable[Any]]
ProcessorLock = AsyncContextManager[Processor]
ProcessorFactory = AsyncContextManager[Processor]


@asynccontextmanager
async def stub_factory(processor: Processor):
    yield processor


@asynccontextmanager
async def stub_factory_lock(processor: Processor, resource_manager: ProcessorLock):
    async with resource_manager:
        yield processor


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

    def add_job(self, processor: Processor, capacity=None, name=None):
        """Adds a job processor to the flow"""
        self.add_job_with_factory(stub_factory(processor), capacity, name)

    def add_job_with_lock(
        self,
        processor: Processor,
        resource_manager: ProcessorLock,
        capacity=None,
        name=None,
    ):
        self.add_job_with_factory(
            stub_factory_lock(processor, resource_manager), capacity, name
        )

    def add_job_with_factory(
        self,
        processor_factory: ProcessorFactory,
        capacity=None,
        name=None,
    ):
        job = self._create_job(processor_factory, capacity, name)
        if self._jobs:
            prev = self._jobs[-1]
            prev._output = asyncio.Queue(prev._cap.queue_length)
            job._input = prev._output
            prev._next = job
            job._parent = prev
            job._error_job = self._error_job
        self._jobs.append(job)
        self._log.debug('Job "%s" added, cap: %s', job._name, str(job._cap))

    def error(self, processor: Processor, capacity=None, name="Error"):
        self.error_with_factory(stub_factory(processor), capacity, name)

    def error_with_lock(
        self,
        processor: Processor,
        resource_manager: ProcessorLock,
        capacity=None,
        name="Error",
    ):
        self.error_with_factory(
            stub_factory_lock(processor, resource_manager), capacity, name
        )

    def error_with_factory(self, processor_factory, capacity=None, name="Error"):
        ejob = self._create_job(processor_factory, capacity, name)
        ejob._type = _Job.ERROR
        self._error_job = ejob
        assert ejob._cap is not None
        ejob._input = asyncio.Queue(ejob._cap.queue_length)
        for job in self._jobs:
            job._error_job = ejob
        self._log.debug('Error job "%s" added, cap: %s', ejob._name, str(ejob._cap))

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

    def _create_job(self, processor_factory, cap, name):
        decorator = (
            processor_factory
            if isinstance(processor_factory, _UtilizationDecorator)
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
        assert name is not None
        cap = cap or self._strategy.default_capacity()
        assert cap is not None
        job = _Job(self, processor_factory, cap)
        job._name = name
        job._flow = self
        return job
