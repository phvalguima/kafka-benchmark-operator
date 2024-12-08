# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module manages the entire benchmark deployment.

Normally, this deployment will be composed of several processes doing either the
same task or running different tasks at once.
"""

import asyncio
import logging
import os
import subprocess
import time
from abc import ABC, abstractmethod

from core import (
    BenchmarkCommand,
    BenchmarkMetrics,
    ProcessModel,
    ProcessStatus,
    WorkloadCLIArgsModel,
)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


logger = logging.getLogger(__name__)


class BenchmarkProcess(ABC):
    """This class models one of the processes being executed in the benchmark.

    Overload this class and implement the `process_line` method to return either str
    or None. It will listen to the process output and process it accordingly. If the
    method `process_line` returns None, the line will only be logged. The intention
    is to represent both cases where a given process output is relevant and contains
    metrics that need to be uploaded to Prometheus OR when the output is just a log
    line to keep track of the information.
    """
    def __init__(
        self,
        model: ProcessModel,
        args: WorkloadCLIArgsModel,
        metrics: BenchmarkMetrics,
    ):
        self.model = model
        self.metrics = metrics
        self.args = args
        self._proc = None

    def start(self):
        """Start the process."""
        self._proc = subprocess.Popen(
            self.model.cmd,
            user=self.model.user,
            group=self.model.group,
            stdin=None,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            universal_newlines=True,
            shell=True,
        )
        # Now, let's make stdout a non-blocking file
        os.set_blocking(self._proc.stdout.fileno(), False)

        self.model.pid = self._proc.pid
        self.model.status = ProcessStatus.RUNNING

    def status(self) -> ProcessStatus:
        """Return the status of the process."""
        stat = ProcessStatus.STOPPED
        if self._proc.poll() is None:
            stat = ProcessStatus.RUNNING
        elif self._proc.returncode != 0:
            stat = ProcessStatus.ERROR
        self.model.status = stat
        return stat

    async def process(
        self,
        auto_stop: bool = True,
    ):
        """Run one step of the main benchmark service loop."""
        initial_time = int(time.time())
        finish_time = initial_time + self.args.duration

        run_count = 0
        while (
            (run_count < self.args.run_count and self.args.run_count != 0)
            or (int(time.time()) >= finish_time and self.args.duration != 0)
            or (self.status() == ProcessStatus.RUNNING and self.args.duration == 0)
        ):
            to_wait = True
            for line in iter(self._proc.stdout.readline, ""):
                if output := self.process_line(line):
                    self.metrics.add(output)

                # Log the output.
                # This way, an user can see what the process is doing and
                # some of the metrics will be readily available without COS.
                logger.info(line)

                if self.status() != ProcessStatus.RUNNING:
                    # Process has finished
                    break

                await asyncio.sleep(self.args.report_interval)
                to_wait = False

            if to_wait:
                # In case the stdout is empty, we ensure we sleep anyways
                await asyncio.sleep(self.args.report_interval)

            # If we are considering the
            if self.status() != ProcessStatus.RUNNING and self.args.run_count:
                # Increase the run count
                run_count += 1

        # Now we are finished, check if we need to stop the process
        if auto_stop and self.status() == ProcessStatus.RUNNING:
            self.stop()

    def stop(self):
        """Stop the process."""
        try:
            self._proc.kill()
        except Exception as e:
            logger.warning(f"Error stopping worker: {e}")
        self.model.status = ProcessStatus.STOPPED

    @abstractmethod
    def process_line(self, line: str) -> str | None:
        """Process the output of the process."""
        ...


class BenchmarkManager(BenchmarkProcess):
    """This class is in charge of managing all the processes in the benchmark run."""

    def __init__(
        self,
        model: ProcessModel | None,
        args: WorkloadCLIArgsModel,
        metrics: BenchmarkMetrics,
        unstarted_workers: list[BenchmarkProcess],
    ):
        super().__init__(model, args, metrics)
        self.workers = unstarted_workers

    async def _exec(self, auto_stop: bool = True):
        tasks = []
        for worker in self.workers:
            tasks.append(asyncio.create_task(worker.process(auto_stop=auto_stop)))
        tasks.append(asyncio.create_task(self.process(auto_stop=auto_stop)))
        await asyncio.gather(*tasks)

    def run(self):
        """Run all the workers in the async loop."""
        asyncio.run(self._exec())

    def all_running(self) -> bool:
        """Check if all the workers are running."""
        return (
            all(w.status() == ProcessStatus.RUNNING for w in self.workers)
            and self.status() == ProcessStatus.RUNNING
        )

    def start(self):
        """Start the benchmark tool."""
        if self.model:
            super().start()
        for worker in self.workers:
            worker.start()

    def stop(self):
        """Stop the benchmark tool."""
        for worker in self.workers:
            worker.stop()
        if self.model:
            super().stop()


class WorkloadToProcessMapping(ABC):
    """This class maps the workload model to the process."""

    def __init__(self, args: WorkloadCLIArgsModel, metrics: BenchmarkMetrics):
        self.args = args
        self.manager = None
        self.metrics = metrics

    def status(self) -> ProcessStatus:
        """Return the status of the benchmark."""
        return self.manager.status()

    def map(self, cmd: BenchmarkCommand) -> tuple[BenchmarkManager, list[BenchmarkProcess]]:
        """Processes high-level arguments into the benchmark manager and workers.

        Returns all the processes that will be running the benchmark.

        The processes will not be started.
        """
        if cmd == BenchmarkCommand.PREPARE:
            self.manager, procs = self._map_prepare()
            return self.manager, procs
        elif cmd == BenchmarkCommand.RUN:
            self.manager, procs = self._map_run()
            return self.manager, procs
        elif cmd == BenchmarkCommand.CLEANUP:
            self.manager, procs = self._map_clean()
            return self.manager, procs
        raise ValueError(f"Invalid command: {cmd}")

    @abstractmethod
    def _map_prepare(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the prepare phase."""
        ...

    @abstractmethod
    def _map_run(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the run phase."""
        ...

    @abstractmethod
    def _map_clean(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the clean phase."""
        ...
