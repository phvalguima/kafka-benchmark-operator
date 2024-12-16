# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The core models for the wrapper script."""

from enum import Enum

from prometheus_client import Gauge
from pydantic import BaseModel


class BenchmarkCommand(Enum):
    """Enum to hold the benchmark phase."""

    PREPARE = "prepare"
    RUN = "run"
    STOP = "stop"
    COLLECT = "collect"
    UPLOAD = "upload"
    CLEANUP = "cleanup"


class ProcessStatus(Enum):
    """Enum to hold the process status."""

    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"
    TO_START = "to_start"


class ProcessModel(BaseModel):
    """Model to hold the process information."""

    cmd: str
    pid: int = -1
    status: str = ProcessStatus.TO_START
    user: str | None = None
    group: str | None = None


class MetricOptionsModel(BaseModel):
    """Model to hold the metrics."""

    label: str | None = None
    extra_labels: list[str] = []
    description: str | None = None


class WorkloadCLIArgsModel(BaseModel):
    """Model to hold the workload options."""

    test_name: str
    command: BenchmarkCommand
    workload: str
    threads: int
    parallel_processes: int
    duration: int
    run_count: int
    target_hosts: str
    report_interval: int
    extra_labels: str


class BenchmarkMetrics:
    """Class to hold the benchmark metrics."""

    def __init__(
        self,
        options: MetricOptionsModel,
    ):
        self.options = options
        self.metrics = {}

    def add(self, value):
        """Add the benchmark to the prometheus metric."""
        if self.options.label not in self.metrics:
            self.metrics[self.options.label] = Gauge(
                self.options.label,
                self.options.description,
                ["model", "unit"],
            )
        self.metrics[self.options.label].labels(*self.options.extra_labels).set(value)
