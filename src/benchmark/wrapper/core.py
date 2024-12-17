# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The core models for the wrapper script."""

from enum import Enum

from prometheus_client import Gauge
from pydantic import BaseModel


class BenchmarkCommand(str, Enum):
    """Enum to hold the benchmark phase."""

    PREPARE = "prepare"
    RUN = "run"
    STOP = "stop"
    COLLECT = "collect"
    UPLOAD = "upload"
    CLEANUP = "cleanup"


class ProcessStatus(str, Enum):
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
    cwd: str | None = None


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
    report_interval: int
    extra_labels: str
    peers: str


class BenchmarkMetrics:
    """Class to hold the benchmark metrics."""

    def __init__(
        self,
        options: MetricOptionsModel,
    ):
        self.options = options
        self.metrics = {}

    def add(self, sample: BaseModel):
        """Add the benchmark to the prometheus metric."""
        for key, value in sample.dict().items():
            if self.options.label not in self.metrics:
                self.metrics[f"{self.options.label}_{key}"] = Gauge(
                    self.options.label,
                    f"{self.options.description} {key}",
                    ["model", "unit"],
                )
            self.metrics[f"{self.options.label}_{key}"].labels(*self.options.extra_labels).set(value)
