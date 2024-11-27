# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Supporting objects for Benchmark charm state."""

from abc import ABC, abstractmethod

from benchmark.literals import DPBenchmarkServiceState


class WorkloadTemplatePaths(ABC):
    """Interface for workload template paths."""

    @property
    @abstractmethod
    def benchmark_wrapper(self) -> str:
        """The main benchmark_wrapper managed by the service."""
        ...

    @property
    @abstractmethod
    def service(self) -> str | None:
        """The optional path to the service file managing the python wrapper."""
        ...

    @property
    @abstractmethod
    def workload_params(self) -> str:
        """The path to the workload parameters file."""
        ...

    @property
    def workload_params_template(self) -> str:
        """The path to the workload parameters template."""
        ...

    @property
    @abstractmethod
    def results(self) -> str:
        """The path to the results folder."""
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if the workload template paths exist."""
        ...

    @property
    @abstractmethod
    def templates(self) -> str:
        """The path to the workload template folder."""
        ...


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    paths: WorkloadTemplatePaths

    @abstractmethod
    def install(self) -> None:
        """Installs the workload."""
        ...

    def start(self) -> bool:
        """Starts the workload service."""
        return self.restart()

    @abstractmethod
    def stop(self) -> bool:
        """Stops the workload service."""
        ...

    @abstractmethod
    def restart(self) -> bool:
        """Restarts the workload service."""
        ...

    @abstractmethod
    def read(self, path: str) -> list[str]:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            List of string lines from the specified path
        """
        ...

    @abstractmethod
    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        ...

    @abstractmethod
    def is_running_on_k8s(self) -> bool:
        """Returns True if running on k8s env."""
        ...

    @abstractmethod
    def exec(
        self,
        command: list[str] | str,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str:
        """Executes a command on the workload substrate."""
        ...

    @abstractmethod
    def active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def check_service(self) -> DPBenchmarkServiceState:
        """Checks if the workload service is running."""
        ...

    def is_prepared(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        return (
            self.paths.exists(self.paths.benchmark_wrapper)
            and self.paths.exists(self.paths.workload_params)
            and self.paths.exists(self.paths.service)
        )

    def is_running(self) -> bool:
        """Checks if the benchmark service is running."""
        return self.is_prepared() and self.check_service() == DPBenchmarkServiceState.RUNNING

    def is_stopped(self) -> bool:
        """Checks if the benchmark service has stopped."""
        return self.is_prepared() and not self.is_running() and not self.is_failed()

    @abstractmethod
    def is_uploading(self) -> bool:
        """Checks if the benchmark service is still running the upload process."""
        ...

    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        return self.is_prepared() and self.check_service() == DPBenchmarkServiceState.FAILED
