# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Supporting objects for Benchmark charm state."""

import os
from abc import ABC, abstractmethod


class WorkloadParamsTemplateBase(ABC):
    """Returns the workload parameters template.

    Ideally, this class should be static and its template content remains the same across
    different types of implementation (VM and k8s).
    """

    def template(self) -> str:
        """Returns the workload parameters template."""
        ...


class WorkloadTemplatePaths(ABC):
    """Interface for workload template paths."""

    @property
    def benchmark_wrapper(self) -> str:
        """The main benchmark_wrapper managed by the service."""
        os.path.join(os.environ.get("CHARM_DIR", ""), "src/benchmark/wrapper/main.py")

    @property
    @abstractmethod
    def svc_name(self) -> str:
        """The service name."""
        ...

    @property
    @abstractmethod
    def service(self) -> str | None:
        """The optional path to the service file managing the python wrapper."""
        ...

    @property
    @abstractmethod
    def service_template(self) -> str | None:
        """The path to the service template file."""
        ...

    @property
    @abstractmethod
    def workload_params(self) -> str:
        """The path to the workload parameters file."""
        ...

    @property
    @abstractmethod
    def results(self) -> str:
        """The path to the results folder."""
        ...

    def exists(self, path: str) -> bool:
        """Check if the workload path exist."""
        return os.path.exists(path)

    @property
    @abstractmethod
    def templates(self) -> str:
        """The path to the workload template folder."""
        ...


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    paths: WorkloadTemplatePaths
    workload_params_template: WorkloadParamsTemplateBase

    @abstractmethod
    def install(self) -> bool:
        """Installs the workload."""
        ...

    @abstractmethod
    def start(self) -> bool:
        """Starts the workload service."""
        ...

    @abstractmethod
    def restart(self) -> bool:
        """Retarts the workload service."""
        ...

    @abstractmethod
    def halt(self) -> bool:
        """Halts the workload service."""
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
    def exec(
        self,
        command: list[str] | str,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str | None:
        """Executes a command on the workload substrate.

        Returns None if the command failed to be executed.
        """
        ...

    @abstractmethod
    def is_active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @abstractmethod
    def _is_stopped(self) -> bool:
        """Checks that the workload is stopped."""
        ...

    @abstractmethod
    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        ...

    def is_halted(self) -> bool:
        """Checks if the benchmark service has halted."""
        return self._is_stopped() and not self.is_failed()
