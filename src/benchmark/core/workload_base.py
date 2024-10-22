# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""Supporting objects for Benchmark charm state."""

from abc import ABC, abstractmethod
from typing import Any, Optional

from jinja2 import Environment, FileSystemLoader, exceptions

from benchmark.literals import BenchmarkServiceState, DPBenchmarkExecutionModel


class WorkloadTemplatePaths(ABC):
    """Interface for workload template paths."""

    @property
    @abstractmethod
    def script(self) -> str:
        """The main script managed by the service."""
        ...

    @property
    @abstractmethod
    def service(self) -> str|None:
        """The optional path to the service file managing the script."""
        ...

    @property
    @abstractmethod
    def workload_parameters(self) -> str:
        """The path to the workload parameters folder."""
        ...

    @abstractmethod
    def exists(self, path: str) -> bool:
        """Check if the workload template paths exist."""
        ...


class WorkloadBase(ABC):
    """Base interface for common workload operations."""

    paths: WorkloadTemplatePaths

    def __init__(self, db: DPBenchmarkExecutionModel):
        self.db = db

    def start(self) -> bool:
        """Starts the workload service."""
        self.restart(self.paths.service)

    @abstractmethod
    def stop(self) -> bool:
        """Stops the workload service."""
        ...

    @abstractmethod
    def unset(self) -> bool:
        """Unset the benchmark service."""
        ...

    @abstractmethod
    def restart(self) -> None:
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
    def check_service(self) -> BenchmarkServiceState:
        """Checks if the workload service is running."""
        ...

    def _render(
        self,
        template_file: str,
        values: dict[str, Any],
        dst_filepath: str|None = None,
    ) -> str:
        """Renders files and return its contents."""
        template_env = Environment(loader=FileSystemLoader(self.paths.template))
        try:
            template = template_env.get_template(template_file)
            content = template.render(values)
        except exceptions.TemplateNotFound as e:
            raise e
        if not dst_filepath:
            return content
        self.write(content, dst_filepath)

    def is_prepared(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        return (
            self.paths.exists(self.svc_path)
            and self.paths.exists(self.workload_parameter_path)
            and self.paths.exists(self.executable)
        )

    @abstractmethod
    def prepare(
        self,
        workload_name: str,
        labels: Optional[str] = "",
        extra_config: Optional[str] = "",
    ) -> bool:
        """Prepare the benchmark service."""
        ...

    def is_running(self) -> bool:
        """Checks if the benchmark service is running."""
        return (
            self.is_prepared()
            and self.check_service() == BenchmarkServiceState.RUNNING
        )

    def is_stopped(self) -> bool:
        """Checks if the benchmark service has stopped."""
        return (
            self.is_prepared()
            and not self.is_running()
            and not self.is_failed()
        )

    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        return (
            self.is_prepared()
            and self.check_service() == BenchmarkServiceState.FAILED
        )
