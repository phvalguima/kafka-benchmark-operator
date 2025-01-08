# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark workload manager for systemd.

Its implementation follows the WorkloadBase interface. The final workload class
must implement most of the WorkloadBase methods.
"""

import os

from charms.operator_libs_linux.v1.systemd import (
    service_restart,
    service_stop,
)
from overrides import override

from benchmark.core.workload_base import WorkloadBase, WorkloadTemplatePaths


class DPBenchmarkPebbleTemplatePaths(WorkloadTemplatePaths):
    """Represents the benchmark service template paths."""

    def __init__(self):
        super().__init__()
        self.svc_name = "dpe_benchmark"

    @property
    @override
    def service(self) -> str | None:
        """The optional path to the service file managing the script."""
        return f"/etc/systemd/system/{self.svc_name}.service"

    @property
    @override
    def service_template(self) -> str:
        """The service template file."""
        return os.path.join(self.templates, "dpe_benchmark.service.j2")

    @override
    def exists(self, path: str) -> bool:
        """Check if the workload template paths exist."""
        return os.path.exists(path)


class DPBenchmarkPebbleWorkloadBase(WorkloadBase):
    """Represents the benchmark service backed by systemd."""

    def __init__(self, workload_params_template: str):
        super().__init__(workload_params_template)
        self.paths = DPBenchmarkPebbleTemplatePaths()
        os.chmod(self.paths.workload_params, 0o700)

    @override
    def install(self) -> bool:
        """Installs the workload."""
        return True

    @override
    def start(self) -> bool:
        """Starts the workload service."""
        return True

    @override
    def restart(self) -> bool:
        """Restarts the benchmark service."""
        return service_restart(self.paths.svc_name)

    @override
    def halt(self) -> bool:
        """Stop the benchmark service."""
        if self.is_running():
            return service_stop(self.paths.svc_name)
        return self.is_stopped()

    @override
    def reload(self) -> bool:
        """Reloads the workload service."""
        return True

    @override
    def read(self, path: str) -> list[str]:
        """Reads a file from the workload.

        Args:
            path: the full filepath to read from

        Returns:
            List of string lines from the specified path
        """
        with open(path, "r") as f:
            content = f.read()
        return content.splitlines()

    @override
    def write(self, content: str, path: str, mode: str = "w") -> None:
        """Writes content to a workload file.

        Args:
            content: string of content to write
            path: the full filepath to write to
            mode: the write mode. Usually "w" for write, or "a" for append. Default "w"
        """
        with open(path, mode) as f:
            f.write(content)
            os.chmod(path, 0o640)

    @override
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

    @override
    def is_active(self) -> bool:
        """Checks that the workload is active."""
        ...

    @override
    def _is_stopped(self) -> bool:
        """Checks that the workload is stopped."""
        ...

    @override
    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        ...
