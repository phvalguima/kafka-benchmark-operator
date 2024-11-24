# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark workload manager for systemd.

Its implementation follows the WorkloadBase interface. The final workload class
must implement most of the WorkloadBase methods.
"""

import os

from charms.operator_libs_linux.v1.systemd import (
    service_failed,
    service_restart,
    service_running,
    service_stop,
)
from overrides import override

from benchmark.core.workload_base import WorkloadBase, WorkloadTemplatePaths
from benchmark.literals import (
    BenchmarkServiceState,
)


class DPBenchmarkSystemdTemplatePaths(WorkloadTemplatePaths):
    """Represents the benchmark service template paths."""

    def __init__(self):
        super().__init__()
        self.svc_name = "dpe_benchmark"

    @property
    def script(self) -> str:
        """The main script managed by the service."""
        return "/usr/bin/" + self.svc_name + ".py"

    @property
    @override
    def service(self) -> str | None:
        """The optional path to the service file managing the script."""
        return f"/etc/systemd/system/{self.svc_name}.service"

    @property
    @override
    def workload_parameters(self) -> str:
        """The path to the workload parameters folder."""
        if not self.exists("/root/.benchmark/charmed_parameters"):
            os.makedirs("/root/.benchmark/charmed_parameters", exist_ok=True)
        return "/root/.benchmark/charmed_parameters/" + self.svc_name + ".json"

    @property
    @override
    def templates(self) -> str:
        """The path to the workload template folder."""
        return os.path.join(os.environ.get("CHARM_DIR", ""), "templates")

    @override
    def exists(self, path: str) -> bool:
        """Check if the workload template paths exist."""
        return os.path.exists(path)


class DPBenchmarkSystemdWorkloadBase(WorkloadBase):
    """Represents the benchmark service backed by systemd."""

    def __init__(self):
        super().__init__()
        self.paths = DPBenchmarkSystemdTemplatePaths()

    @override
    def restart(self) -> bool:
        """Restarts the benchmark service."""
        return service_restart(self.paths.svc_name)

    @override
    def stop(self) -> bool:
        """Stop the benchmark service."""
        if self.is_running():
            return service_stop(self.paths.svc_name)
        return self.is_stopped()

    @override
    def active(self) -> bool:
        """Checks that the workload is active."""
        return self.check_service() == BenchmarkServiceState.RUNNING

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
    def check_service(self) -> BenchmarkServiceState:
        """Check the systemd status.

        This proxy method captures the external exception and re-raises as adequate for the benchmark.
        """
        if not self.paths.exists(self.paths.service):
            return BenchmarkServiceState.NOT_PRESENT
        if service_failed(self.paths.svc_name):
            return BenchmarkServiceState.FAILED
        if service_running(self.paths.svc_name):
            return BenchmarkServiceState.RUNNING
        return BenchmarkServiceState.AVAILABLE

    @override
    def exec(
        self,
        command: list[str] | str,
        env: dict[str, str] | None = None,
        working_dir: str | None = None,
    ) -> str:
        """Executes a command on the workload substrate."""
        pass
