# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark workload service.

Its implementation follows the WorkloadBase interface. The final workload class
must implement most of the WorkloadBase methods.
"""

import os
import shutil
from typing import Optional

from charms.operator_libs_linux.v0.systemd import (
    daemon_reload,
    service_failed,
    service_restart,
    service_running,
    service_stop,
)
from overrides import override

from benchmark.core.models import BenchmarkServiceState
from benchmark.core.workload_base import WorkloadBase, WorkloadTemplatePaths
from benchmark.literals import (
    DPBenchmarkExecutionModel,
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
    def service(self) -> str|None:
        """The optional path to the service file managing the script."""
        return f"/etc/systemd/system/{self.svc_name}.service"

    @property
    @override
    def workload_parameters(self) -> str:
        """The path to the workload parameters folder."""
        if not os.path.exists("/root/.benchmark/charmed_parameters"):
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


class DPBenchmarkSystemdService(WorkloadBase):
    """Represents the benchmark service backed by systemd."""

    def __init__(self, db: DPBenchmarkExecutionModel):
        super().__init__(db)
        self.paths = DPBenchmarkSystemdTemplatePaths()

    @override
    def restart(self) -> bool:
        """Restarts the benchmark service."""
        return service_restart(self.paths.service)

    @override
    def stop(self) -> bool:
        """Stop the benchmark service."""
        if self.is_running():
            return service_stop(self.paths.service)
        return self.is_stopped()

    @override
    def active(self) -> bool:
        """Checks that the workload is active."""
        return self.check_service() == BenchmarkServiceState.RUNNING

    @override
    def unset(self) -> bool:
        """Unset the benchmark service."""
        try:
            if not (result := self.stop() and daemon_reload()):
                return False
            os.remove(self.svc_path)
            os.remove(self.workload_parameter_path)
        except Exception:
            return False
        return result

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
    def prepare(
        self,
        workload_name: str,
        labels: Optional[str] = "",
        extra_config: Optional[str] = "",
    ) -> bool:
        """Prepare the benchmark service."""
        try:
            self.render_workload_parameters(
                db=self.db,
                workload_name=workload_name,
            )
            if not self.render_service_file(
                db=self.db,
                labels=labels,
                extra_config=extra_config,
            ):
                return False
        except Exception:
            return False
        return True

    @override
    def check_service(self) -> BenchmarkServiceState:
        """Check the systemd status.

        This proxy method captures the external exception and re-raises as adequate for the benchmark.
        """
        if not os.exists(self.svc_path):
            return BenchmarkServiceState.NOT_PRESENT
        if service_failed(self.paths.service):
            return BenchmarkServiceState.FAILED
        if service_running(self.paths.service):
            return BenchmarkServiceState.RUNNING
        return BenchmarkServiceState.AVAILABLE

    def render_service_executable(self) -> bool:
        """Render the benchmark service executable."""
        shutil.copyfile("templates/" + self.paths.service + ".py", self.executable)
        os.chmod(self.executable, 0o755)

    def render_service_file(
        self,
        db: DPBenchmarkExecutionModel,
        labels: Optional[str] = "",
        extra_config: str | None = None,
    ) -> bool:
        """Render the systemd service file."""
        config = {
            "target_hosts": ",".join(db.db_info.hosts)
            if isinstance(db.db_info.hosts, list)
            else db.db_info.hosts,
            "workload": db.db_info.workload_name,
            "threads": db.threads,
            "clients": db.clients,
            "db_user": db.db_info.username,
            "db_password": db.db_info.password,
            "duration": db.duration,
            "workload_params": self.workload_parameter_path,
            "extra_labels": labels,
        }
        if extra_config:
            config["extra_config"] = extra_config

        self._render(
            self.paths.service + ".service.j2",
            config,
           dst_filepath = self.svc_path,
        ),
        return daemon_reload()

    def render_workload_parameters(
        self,
        db: DPBenchmarkExecutionModel,
        workload_name: str,
    ):
        """Renders the workload parameters file."""
        self._render(
            "src/workload_parameter_templates/" + workload_name + ".json.j2",
            self.workload_parameter_path,
            db.db_info.workload_params,
        )
