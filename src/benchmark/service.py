# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the benchmark service."""

import os
import shutil
from typing import Any, Dict, Optional

from charms.operator_libs_linux.v1.systemd import (
    daemon_reload,
    service_failed,
    service_restart,
    service_running,
    service_stop,
)
from jinja2 import Environment, FileSystemLoader, exceptions

from .constants import (
    DPBenchmarkExecutionModel,
)


def _render(src_template_file: str, dst_filepath: str, values: Dict[str, Any]):
    templates_dir = os.path.join(os.environ.get("CHARM_DIR", ""), "templates")
    template_env = Environment(loader=FileSystemLoader(templates_dir))
    try:
        template = template_env.get_template(src_template_file)
        content = template.render(values)
    except exceptions.TemplateNotFound as e:
        raise e
    # save the file in the destination
    with open(dst_filepath, "w") as f:
        f.write(content)
        os.chmod(dst_filepath, 0o640)


class DPBenchmarkService:
    """Represents the benchmark service."""

    SVC_NAME = "dpe_benchmark"
    SVC_EXECUTABLE_PATH = "/usr/bin/"
    SVC_PATH = f"/etc/systemd/system/{SVC_NAME}.service"

    @property
    def svc_path(self) -> str:
        """Returns the path to the service file."""
        return self.SVC_PATH

    @property
    def executable(self) -> str:
        """Returns the path to the service executable."""
        return self.SVC_EXECUTABLE_PATH + self.SVC_NAME + ".py"

    @property
    def workload_parameter_path(self) -> str:
        """Returns the path to the workload parameters file."""
        if not os.path.exists("/root/.benchmark/charmed_parameters"):
            os.makedirs("/root/.benchmark/charmed_parameters", exist_ok=True)
        return "/root/.benchmark/charmed_parameters/" + self.SVC_NAME + ".json"

    def render_service_executable(self) -> bool:
        """Render the benchmark service executable."""
        shutil.copyfile("templates/" + self.SVC_NAME + ".py", self.executable)
        os.chmod(self.executable, 0o755)

    def render_service_file(
        self,
        db: DPBenchmarkExecutionModel,
        labels: Optional[str] = "",
        extra_config: Optional[str] = "",
    ) -> bool:
        """Render the systemd service file."""
        _render(
            self.SVC_NAME + ".service.j2",
            self.svc_path,
            {
                "target_hosts": ",".join(db.db_info.hosts)
                if isinstance(db.db_info.hosts, list)
                else db.db_info.hosts,
                "workload": db.db_info.workload_name,
                "threads": db.threads,
                "clients": db.clients,
                "db_user": db.db_info.username,
                "db_password": db.db_info.password,
                "duration": db.duration,
                "workload_params": db.db_info.workload_params,
                "extra_labels": labels,
                "extra_config": str(db.extra) + " " + extra_config,
            },
        )
        return daemon_reload()

    def render_workload_parameters(
        self,
        db: DPBenchmarkExecutionModel,
        workload_name: str,
    ):
        """Renders the workload parameters file."""
        _render(
            "workload_parameter_templates/" + workload_name + ".json.j2",
            self.workload_parameter_path,
            {
                "index_name": db.db_info.db_name,
                "clients": db.clients,
                "duration": db.duration,
            },
        )

    def is_prepared(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        return (
            os.path.exists(self.svc_path)
            and os.path.exists(self.workload_parameter_path)
            and os.path.exists(self.executable)
        )

    def prepare(
        self,
        db: DPBenchmarkExecutionModel,
        workload_parameter_template_path: str,
        labels: Optional[str] = "",
        extra_config: Optional[str] = "",
    ) -> bool:
        """Prepare the benchmark service."""
        try:
            if not self.render_service_file(
                db=db,
                labels=labels,
                extra_config=extra_config,
            ):
                return False
            self.render_workload_parameters(
                db=db,
                labels=labels,
                workload_parameter_template_path=workload_parameter_template_path,
            )
        except Exception:
            return False
        return True

    def is_running(self) -> bool:
        """Checks if the benchmark service is running."""
        return self.is_prepared() and service_running(self.SVC_NAME)

    def is_stopped(self) -> bool:
        """Checks if the benchmark service has stopped."""
        return self.is_prepared() and not self.is_running() and not self.is_failed()

    def is_failed(self) -> bool:
        """Checks if the benchmark service has failed."""
        return self.is_prepared() and service_failed(self.SVC_NAME)

    def run(self) -> bool:
        """Run the benchmark service."""
        if self.is_stopped() or self.is_failed():
            return service_restart(self.SVC_NAME)
        return self.is_running()

    def stop(self) -> bool:
        """Stop the benchmark service."""
        if self.is_running():
            return service_stop(self.SVC_NAME)
        return self.is_stopped()

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
