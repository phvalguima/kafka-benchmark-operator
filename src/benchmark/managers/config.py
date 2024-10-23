# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The config manager class.

This class summarizes all the configuration needed for the workload execution
and returns a model containing that information.
"""

import os
import shutil
from typing import Any, Optional

from charms.operator_libs_linux.v1.systemd import daemon_reload
from jinja2 import Environment, FileSystemLoader, exceptions

from benchmark.core.models import (
    DatabaseState,
    DPBenchmarkExecutionExtraConfigsModel,
    DPBenchmarkExecutionModel,
)
from benchmark.core.workload_base import WorkloadBase


class ConfigManager:
    """The config manager class."""

    def __init__(
        self,
        workload: WorkloadBase,
        database: DatabaseState,
        config: dict[str, Any],
    ):
        self.workload = workload
        self.config = config
        self.database = database

    def get_execution_options(
        self,
        extra_config: DPBenchmarkExecutionExtraConfigsModel | None = None,
    ) -> Optional[DPBenchmarkExecutionModel]:
        """Returns the execution options.

        Raises:
            DPBenchmarkMissingOptionsError: If the database is not ready.
        """
        if not (db := self.database.get()):
            # It means we are not yet ready. Return None
            # This check also serves to ensure we have only one valid relation at the time
            return None
        return DPBenchmarkExecutionModel(
            threads=self.config.get("threads"),
            duration=self.config.get("duration"),
            clients=self.config.get("clients"),
            db_info=db,
            workload_name=self.config.get("workload_name"),
            workload_params=self._generate_workload_params(),
            extra=extra_config,
        )

    def prepare(
        self,
        workload_name: str,
        labels: Optional[str] = "",
    ) -> bool:
        """Prepare the benchmark service."""
        try:
            self.render_workload_parameters(
                workload_name=workload_name,
            )
            if not self.render_service_file(
                labels=labels,
            ):
                return False
        except Exception:
            return False
        return True

    def render_service_executable(self) -> bool:
        """Render the benchmark service executable."""
        shutil.copyfile(
            "templates/" + self.workload.paths.svc_name + ".py", self.workload.paths.script
        )
        os.chmod(
            self.workload.paths.script,
            0o755,
        )

    def render_service_file(
        self,
        labels: Optional[str] = "",
    ) -> bool:
        """Render the systemd service file."""
        if not (db := self.get_execution_options()):
            return False
        config = {
            "target_hosts": ",".join(db.db_info.hosts)
            if isinstance(db.db_info.hosts, list)
            else db.db_info.hosts,
            "workload": db.workload_name,
            "threads": db.threads,
            "clients": db.clients,
            "db_user": db.db_info.username,
            "db_password": db.db_info.password,
            "duration": db.duration,
            "workload_params": self.workload.paths.workload_parameters,
            "extra_labels": labels,
        }
        if db.extra:
            config["extra_config"] = db.extra
        self._render(
            self.workload.paths.svc_name + ".service.j2",
            config,
            dst_filepath=self.workload.paths.service,
        )
        return daemon_reload()

    def render_workload_parameters(
        self,
        workload_name: str,
    ):
        """Renders the workload parameters file."""
        self._render(
            "workload_parameter/" + workload_name + ".json.j2",
            self.get_execution_options(),
            self.workload.paths.workload_parameters,
        )

    def _render(
        self,
        template_file: str,
        values: dict[str, Any],
        dst_filepath: str | None = None,
    ) -> str:
        """Renders files and return its contents."""
        template_env = Environment(loader=FileSystemLoader(self.workload.paths.templates))
        try:
            template = template_env.get_template(template_file)
            content = template.render(values)
        except exceptions.TemplateNotFound as e:
            raise e
        if not dst_filepath:
            return content
        self.workload.write(content, dst_filepath)

    def unset(self) -> bool:
        """Unset the benchmark service."""
        try:
            if not self.workload.stop():
                return False
            os.remove(self.workload.paths.service)
            os.remove(self.workload.paths.workload_parameters)
            if not daemon_reload():
                return False
        except Exception:
            return False
        return True

    def _generate_workload_params(self):
        return {}
