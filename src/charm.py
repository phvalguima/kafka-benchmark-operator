#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This connects the benchmark service to the database and the grafana agent.

The first action after installing the benchmark charm and relating it to the different
apps, is to prepare the db. The user must run the prepare action to create the database.

The prepare action will run the benchmark prepare command to create the database and, at its
end, it sets a systemd target informing the service is ready.

The next step is to execute the run action. This action renders the systemd service file and
starts the service. If the target is missing, then service errors and returns an error to
the user.
"""

import logging
import os
import subprocess
from typing import Any, Optional

import ops
from charms.data_platform_libs.v0.data_interfaces import OpenSearchRequires
from ops.charm import CharmBase, EventBase
from ops.model import ActiveStatus, Application, BlockedStatus, MaintenanceStatus, Relation, Unit
from overrides import override
from pydantic import error_wrappers

from benchmark.base_charm import DPBenchmarkCharmBase
from benchmark.benchmark_workload_base import DPBenchmarkSystemdService
from benchmark.core.models import (
    DatabaseState,
    DPBenchmarkBaseDatabaseModel,
    DPBenchmarkExecutionExtraConfigsModel,
    DPBenchmarkExecutionModel,
)
from benchmark.events.db import DatabaseRelationHandler
from benchmark.literals import DPBenchmarkMissingOptionsError
from benchmark.managers.config import ConfigManager
from literals import INDEX_NAME, OpenSearchExecutionExtraConfigsModel

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class OpenSearchConfigManager(ConfigManager):
    """The config manager class."""

    def __init__(
        self,
        workload: DPBenchmarkSystemdService,
        database: DatabaseState,
        config: dict[str, Any],
    ):
        self.workload = workload
        self.config = config
        self.database = database

    @override
    def get_execution_options(
        self,
        extra_config: DPBenchmarkExecutionExtraConfigsModel | None = None,
    ) -> Optional[DPBenchmarkExecutionModel]:
        """Returns the execution options."""
        return super().get_execution_options(
            extra_config=extra_config
            or OpenSearchExecutionExtraConfigsModel(
                run_count=self.config.get("run_count", 0),
                test_mode=self.config.get("test_mode", False),
            )
        )


class OpenSearchDatabaseState(DatabaseState):
    """State collection for the database relation."""

    def __init__(
        self, component: Application | Unit, relation: Relation, client: OpenSearchRequires
    ):
        super().__init__(
            component=component,
            relation=relation,
        )
        self.database_key = "index"
        self.client = client

    @property
    @override
    def remote_data(self) -> dict[str, str]:
        """Returns the relation data."""
        return list(self.client.fetch_relation_data().values())[0]

    @override
    def get(self) -> DPBenchmarkBaseDatabaseModel | None:
        """Returns the value of the key."""
        if not self.relation or not (endpoints := self.remote_data.get("endpoints")):
            return None

        unix_socket = None
        if endpoints.startswith("file://"):
            unix_socket = endpoints[7:]
        try:
            return DPBenchmarkBaseDatabaseModel(
                hosts=[f"https://{ep}" for ep in endpoints.split()],
                unix_socket=unix_socket,
                username=self.remote_data.get("username"),
                password=self.remote_data.get("password"),
                db_name=self.remote_data.get(self.database_key),
            )
        except error_wrappers.ValidationError as e:
            logger.warning(f"Failed to validate the database model: {e}")
            entries = [entry.get("loc")[0] for entry in e.errors()]
            raise DPBenchmarkMissingOptionsError(f"{entries}")
        return None


class OpenSearchDatabaseRelationHandler(DatabaseRelationHandler):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """

    DATABASE_KEY = "index"

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
    ):
        super().__init__(charm, relation_name)
        self.state = OpenSearchDatabaseState(self.charm.app, self.relation, client=self.client)

    @property
    def client(self) -> Any:
        """Returns the data_interfaces client corresponding to the database."""
        return OpenSearchRequires(
            self.charm,
            "opensearch",
            INDEX_NAME,
            extra_user_roles="admin",
        )


class OpenSearchBenchmarkOperator(DPBenchmarkCharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args, db_relation_name="opensearch")
        self.labels = ",".join([self.model.name, self.unit.name.replace("/", "-")])
        self.database = OpenSearchDatabaseRelationHandler(
            self,
            "opensearch",
        )
        self.config_manager = OpenSearchConfigManager(
            workload=self.workload,
            database=self.database.state,
            config=self.config,
        )

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    @override
    def _on_install(self, event: EventBase) -> None:
        super()._on_install(event)
        self.unit.status = MaintenanceStatus("Installing packages...")
        self._install_packages(["python3-pip", "python3-prometheus-client"])

        if os.path.exists("/usr/lib/python3.12/EXTERNALLY-MANAGED"):
            os.remove("/usr/lib/python3.12/EXTERNALLY-MANAGED")

    def _on_start(self, event: EventBase) -> None:
        """Start the service."""
        # sudo guarantees we are installing this dependency system-wide instead of only
        # for the charm. It also ensures we are not setting PYTHONPATH, given we have
        # it set in the charm itself, coming from "./dispatch"
        subprocess.run("sudo pip3 install opensearch-benchmark", shell=True)

        self.config_manager.render_service_executable()
        self.unit.status = ActiveStatus()

    def _on_config_changed(self, event: EventBase) -> None:
        # We need to narrow the options of workload_name to the supported ones
        if self.config.get("workload_name", "nyc_taxis") not in self.supported_workloads():
            self.unit.status = BlockedStatus("Unsupported workload")
            logger.error(f"Unsupported workload {self.config.get('workload_name', 'nyc_taxis')}")
            return
        return super()._on_config_changed(event)


if __name__ == "__main__":
    ops.main(OpenSearchBenchmarkOperator)
