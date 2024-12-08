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
from typing import Any

import ops
from charms.data_platform_libs.v0.data_interfaces import KafkaRequires

from literals import TOPIC_NAME, CLIENT_RELATION_NAME
from ops.charm import CharmBase, EventBase
from ops.model import Application, BlockedStatus, Relation, Unit
from overrides import override

from benchmark.base_charm import DPBenchmarkCharmBase
from benchmark.core.models import (
    DatabaseState,
)
from benchmark.events.db import DatabaseRelationHandler
from benchmark.managers.config import ConfigManager
from benchmark.core.systemd_workload_base import DPBenchmarkSystemdService
from benchmark.events.peer import PeersRelationHandler

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class KafkaConfigManager(ConfigManager):
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
    def get_workload_params(self) -> dict[str, Any]:
        """Return the workload parameters."""
        return {

        }


class KafkaDatabaseState(DatabaseState):
    """State collection for the database relation."""
    def __init__(self, component: Application | Unit, relation: Relation | None):
        super().__init__(
            component=component,
            relation=relation,
        )
        self.database_key = "topic"


class KafkaDatabaseRelationHandler(DatabaseRelationHandler):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """

    DATABASE_KEY = "topic"

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
    ):
        super().__init__(charm, relation_name)
        self.state = KafkaDatabaseState(self.charm.app, self.relation, client=self.client)

    @override
    @property
    def client(self) -> Any:
        """Returns the data_interfaces client corresponding to the database."""
        return KafkaRequires(
            self.charm,
            self.relation_name,
            TOPIC_NAME,
            extra_user_roles="admin",
        )


class KafkaPeersRelationHandler(PeersRelationHandler):

    @override
    def workers(self) -> list[str]:
        """Return the peer workers."""
        return [
            f"http://{self.relation.data[u]['ingress-address']}:{port}"
            for u in self.units() + [self.this_unit()]
            for port in range(8080, 8080 + self.charm.config.get("parallel_processes"))
        ]


class KafkaBenchmarkOperator(DPBenchmarkCharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args, db_relation_name=CLIENT_RELATION_NAME)
        self.labels = ",".join([self.model.name, self.unit.name.replace("/", "-")])
        self.database = KafkaDatabaseRelationHandler(
            self,
            CLIENT_RELATION_NAME,
        )
        self.config_manager = KafkaConfigManager(
            workload=self.workload,
            database=self.database.state,
            config=self.config,
        )

        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.start, self._on_start)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    def _on_config_changed(self, event: EventBase) -> None:
        # We need to narrow the options of workload_name to the supported ones
        if self.config.get("workload_name", "default") not in self.supported_workloads():
            self.unit.status = BlockedStatus("Unsupported workload")
            logger.error(f"Unsupported workload {self.config.get('workload_name', 'nyc_taxis')}")
            return
        return super()._on_config_changed(event)


if __name__ == "__main__":
    ops.main(KafkaBenchmarkOperator)
