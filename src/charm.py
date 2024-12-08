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
from typing import Any, Optional

import ops
from charms.data_platform_libs.v0.data_interfaces import KafkaRequires
from ops.charm import CharmBase, EventBase
from ops.model import Application, BlockedStatus, Relation, Unit
from overrides import override

from benchmark.base_charm import DPBenchmarkCharmBase
from benchmark.core.models import (
    DatabaseState,
    RelationState,
)
from benchmark.core.workload_base import WorkloadBase
from benchmark.events.db import DatabaseRelationHandler
from benchmark.events.peer import PeerRelationHandler
from benchmark.literals import PEER_RELATION
from benchmark.managers.config import ConfigManager
from literals import CLIENT_RELATION_NAME, TOPIC_NAME

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class KafkaDatabaseState(DatabaseState):
    """State collection for the database relation."""

    def __init__(
        self, component: Application | Unit, relation: Relation | None, data: dict[str, Any] = {}
    ):
        super().__init__(
            component=component,
            relation=relation,
            data=data,
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
        self.consumer_prefix = f"{charm.model.name}-{charm.app.name}-benchmark-consumer"
        # We can have only one reference to a Requires object.
        # Hence, we will store it here:
        self._internal_client = KafkaRequires(
            self.charm,
            self.relation_name,
            TOPIC_NAME,
            extra_user_roles="admin",
            consumer_group_prefix=self.consumer_prefix,
        )

    @property
    @override
    def state(self) -> RelationState:
        """Returns the state of the database."""
        return KafkaDatabaseState(
            self.charm.app,
            self.relation,
            data=self.client.fetch_relation_data()[self.relation.id],
        )

    @property
    @override
    def client(self) -> Any:
        """Returns the data_interfaces client corresponding to the database."""
        return self._internal_client

    def bootstrap_servers(self) -> str | None:
        """Return the bootstrap servers."""
        return self.client.endpoints

    def tls(self) -> tuple[str, str] | None:
        """Return the tls."""
        if not self.state.tls:
            return None, None
        if not self.state.tls_ca:
            return self.state.tls, None
        return self.state.tls, self.state.tls_ca


class KafkaPeersRelationHandler(PeerRelationHandler):
    """Listens to all the peer-related events and react to them."""

    @override
    def workers(self) -> list[str]:
        """Return the peer workers."""
        return [
            f"http://{self.relation.data[u]['ingress-address']}:{port}"
            for u in self.units() + [self.this_unit()]
            for port in range(8080, 8080 + self.charm.config.get("parallel_processes"))
        ]


class KafkaConfigManager(ConfigManager):
    """The config manager class."""

    def __init__(
        self,
        workload: WorkloadBase,
        database: KafkaDatabaseRelationHandler,
        peer: KafkaPeersRelationHandler,
        config: dict[str, Any],
        labels: Optional[str] = "",
    ):
        self.workload = workload
        self.config = config
        self.peer = peer
        self.database = database
        self.labels = labels

    @override
    def get_workload_params(self) -> dict[str, Any]:
        """Return the workload parameters."""
        return {
            "total_number_of_brokers": len(self.peer.units()) + 1,
            "list_of_brokers_bootstrap": self.database.bootstrap_servers(),
            "username": self.database.client.username,
            "password": self.database.client.password,
        }


class KafkaBenchmarkOperator(DPBenchmarkCharmBase):
    """Charm the service."""

    def __init__(self, *args):
        super().__init__(*args, db_relation_name=CLIENT_RELATION_NAME)
        self.labels = ",".join([self.model.name, self.unit.name.replace("/", "-")])

        self.database = KafkaDatabaseRelationHandler(
            self,
            CLIENT_RELATION_NAME,
        )
        self.peer_handler = KafkaPeersRelationHandler(self, PEER_RELATION)
        self.config_manager = KafkaConfigManager(
            workload=self.workload,
            database=self.database,
            peer=self.peer_handler,
            config=self.config,
        )
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    def supported_workloads(self) -> list[str]:
        """List of supported workloads."""
        return ["default"]

    def _on_config_changed(self, event: EventBase) -> None:
        # We need to narrow the options of workload_name to the supported ones
        if self.config.get("workload_name", "default") not in self.supported_workloads():
            self.unit.status = BlockedStatus("Unsupported workload")
            logger.error(f"Unsupported workload {self.config.get('workload_name', 'nyc_taxis')}")
            return
        return super()._on_config_changed(event)


if __name__ == "__main__":
    ops.main(KafkaBenchmarkOperator)
