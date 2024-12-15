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
from charms.kafka.v0.client import KafkaClient, NewTopic
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
from benchmark.managers.lifecycle import LifecycleManager
from literals import CLIENT_RELATION_NAME, TOPIC_NAME
from functools import cached_property

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


KAFKA_WORKLOAD_PARAMS_TEMPLATE = """name: Kafka-benchmark
driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver

# Kafka client-specific configuration
replicationFactor: {{ total_number_of_brokers }}

topicConfig: |
min.insync.replicas={{ total_number_of_brokers }}

commonConfig: |
security.protocol=SASL_PLAINTEXT
bootstrap.servers={{ list_of_brokers_bootstrap }}
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username={{ username }} password={{ password }};

producerConfig: |
enable.idempotence=true
max.in.flight.requests.per.connection=1
retries=2147483647
acks=all
linger.ms=1
batch.size=1048576

consumerConfig: |
auto.offset.reset=earliest
enable.auto.commit=false
max.partition.fetch.bytes=10485760
"""


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
        if not (
            self.relation and self.client and self.relation.id in self.client.fetch_relation_data()
        ):
            logger.error("Relation data not found")
            # We may have an error if the relation is gone but self.relation.id still exists
            # or if the relation is not found in the fetch_relation_data yet
            return KafkaDatabaseState(
                self.charm.app,
                None,
            )
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
        return self.state.get().hosts

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
        db = self.database.state.get()

        return {
            "total_number_of_brokers": len(self.peer.units()) + 1,
            "list_of_brokers_bootstrap": self.database.bootstrap_servers(),
            "username": db.username,
            "password": db.password,
        }

    @override
    def prepare(self) -> bool:
        """Prepare the benchmark service."""
        # First, clean if a topic already existed
        self.clean()
        try:
            topic = NewTopic(
                name=self.database.state.get().db_name,
                num_partitions=self.config.get("threads") * self.config.get("parallel_processes"),
                replication_factor=self.client.replication_factor,
            )
            self.client.create_topic(topic)
        except Exception as e:
            logger.debug(f"Error creating topic: {e}")

        # We may fail to create the topic, as the relation has been recently stablished
        return self.is_prepared()

    @override
    def is_prepared(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        try:
            return self.database.state.get().db_name in self.client._admin_client.list_topics()
        except Exception as e:
            logger.info(f"Error describing topic: {e}")
            return False

    @override
    def clean(self) -> bool:
        """Clean the benchmark service."""
        try:
            self.client.delete_topics([self.database.state.get().db_name])
        except Exception as e:
            logger.info(f"Error deleting topic: {e}")
        return self.is_cleaned()

    @override
    def is_cleaned(self) -> bool:
        """Checks if the benchmark service has passed its "prepare" status."""
        try:
            return self.database.state.get().db_name not in self.client._admin_client.list_topics()
        except Exception as e:
            logger.info(f"Error describing topic: {e}")
            return False

    @cached_property
    def client(self) -> KafkaClient:
        """Return the Kafka client."""
        state = self.database.state.get()
        return KafkaClient(
            servers=self.database.bootstrap_servers(),
            username=state.username,
            password=state.password,
            security_protocol="SASL_SSL" if (state.tls or state.tls_ca) else "SASL_PLAINTEXT",
            cafile_path=state.tls_ca,
            certfile_path=state.tls,
            replication_factor=len(self.peer.units()) + 1,
        )


class KafkaBenchmarkOperator(DPBenchmarkCharmBase):
    """Charm the service."""

    def __init__(self, *args):
        self.workload_params_template = KAFKA_WORKLOAD_PARAMS_TEMPLATE

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
        self.lifecycle = LifecycleManager(self.peers, self.config_manager)

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
