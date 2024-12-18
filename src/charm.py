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
from functools import cached_property
from typing import Any, Optional

import ops
import charms.operator_libs_linux.v0.apt as apt
from charms.data_platform_libs.v0.data_interfaces import KafkaRequires
from charms.kafka.v0.client import KafkaClient, NewTopic
from ops.charm import CharmBase
from ops.model import Application, Relation, Unit
from overrides import override
from ops.framework import EventBase
from ops.model import BlockedStatus

from benchmark.literals import (
    DPBenchmarkMissingOptionsError,
)
from benchmark.base_charm import DPBenchmarkCharmBase
from benchmark.core.models import (
    DatabaseState,
    RelationState,
    DPBenchmarkBaseDatabaseModel,
)
from benchmark.core.workload_base import WorkloadBase
from benchmark.events.db import DatabaseRelationHandler
from benchmark.events.peer import PeerRelationHandler
from benchmark.literals import PEER_RELATION
from benchmark.managers.config import ConfigManager
from benchmark.managers.lifecycle import LifecycleManager
from literals import CLIENT_RELATION_NAME, TOPIC_NAME
from benchmark.literals import DPBenchmarkLifecycleTransition


# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


KAFKA_WORKER_PARAMS_TEMPLATE = """name: Kafka-benchmark
driverClass: io.openmessaging.benchmark.driver.kafka.KafkaBenchmarkDriver

# Kafka client-specific configuration
replicationFactor: {{ total_number_of_brokers }}

topicConfig: |
  min.insync.replicas={{ total_number_of_brokers }}

commonConfig: |
  security.protocol=SASL_PLAINTEXT
  {{ list_of_brokers_bootstrap }}
  sasl.mechanism=SCRAM-SHA-512
  sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username={{ username }} password={{ password }};

producerConfig: |
  max.in.flight.requests.per.connection={{ threads }}
  retries=2147483647
  acks=all
  linger.ms=1
  batch.size=1048576

consumerConfig: |
  auto.offset.reset=earliest
  enable.auto.commit=false
  max.partition.fetch.bytes=10485760
"""

KAFKA_WORKLOAD_PARAMS_TEMPLATE = """name: {{ partitionsPerTopic }} producer / {{ partitionsPerTopic }} consumers on 1 topic

topics: 1
partitionsPerTopic: {{ partitionsPerTopic }}
messageSize: 1024
payloadFile: "{{ charm_root }}/openmessaging-benchmark/payload/payload-1Kb.data"
subscriptionsPerTopic: {{ partitionsPerTopic }}
consumerPerSubscription: 1
producersPerTopic: {{ partitionsPerTopic }}
producerRate: 50000
consumerBacklogSizeGB: 0
testDurationMinutes: {{ duration }}
"""

KAFKA_SYSTEMD_SERVICE_TEMPLATE = """[Unit]
Description=Service for controlling kafka openmessaging benchmark
Wants=network.target
Requires=network.target

[Service]
EnvironmentFile=-/etc/environment
Environment=PYTHONPATH={{ charm_root }}/lib:{{ charm_root }}/venv:{{ charm_root }}/src/benchmark/wrapper
ExecStart={{ charm_root }}/src/wrapper.py --test_name={{ test_name }} --command={{ command }} --workload={{ workload_name }} --threads={{ threads }} --parallel_processes={{ parallel_processes }} --duration={{ duration }} --peers={{ peers }} --extra_labels={{ labels }} {{ extra_config }}
Restart=no
TimeoutSec=600
Type=simple
"""

WORKER_PARAMS_YAML_FILE = "worker_params.yaml"
TEN_YEARS_IN_MINUTES = 5_256_000


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

    def get(self) -> DPBenchmarkBaseDatabaseModel | None:
        """Returns the value of the key."""
        if not self.relation or not (endpoints := self.remote_data.get("endpoints")):
            return None

        dbmodel = super().get()
        return DPBenchmarkBaseDatabaseModel(
            hosts=endpoints.split(","),
            unix_socket=dbmodel.unix_socket,
            username=dbmodel.username,
            password=dbmodel.password,
            db_name=dbmodel.db_name,
            tls=self.tls,
            tls_ca=self.tls_ca,
        )


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
        if not self.state.tls_ca:
            return self.state.tls, None
        return self.state.tls, self.state.tls_ca


class KafkaPeersRelationHandler(PeerRelationHandler):
    """Listens to all the peer-related events and react to them."""

    @override
    def peers(self) -> list[str]:
        """Return the peers."""
        return [
            f"{self.relation.data[u]['ingress-address']}:{8080 + 2 * port}"
            for u in list(self.units()) + [self.this_unit()]
            for port in range(0, self.charm.config.get("parallel_processes"))
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
        self.workload.worker_params_template = KAFKA_WORKER_PARAMS_TEMPLATE

        self.config = config
        self.peer = peer
        self.database = database
        self.labels = labels

    @override
    def _render_service(
        self,
        transition: DPBenchmarkLifecycleTransition,
        dst_path: str | None = None,
    ) -> str | None:
        """Render the workload parameters."""
        values = self.get_execution_options().dict() | {
            "charm_root": os.environ.get("CHARM_DIR", ""),
            "command": transition.value,
        }
        return self._render(
            values=values,
            template_file=None,
            template_content=KAFKA_SYSTEMD_SERVICE_TEMPLATE,
            dst_filepath=dst_path,
        )

    @override
    def _check(
        self,
        transition: DPBenchmarkLifecycleTransition,
    ) -> bool:
        if not (
            os.path.exists(self.workload.paths.service)
            and os.path.exists(self.workload.paths.workload_params)
            and (values := self.get_execution_options())
        ):
            return False
        values = values.dict() | {
            "charm_root": os.environ.get("CHARM_DIR", ""),
            "command": transition.value,
            "target_hosts": values.db_info.hosts,
        }
        compare_svc = "\n".join(self.workload.read(self.workload.paths.service)) == self._render(
            values=values,
            template_file=None,
            template_content=KAFKA_SYSTEMD_SERVICE_TEMPLATE,
            dst_filepath=None,
        )
        compare_params = "\n".join(
            self.workload.read(self.workload.paths.workload_params)
        ) == self._render(
            values=self.get_workload_params(),
            template_file=None,
            template_content=self.workload.workload_params_template,
            dst_filepath=None,
        )
        return compare_svc and compare_params

    def get_worker_params(self) -> dict[str, Any]:
        """Return the workload parameters."""
        db = self.database.state.get()

        return {
            "total_number_of_brokers": len(self.peer.units()) + 1,
            # We cannot have quotes nor brackets in this string.
            # Therefore, we render the entire line instead
            "list_of_brokers_bootstrap": "bootstrap.servers={}".format(",".join(self.database.bootstrap_servers())),
            "username": db.username,
            "password": db.password,
            "threads": self.config.get("threads", 1) if self.config.get("threads") > 0 else 1,
        }

    def _render_worker_params(
            self,
            dst_path: str | None = None,
        ) -> str|None:
        """Render the worker parameters."""
        return self._render(
            values=self.get_worker_params(),
            template_file=None,
            template_content=self.workload.worker_params_template,
            dst_filepath=dst_path,
        )

    @override
    def get_workload_params(self) -> dict[str, Any]:
        """Return the worker parameters."""
        return {
            "partitionsPerTopic": self.config.get("parallel_processes"),
            "duration": int(self.config.get("duration") / 60) if self.config.get("duration") > 0 else TEN_YEARS_IN_MINUTES,
            "charm_root": os.environ.get("CHARM_DIR", ""),
        }

    @override
    def _render_params(
        self,
        dst_path: str | None = None,
    ) -> str | None:
        """Render the workload parameters.

        Overloaded as we need more than one file to be rendered.

        This extra file will rendered in the same folder as `dst_path`, but with a different name.
        """
        super()._render_params(dst_path)
        self._render_worker_params(
            os.path.join(
                os.path.dirname(os.path.abspath(dst_path)),
                WORKER_PARAMS_YAML_FILE,
            )
        )

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
            labels=self.labels,
        )
        self.lifecycle = LifecycleManager(self.peers, self.config_manager)

        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    @override
    def _on_install(self, event: EventBase) -> None:
        """Install the charm."""
        apt.add_package("openjdk-18-jre", update_cache=True)

    @override
    def _preflight_checks(self) -> bool:
        """Check if we have the necessary relations.

        In kafka case, we need the client relation to be able to connect to the database.
        """
        if self.config.get("parallel_processes") < 2:
            logger.error("The number of parallel processes must be greater than 1.")
            self.unit.status = BlockedStatus("The number of parallel processes must be greater than 1.")
            return False
        return super()._preflight_checks()

    @override
    def _on_config_changed(self, event):
        """Handle the config changed event."""
        if not self._preflight_checks():
            event.defer()
            return
        return super()._on_config_changed(event)

    @override
    def supported_workloads(self) -> list[str]:
        """List of supported workloads."""
        return ["default"]


if __name__ == "__main__":
    ops.main(KafkaBenchmarkOperator)
