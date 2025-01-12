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

import charms.operator_libs_linux.v0.apt as apt
import ops
from charms.data_platform_libs.v0.data_interfaces import KafkaRequires
from charms.kafka.v0.client import KafkaClient, NewTopic
from ops.charm import CharmBase
from ops.framework import EventBase
from ops.model import Application, BlockedStatus, Relation, Unit
from overrides import override

from benchmark.base_charm import DPBenchmarkCharmBase
from benchmark.core.models import (
    DatabaseState,
    DPBenchmarkBaseDatabaseModel,
    RelationState,
)
from benchmark.core.workload_base import WorkloadBase
from benchmark.events.db import DatabaseRelationHandler
from benchmark.events.peer import PeerRelationHandler
from benchmark.literals import (
    PEER_RELATION,
    DPBenchmarkLifecycleState,
    DPBenchmarkLifecycleTransition,
)
from benchmark.managers.config import ConfigManager
from benchmark.managers.lifecycle import LifecycleManager
from literals import CLIENT_RELATION_NAME, JAVA_VERSION, TOPIC_NAME
from models import WorkloadTypeParameters

# TODO: This file must go away once Kafka starts sharing its certificates via client relation
from tls import JavaTlsHandler, JavaTlsStoreManager

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
  sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="{{ username }}" password="{{ password }}";
  {% if truststore_path and truststore_pwd -%}
  security.protocol=SASL_SSL
  ssl.truststore.location={{ truststore_path }}
  ssl.truststore.password={{ truststore_pwd }}
  {%- endif %}
  {% if keystore_path and keystore_pwd -%}
  ssl.keystore.location={{ keystore_path }}
  ssl.keystore.password={{ keystore_pwd }}
  {%- endif %}
  ssl.client.auth={{ ssl_client_auth }}

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
messageSize: {{ messageSize }}
payloadFile: "{{ charm_root }}/openmessaging-benchmark/payload/payload-1Kb.data"
subscriptionsPerTopic: {{ partitionsPerTopic }}
consumerPerSubscription: 1
producersPerTopic: {{ partitionsPerTopic }}
producerRate: {{ producerRate }}
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
ExecStart={{ charm_root }}/src/wrapper.py --test_name={{ test_name }} --command={{ command }} --is_coordinator={{ is_coordinator }} --workload={{ workload_name }} --threads={{ threads }} --parallel_processes={{ parallel_processes }} --duration={{ duration }} --peers={{ peers }} --extra_labels={{ labels }} {{ extra_config }}
Restart=no
KillSignal=SIGKILL
TimeoutSec=600
Type=simple
"""

WORKER_PARAMS_YAML_FILE = "worker_params.yaml"
TEN_YEARS_IN_MINUTES = 5_256_000


class KafkaDatabaseState(DatabaseState):
    """State collection for the database relation."""

    def __init__(
        self,
        component: Application | Unit,
        relation: Relation | None,
        data: dict[str, Any] = {},
        tls_relation: Relation
        | None = None,  # TODO: remove once Kafka emits the TLS data via client relation
    ):
        super().__init__(
            component=component,
            relation=relation,
            data=data,
        )
        self.database_key = "topic"
        # TODO: remove once Kafka emits the TLS data via client relation
        self.tls_relation = tls_relation

    @property
    @override
    def tls_ca(self) -> str | None:
        """Return the TLS CA."""
        if not super().tls_ca:
            return None
        self.tls_relation

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
            data=self.client.fetch_relation_data()[self.relation.id] | {},
        )

    @property
    @override
    def client(self) -> Any:
        """Returns the data_interfaces client corresponding to the database."""
        return self._internal_client

    def bootstrap_servers(self) -> list[str] | None:
        """Return the bootstrap servers."""
        return self.state.get().hosts

    def tls(self) -> tuple[str, str] | None:
        """Return the TLS certificates."""
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
        java_tls: JavaTlsStoreManager,
        peer: KafkaPeersRelationHandler,
        config: dict[str, Any],
        is_leader: bool,
        test_name: Optional[str] = None,
        labels: Optional[str] = "",
    ):
        super().__init__(workload, database, peer, config, labels, test_name)
        self.workload = workload
        self.workload.worker_params_template = KAFKA_WORKER_PARAMS_TEMPLATE
        self.java_tls = java_tls

        self.config = config
        self.peer = peer
        self.database = database
        self.is_leader = is_leader
        self.labels = labels

    def _service_args(
        self, args: dict[str, Any], transition: DPBenchmarkLifecycleTransition
    ) -> dict[str, Any]:
        return args | {
            "charm_root": self.workload.paths.charm_dir,
            "command": transition.value,
            "is_coordinator": "" if not self.is_leader else "True",
        }

    @override
    def _render_service(
        self,
        transition: DPBenchmarkLifecycleTransition,
        dst_path: str | None = None,
    ) -> str | None:
        """Render the workload parameters."""
        values = self._service_args(self.get_execution_options().dict(), transition)
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
        values = self._service_args(values.dict(), transition)
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
        # Generate the truststore, if applicable
        self.java_tls.set()

        db = self.database.state.get()
        return {
            "total_number_of_brokers": len(self.database.bootstrap_servers()) - 1,
            # We cannot have quotes nor brackets in this string.
            # Therefore, we render the entire line instead
            "list_of_brokers_bootstrap": "bootstrap.servers={}".format(
                ",".join(self.database.bootstrap_servers())
            ),
            "username": db.username,
            "password": db.password,
            "threads": self.config.get("threads", 1) if self.config.get("threads") > 0 else 1,
            "truststore_path": self.java_tls.java_paths.truststore,
            "truststore_pwd": self.java_tls.truststore_pwd,
            "ssl_client_auth": "none",
        }

    def _render_worker_params(
        self,
        dst_path: str | None = None,
    ) -> str | None:
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
        workload = WorkloadTypeParameters[self.config.get("workload_profile")]
        return {
            "partitionsPerTopic": self.config.get("parallel_processes"),
            "duration": int(self.config.get("duration") / 60)
            if self.config.get("duration") > 0
            else TEN_YEARS_IN_MINUTES,
            "charm_root": self.workload.paths.charm_dir,
            "producerRate": workload.producer_rate,
            "messageSize": workload.message_size,
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
        super().prepare()

        # First, clean if a topic already existed
        self.clean()
        try:
            topic = NewTopic(
                name=self.database.state.get().db_name,
                num_partitions=self.config.get("parallel_processes"),
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
            self.workload.remove(self.workload.paths.service)
            self.workload.reload()

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
        has_tls_ca = False
        if os.path.exists(self.java_tls.java_paths.ca):
            has_tls_ca = True

        state = self.database.state.get()
        return KafkaClient(
            servers=self.database.bootstrap_servers(),
            username=state.username,
            password=state.password,
            security_protocol="SASL_SSL" if has_tls_ca else "SASL_PLAINTEXT",
            cafile_path=self.java_tls.java_paths.ca,
            certfile_path=None,
            replication_factor=len(self.database.bootstrap_servers()) - 1,
        )


class KafkaLifecycleManager(LifecycleManager):
    """The lifecycle manager class.

    There is one extra case we must consider: before moving to running, we need to ensure
    all the peers are in the same state are already running.
    """

    def __init__(
        self,
        peers: PeerRelationHandler,
        config_manager: ConfigManager,
        is_leader: bool = False,
    ):
        super().__init__(peers, config_manager)
        self.is_leader = is_leader

    @override
    def _peers_state(self) -> DPBenchmarkLifecycleState | None:
        next_state = super()._peers_state()

        # Now, there is a special rule: if we are the leader unit, we can only move to RUNNING
        # if all the peers are already running or if peer count is zero
        if not self.is_leader:
            return next_state

        # Check the special case for running && leader
        if next_state == DPBenchmarkLifecycleState.RUNNING:
            if len(self.peers.units()) == 0:
                return DPBenchmarkLifecycleState.RUNNING
            # Now, as we are not the only unit and we are the leader, we must ensure everyone
            # is in RUNNING state before moving forward:
            if self.check_all_peers_in_state(DPBenchmarkLifecycleState.RUNNING):
                return DPBenchmarkLifecycleState.RUNNING

            # None of the conditions met, we return an empty state
            return None

        return next_state


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
        self.tls_handler = JavaTlsHandler(self)

        self.java_tls_manager = self.tls_handler.tls_manager
        self.config_manager = KafkaConfigManager(
            workload=self.workload,
            database=self.database,
            java_tls=self.java_tls_manager,
            peer=self.peer_handler,
            config=self.config,
            is_leader=self.unit.is_leader(),
            labels=self.labels,
            test_name=self.peers.test_name,
        )

        self.lifecycle = KafkaLifecycleManager(
            self.peers, self.config_manager, self.unit.is_leader()
        )
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

    @override
    def _on_install(self, event: EventBase) -> None:
        """Install the charm."""
        apt.add_package(f"openjdk-{JAVA_VERSION}-jre", update_cache=True)

    @override
    def _preflight_checks(self) -> bool:
        """Check if we have the necessary relations.

        In kafka case, we need the client relation to be able to connect to the database.
        """
        if self.config.get("parallel_processes") * (len(self.peers.units()) + 1) < 2:
            logger.error("The number of parallel processes must be greater than 1.")
            self.unit.status = BlockedStatus(
                "The number of parallel processes or peers must be greater than 1."
            )
            return False
        return super()._preflight_checks()

    @override
    def on_run_action(self, event: EventBase) -> None:
        """Process the run action.

        This method avoids execution of RUN if this unit is a leader. Only non-leaders can
        kickstart the RUN logic as we need all non-leaders to be RUNNING before leader can start.
        """
        if self.unit.is_leader():
            event.fail("Only non-leaders can start the benchmark")
            return
        super().on_run_action(event)

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
        return list(WorkloadTypeParameters.keys())


if __name__ == "__main__":
    ops.main(KafkaBenchmarkOperator)
