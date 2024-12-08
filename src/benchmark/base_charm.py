# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This class implements the default benchmark workflow.

It is a functional charm that connects the benchmark service to the database and the grafana agent.

The charm should inherit from this class and implement only the specifics for its own tool.

The main step is to execute the run action. This action renders the systemd service file and
starts the service. If the target is missing, then service errors and returns an error to
the user.

This charm should also be the main entry point to all the modelling of your benchmark tool.
"""

import logging
import subprocess
from abc import ABC, abstractmethod
from typing import Any

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from ops.charm import CharmEvents
from ops.framework import EventBase, EventSource
from ops.model import (
    ActiveStatus,
    BlockedStatus,
    WaitingStatus,
)

from benchmark.core.models import DPBenchmarkLifecycleState, PeerState
from benchmark.core.pebble_workload_base import DPBenchmarkPebbleWorkloadBase
from benchmark.core.systemd_workload_base import DPBenchmarkSystemdWorkloadBase
from benchmark.core.workload_base import WorkloadBase
from benchmark.events.db import DatabaseRelationHandler
from benchmark.events.peer import PeerRelationHandler
from benchmark.literals import (
    COS_AGENT_RELATION,
    METRICS_PORT,
    PEER_RELATION,
    DPBenchmarkLifecycleTransition,
    DPBenchmarkMissingOptionsError,
)
from benchmark.managers.config import ConfigManager
from benchmark.managers.lifecycle import LifecycleManager

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class DPBenchmarkCheckUploadEvent(EventBase):
    """Informs to check upload is finished."""


class DPBenchmarkCheckCollectEvent(EventBase):
    """Informs to check collect is finished."""


class DPBenchmarkEvents(CharmEvents):
    """Events used by the charm to check the upload."""

    check_collect = EventSource(DPBenchmarkCheckCollectEvent)
    check_upload = EventSource(DPBenchmarkCheckUploadEvent)


def workload_build() -> WorkloadBase:
    """Build the workload."""
    try:
        # Really simple check to see if we have systemd
        subprocess.check_output(["systemctl", "--help"])
    except subprocess.CalledProcessError:
        return DPBenchmarkPebbleWorkloadBase()
    return DPBenchmarkSystemdWorkloadBase()


class DPBenchmarkCharmBase(ops.CharmBase, ABC):
    """The base benchmark class."""

    on = DPBenchmarkEvents()  # pyright: ignore [reportGeneralTypeIssues]

    RESOURCE_DEB_NAME = "benchmark-deb"

    def __init__(self, *args, db_relation_name: str):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.update_status, self._on_update_status)

        self.framework.observe(self.on.prepare_action, self.on_prepare_action)
        self.framework.observe(self.on.run_action, self.on_run_action)
        self.framework.observe(self.on.stop_action, self.on_stop_action)
        self.framework.observe(self.on.clean_action, self.on_clean_action)

        # Peer relation endpoints
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_joined,
            self._on_peer_changed,
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_changed,
            self._on_peer_changed,
        )
        self.framework.observe(
            self.charm.on[PEER_RELATION].relation_broken,
            self._on_peer_changed,
        )

        self.framework.observe(
            self.charm.on.check_upload,
            self._on_check_upload,
        )
        self.framework.observe(
            self.charm.on.check_collect,
            self._on_check_collect,
        )

        self.database = DatabaseRelationHandler(self, db_relation_name)
        self.peers = PeerRelationHandler(self, PEER_RELATION)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

        self.workload = workload_build()

        self.lifecycle = LifecycleManager(self.peers, self.workload)

        self._grafana_agent = COSAgentProvider(
            self,
            relation_name=COS_AGENT_RELATION,
            metrics_endpoints=[],
            refresh_events=[
                self.on.config_changed,
            ],
            scrape_configs=self.scrape_config,
        )
        self.labels = f"{self.model.name},{self.unit.name}"

        self.config_manager = ConfigManager(
            workload=self.workload,
            database=self.database.state,
            config=self.config,
            labes=self.labels,
        )

    @abstractmethod
    def supported_workload(self) -> list[str]:
        """List of supported workloads."""
        ...

    ###########################################################################
    #
    #  Charm Event Handlers and Internals
    #
    ###########################################################################

    def _on_check_collect(self, event: EventBase) -> None:
        """Check if the upload is finished."""
        if self.config_manager.is_collecting():
            # Nothing to do, upload is still in progress
            event.defer()
            return

        if self.unit.is_leader():
            PeerState(self.model.unit, PEER_RELATION).set(DPBenchmarkLifecycleState.UPLOADING)
            # Raise we are running an upload and we will check the status later
            self.on.check_upload.emit()
            return
        PeerState(self.model.unit, PEER_RELATION).set(DPBenchmarkLifecycleState.FINISHED)

    def _on_check_upload(self, event: EventBase) -> None:
        """Check if the upload is finished."""
        if self.config_manager.is_uploading():
            # Nothing to do, upload is still in progress
            event.defer()
            return
        PeerState(self.model.unit, PEER_RELATION).lifecycle = DPBenchmarkLifecycleState.FINISHED

    def _on_update_status(self, event: EventBase) -> None:
        """Set status for the operator and finishes the service.

        First, we check if there are relations with any meaningful data. If not, then
        this is the most important status to report. Then, we check the details of the
        benchmark service and the benchmark status.
        """
        try:
            status = self.database.state.get()
        except DPBenchmarkMissingOptionsError as e:
            self.unit.status = BlockedStatus(str(e))
            return
        if not status:
            self.unit.status = BlockedStatus("No database relation available")
            return
        self._set_status()

    def _set_status(self) -> None:
        """Recovers the benchmark status."""
        if self.config_manager.is_failed():
            self.unit.status = BlockedStatus("Benchmark failed, please check logs")
        elif self.config_manager.is_running():
            self.unit.status = ActiveStatus("Benchmark is running")
        elif self.config_manager.is_prepared():  # and self.peer_state.is_prepared():
            self.unit.status = WaitingStatus("Benchmark is prepared: execute run to start")
        elif self.config_manager.is_stopped():
            self.unit.status = BlockedStatus("Benchmark is stopped after run")
        else:
            self.unit.status = ActiveStatus()

    def _on_config_changed(self, event: EventBase) -> None:
        """Config changed event."""
        if not self.config_manager.is_prepared():
            # nothing to do: set the status and leave
            self._set_status()
            return

        if not self.config_manager.is_stopped() or not self.config_manager.stop():
            # The stop process may be async so we defer
            logger.warning("Config changed: tried stopping the service but returned False")
            event.defer()
            return
        self.config_manager.run()
        self._set_status()

    def _on_relation_broken(self, _: EventBase) -> None:
        self.config_manager.stop()
        self.config_manager.clean()

    def scrape_config(self) -> list[dict[str, Any]]:
        """Generate scrape config for the Patroni metrics endpoint."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [{"targets": [f"{self._unit_ip()}:{METRICS_PORT}"]}],
                "tls_config": {"insecure_skip_verify": True},
                "scheme": "http",
            }
        ]

    ###########################################################################
    #
    #  Action and Lifecycle Handlers
    #
    ###########################################################################

    def _preflight_checks(self) -> bool:
        """Check if we have the necessary relations."""
        return bool(self.database.state.get()) and bool(self.peers.state.get())

    def on_prepare_action(self, event: EventBase) -> None:
        """Process the prepare action."""
        if not self._preflight_checks():
            event.fail("Missing DB or S3 relations")
            return

        if not self._process_transition(DPBenchmarkLifecycleTransition.PREPARE):
            event.fail("Failed to prepare the benchmark")
        event.set_results({"message": "Benchmark is prepared"})

    def on_run_action(self, event: EventBase) -> None:
        """Process the run action."""
        if not self._preflight_checks():
            event.fail("Missing DB or S3 relations")
            return

        if not self._process_transition(DPBenchmarkLifecycleTransition.RUN):
            event.fail("Failed to run the benchmark")
        event.set_results({"message": "Benchmark is running"})

    def on_stop_action(self, event: EventBase) -> None:
        """Process the stop action."""
        if not self._preflight_checks():
            event.fail("Missing DB or S3 relations")
            return

        if not self._process_transition(DPBenchmarkLifecycleTransition.STOP):
            event.fail("Failed to stop the benchmark")
        event.set_results({"message": "Benchmark is stopped"})

    def on_clean_action(self, event: EventBase) -> None:
        """Process the clean action."""
        if not self._preflight_checks():
            event.fail("Missing DB or S3 relations")
            return

        if not self._process_transition(DPBenchmarkLifecycleTransition.CLEAN):
            event.fail("Failed to clean the benchmark")
        event.set_results({"message": "Benchmark is cleaned"})

    def _process_transition(self, transition: DPBenchmarkLifecycleTransition) -> bool:
        """Process the action."""
        if not (state := self.lifecycle.next(transition)):
            return False

        result = False
        if state == DPBenchmarkLifecycleState.PREPARING and self.config_manager.prepare():
            result = True
        elif state == DPBenchmarkLifecycleState.RUNNING and self.config_manager.run():
            result = True
        elif state == DPBenchmarkLifecycleState.STOPPED and self.config_manager.stop():
            result = True
        elif state == DPBenchmarkLifecycleState.UNSET and self.config_manager.clean():
            result = True

        if result:
            # Now, we update our state:
            self.lifecycle.update(state)
        return result

    ###########################################################################
    #
    #  Helpers
    #
    ###########################################################################

    def _unit_ip(self) -> str:
        """Current unit ip."""
        return self.model.get_binding(PEER_RELATION).network.bind_address
