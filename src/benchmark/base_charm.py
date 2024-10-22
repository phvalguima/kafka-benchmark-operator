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
import os
from typing import Dict, List

import ops
from charms.grafana_agent.v0.cos_agent import COSAgentProvider
from charms.operator_libs_linux.v0 import apt
from ops.framework import EventBase
from ops.model import ActiveStatus, BlockedStatus, MaintenanceStatus, WaitingStatus

from benchmark.benchmark_workload_base import DPBenchmarkSystemdService
from benchmark.events.db import DatabaseRelationHandler
from benchmark.literals import (
    COS_AGENT_RELATION,
    METRICS_PORT,
    PEER_RELATION,
    DPBenchmarkError,
    DPBenchmarkExecError,
    DPBenchmarkMissingOptionsError,
    DPBenchmarkServiceError,
    DPBenchmarkUnitNotReadyError,
)
from benchmark.managers.config import ConfigManager

# Log messages can be retrieved using juju debug-log
logger = logging.getLogger(__name__)


class DPBenchmarkCharmBase(ops.CharmBase):
    """The base benchmark class."""

    def __init__(self, *args, db_relation_name: str):
        super().__init__(*args)
        self.framework.observe(self.on.install, self._on_install)
        self.framework.observe(self.on.config_changed, self._on_config_changed)
        self.framework.observe(self.on.prepare_action, self.on_prepare_action)
        self.framework.observe(self.on.run_action, self.on_run_action)
        self.framework.observe(self.on.stop_action, self.on_stop_action)
        self.framework.observe(self.on.clean_action, self.on_clean_action)
        self.framework.observe(self.on.update_status, self._on_update_status)
        self.framework.observe(self.on.list_workloads_action, self.on_list_workloads_action)

        self.database = DatabaseRelationHandler(self, db_relation_name)
        self.framework.observe(self.database.on.db_config_update, self._on_config_changed)

        self._grafana_agent = COSAgentProvider(
            self,
            relation_name=COS_AGENT_RELATION,
            metrics_endpoints=[],
            refresh_events=[
                self.on.config_changed,
            ],
            scrape_configs=self.scrape_config,
        )

        self.workload = DPBenchmarkSystemdService()
        self.config_manager = ConfigManager(
            workload=self.workload,
            database=self.database.state,
            config=self.config,
        )

        self.labels = f"{self.model.name},{self.unit.name}"

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
        if self.workload.is_failed():
            self.unit.status = BlockedStatus("Benchmark failed, please check logs")
        elif self.workload.is_running():
            self.unit.status = ActiveStatus("Benchmark is running")
        elif self.workload.is_prepared() and self.peer_state.is_prepared():
            self.unit.status = WaitingStatus("Benchmark is prepared: execute run to start")
        elif self.workload.is_stopped():
            self.unit.status = BlockedStatus("Benchmark is stopped after run")
        else:
            self.unit.status = ActiveStatus()

    def _unit_ip(self) -> str:
        """Current unit ip."""
        return self.model.get_binding(PEER_RELATION).network.bind_address

    def _on_config_changed(self, event: EventBase) -> None:
        """Config changed event."""
        try:
            # First, we check if the status of the service
            if not self.database.state.get():
                logger.debug("The benchmark is not ready")
                return
            if not self.stop():
                logger.warning("Config changed: tried stopping the service but returned False")
                raise DPBenchmarkError()
            if not self.clean_up():
                logger.info("Config changed: tried cleaning up the service but returned False")
                raise DPBenchmarkError()
            if not self.prepare():
                logger.info("Config changed: tried preparing the service but returned False")
                raise DPBenchmarkError()

            # We must set the file, as we were not UNSET:
            if self.workload.state.is_running() or self.workload.state.is_error():
                self.run()
        except DPBenchmarkError:
            event.defer()

        self._set_status()

    def _on_relation_broken(self, event: EventBase) -> None:
        self.stop()
        self.clean_up()

    def scrape_config(self) -> List[Dict]:
        """Generate scrape config for the Patroni metrics endpoint."""
        return [
            {
                "metrics_path": "/metrics",
                "static_configs": [{"targets": [f"{self._unit_ip()}:{METRICS_PORT}"]}],
                "tls_config": {"insecure_skip_verify": True},
                "scheme": "http",
            }
        ]

    def _install_packages(self, extra_packages: List[str] = []) -> None:
        """Install the packages needed for the benchmark."""
        self.unit.status = MaintenanceStatus("Installing apt packages...")
        apt.update()
        apt.add_package(extra_packages)
        self.unit.status = ActiveStatus()

    def _on_install(self, event: EventBase) -> None:
        """Installs the basic packages and python dependencies.

        No exceptions are captured as we need all the dependencies below to even start running.
        """
        self.unit.status = MaintenanceStatus("Installing...")
        self.config_manager.render_service_executable()
        self.unit.status = ActiveStatus()

    def on_list_workloads_action(self, event: EventBase) -> None:
        """Lists all possible workloads."""
        event.set_results({"workloads": self.supported_workloads()})

    def on_prepare_action(self, event: EventBase) -> None:
        """Prepare the database.

        There are two steps: the actual prepare command and setting a target to inform the
        prepare was successful.
        """
        if not self.unit.is_leader():
            event.fail("Failed: only leader can prepare the database")
            return
        if self.workload.is_prepared():
            event.fail("Benchmark is already prepared, stop and clean up the cluster first")
            return

        self.unit.status = MaintenanceStatus("Running prepare command...")
        try:
            self.prepare()
        except DPBenchmarkExecError:
            event.fail("Failed: error in benchmark while executing prepare")
        except DPBenchmarkMissingOptionsError as e:
            event.fail(f"Failed: missing database options {e}")
        else:
            event.set_results({"status": "prepared"})
        self._set_status()

    def prepare(self) -> None:
        """Prepares the database and sets the state."""
        if self.unit.is_leader():
            self.workload.exec("prepare", self.labels)

        if not self.config_manager.prepare(
            db=self.config_manager.get_execution_options(),
            workload_name=self.config["workload_name"],
            labels=self.labels,
        ):
            raise DPBenchmarkExecError()

    def on_run_action(self, event: EventBase) -> None:
        """Run benchmark action."""
        try:
            self.run()
            event.set_results({"status": "running"})
        except DPBenchmarkUnitNotReadyError:
            event.fail("Failed: benchmark is not ready.")
            return
        except DPBenchmarkServiceError as e:
            event.fail(f"Failed: error in benchmark service {e}")
            return
        self._set_status()

    def run(self) -> None:
        """Run the benchmark service."""
        if not self.workload.is_prepared():
            raise DPBenchmarkUnitNotReadyError()
        self.workload.restart()

    def on_stop_action(self, event: EventBase) -> None:
        """Stop benchmark service."""
        try:
            if not self.workload.is_running():
                self.stop()
                event.set_results({"status": "stopped"})
        except (DPBenchmarkUnitNotReadyError, DPBenchmarkServiceError) as e:
            event.fail(f"Failed: error in benchmark service {e}")
            return
        self._set_status()

    def stop(self) -> None:
        """Stop the benchmark service. Returns true if stop was successful."""
        if not self.is_running():
            raise DPBenchmarkUnitNotReadyError()
        self.workload.stop()

    def on_clean_action(self, event: EventBase) -> None:
        """Clean the database."""
        try:
            if not self.clean_up():
                event.fail("Failed: error in benchmark while executing clean")
                return
        except DPBenchmarkMissingOptionsError as e:
            event.fail(f"Failed: missing database options {e}")
            return
        except DPBenchmarkExecError as e:
            event.fail(f"Failed: error in benchmark while executing clean {e}")
            return
        except DPBenchmarkServiceError as e:
            event.fail(f"Failed: error in benchmark service {e}")
            return
        self._set_status()

    def clean_up(self) -> bool:
        """Clean up the database and the unit.

        We recheck the service status, as we do notw ant to make any distinctions between the different steps.
        """
        if self.workload.is_running():
            self.workload.stop()

        if self.unit.is_leader():
            self.workload.exec("clean", self.labels)
        self.config_manager.unset()
        return True

    def supported_workloads(self) -> list[str]:
        """List the supported workloads."""
        return [
            ".".join(name.split(".")[:-2])
            for name in os.listdir("src/workload_parameter_templates")
        ]
