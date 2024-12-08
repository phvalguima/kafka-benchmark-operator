# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The workload lifecycle manager class.

The main goal is to return what is the next transition, based on the charm lifecycle.

Given the workload is pretty simple, we will coalesce both transition and state into a single:
DPBenchmarkWorkloadLifecycleState
"""

from benchmark.literals import (
    DPBenchmarkLifecycleState,
    DPBenchmarkWorkloadLifecycleState,
    DPBenchmarkWorkloadState,
)
from benchmark.managers.config import ConfigManager


class WorkloadLifecycleManager:
    """The workload lifecycle manager class."""

    def __init__(self, config: ConfigManager):
        self.config = config

    def current(self) -> DPBenchmarkWorkloadState:
        """Return the current lifecycle state."""
        return self.workload.state

    def next(
        self, charm_state: DPBenchmarkLifecycleState
    ) -> DPBenchmarkWorkloadLifecycleState | None:
        """Return the next lifecycle state based on the charm_state."""
        if charm_state == DPBenchmarkLifecycleState.STOPPED:
            return DPBenchmarkWorkloadLifecycleState.STOP

        if self.config.is_running():
            # We do not transition to any other state while executing
            return None

        if charm_state == DPBenchmarkLifecycleState.PREPARING:
            return DPBenchmarkWorkloadLifecycleState.PREPARE

        if charm_state == DPBenchmarkLifecycleState.RUNNING:
            return DPBenchmarkWorkloadLifecycleState.RUN

        return None
