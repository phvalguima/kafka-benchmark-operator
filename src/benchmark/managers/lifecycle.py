# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The lifecycle manager class."""

from benchmark.core.workload_base import WorkloadBase
from benchmark.events.peer import PeerRelationHandler
from benchmark.literals import (
    DPBenchmarkLifecycleState,
    LIFECYCLE_KEY,
)

class LifecycleManager:

    def __init__(self, peers: PeerRelationHandler, workload: WorkloadBase):
        self.peers = peers
        self.workload = workload

    def current(self) -> DPBenchmarkLifecycleState:
        """Return the current lifecycle state."""
        return (
            self.peers.unit_state(self.peers.this_unit()).lifecycle
            or DPBenchmarkLifecycleState.UNSET
        )

    def next(self) -> DPBenchmarkLifecycleState|None:
        """Return the next lifecycle state after checking each neighbor."""
        next_state = (
            self.peers.unit_state(self.peers.this_unit()).lifecycle
            or DPBenchmarkLifecycleState.UNSET
        )
        for unit in self.peers.units():
            neighbor = self.peers.unit_state(unit).lifecycle
            if neighbor is None:
                continue
            elif self._compare_lifecycle_states(neighbor, next_state):
                next_state = neighbor

    def _compare_lifecycle_states(
        self, neighbor: DPBenchmarkLifecycleState, this: DPBenchmarkLifecycleState
    ) -> int:
        """Compare the lifecycle, if the unit A is more advanced than unit B or vice-versa.

        neighbor - this: if values return greater than 0, then return greatest neighbor state
        else: return None (no changes should be considered)
        """
        if neighbor == this:
            return 0

        def _get_value(phase: DPBenchmarkLifecycleState) -> int:
            if phase == DPBenchmarkLifecycleState.UNSET:
                return 0
            if phase == DPBenchmarkLifecycleState.PREPARING:
                return 0
            if phase == DPBenchmarkLifecycleState.AVAILABLE:
                return 2
            if phase == DPBenchmarkLifecycleState.RUNNING:
                return 3
            if phase == DPBenchmarkLifecycleState.FAILED:
                return 4
            if phase == DPBenchmarkLifecycleState.COLLECTING:
                return 0
            if phase == DPBenchmarkLifecycleState.UPLOADING:
                return 0
            if phase == DPBenchmarkLifecycleState.FINISHED:
                return 0
            if phase == DPBenchmarkLifecycleState.STOPPED:
                return 5

        return _get_value(neighbor) - _get_value(this)