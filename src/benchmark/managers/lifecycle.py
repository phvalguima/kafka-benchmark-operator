# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The lifecycle manager class."""

from ops.model import (
    ActiveStatus,
    BlockedStatus,
    MaintenanceStatus,
    StatusBase,
    WaitingStatus,
)

from benchmark.events.peer import PeerRelationHandler
from benchmark.literals import (
    DPBenchmarkLifecycleState,
    DPBenchmarkLifecycleTransition,
)
from benchmark.managers.config import ConfigManager


class LifecycleManager:
    """The lifecycle manager class."""

    def __init__(self, peers: PeerRelationHandler, config_manager: ConfigManager):
        self.peers = peers
        self.config_manager = config_manager

    def current(self) -> DPBenchmarkLifecycleState:
        """Return the current lifecycle state."""
        return (
            self.peers.unit_state(self.peers.this_unit()).lifecycle
            or DPBenchmarkLifecycleState.UNSET
        )

    def make_transition(self, new_state: DPBenchmarkLifecycleState) -> bool:  # noqa: C901
        """Update the lifecycle state.

        The main task is to do the update status. The first batch of if statements are
        checks if we cannot do so. In any of these ifs, we return False.

        Once these steps are done, we update the status and return True.
        """
        if new_state == DPBenchmarkLifecycleState.UNSET:
            # We stop any service right away
            if not self.config_manager.is_stopped() or not self.config_manager.stop():
                return False
            # And clean up the workload
            if not self.config_manager.is_cleaned() or not self.config_manager.clean():
                return False

        # The transition "PREPARING" is a special case:
        # Only one unit executes it and the others way.
        # Therefore, the PREPARING state must be processed at "PREPARE" call
        # if new_state == DPBenchmarkLifecycleState.PREPARING:
        #     # Prepare the workload
        #     if not self.config_manager.prepare():
        #         return False

        if new_state == DPBenchmarkLifecycleState.AVAILABLE:
            # workload should be prepared, check we are stopped or stop it
            if not self.config_manager.is_stopped() and not self.config_manager.stop():
                return False

        if new_state == DPBenchmarkLifecycleState.RUNNING:
            # Start the workload
            if not self.config_manager.is_running() or not self.config_manager.run():
                return False

        # TODO: Implement the following states
        # if new_state == DPBenchmarkLifecycleState.COLLECTING:
        #     # Collect the workload data
        #     if not self.config_manager.is_collecting() or not self.config_manager.collect():
        #         return False
        # if new_state == DPBenchmarkLifecycleState.UPLOADING:
        #     # Collect the workload data
        #     if not self.config_manager.is_uploading() or not self.config_manager.upload():
        #         return False

        if new_state == DPBenchmarkLifecycleState.FAILED:
            # Stop the workload
            if (
                not self.config_manager.is_failed()
                or not self.config_manager.is_stopped()
                or not self.config_manager.stop()
            ):
                return False

        if new_state in [
            DPBenchmarkLifecycleState.FINISHED,
            DPBenchmarkLifecycleState.STOPPED,
        ]:
            # Stop the workload
            if not self.config_manager.is_stopped() or not self.config_manager.stop():
                return False

        self.peers.unit_state(self.peers.this_unit()).lifecycle = new_state.value
        return True

    def next(  # noqa: C901
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> DPBenchmarkLifecycleState | None:
        """Return the next lifecycle state."""
        # Changes that takes us to UNSET:
        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            # Simplest case, we return to unset
            return DPBenchmarkLifecycleState.UNSET

        # Changes that takes us to STOPPED:
        # Either we received a stop transition
        if transition == DPBenchmarkLifecycleTransition.STOP:
            return DPBenchmarkLifecycleState.STOPPED
        # OR one of our peers is in stopped state
        if (
            self._compare_lifecycle_states(
                self._peers_state(),
                DPBenchmarkLifecycleState.STOPPED,
            )
            == 0
        ):
            return DPBenchmarkLifecycleState.STOPPED

        # FAILED takes precedence over all other states
        # Changes that takes us to FAILED:
        # Workload has failed and we were:
        # - PREPARING
        # - RUNNING
        # - COLLECTING
        # - UPLOADING
        if (
            self.current()
            in [
                DPBenchmarkLifecycleState.PREPARING,
                DPBenchmarkLifecycleState.RUNNING,
                DPBenchmarkLifecycleState.COLLECTING,
                DPBenchmarkLifecycleState.UPLOADING,
            ]
            and self.config_manager.workload.is_failed()
        ):
            return DPBenchmarkLifecycleState.FAILED

        # Changes that takes us to PREPARING:
        # We received a prepare signal and no one else is available yet or we failed previously
        if transition == DPBenchmarkLifecycleTransition.PREPARE and self._peers_state() in [
            DPBenchmarkLifecycleState.UNSET,
            DPBenchmarkLifecycleState.FAILED,
        ]:
            return DPBenchmarkLifecycleState.PREPARING
        elif transition == DPBenchmarkLifecycleTransition.PREPARE:
            # Failed to calculate a proper state as we have neighbors in more advanced state for now
            return None

        # Changes that takes us to AVAILABLE:
        # Either we were in preparing and we are finished
        if (
            self.current() == DPBenchmarkLifecycleState.PREPARING
            and self.config_manager.is_prepared()
        ):
            return DPBenchmarkLifecycleState.AVAILABLE
        # OR highest peers state is AVAILABLE but no actions has happened
        if (
            transition is None
            and self._compare_lifecycle_states(
                self._peers_state(),
                DPBenchmarkLifecycleState.AVAILABLE,
            )
            == 0
        ):
            return DPBenchmarkLifecycleState.AVAILABLE

        # Changes that takes us to RUNNING:
        # Either we receive a transition to running and we were in one of:
        # - AVAILABLE
        # - FAILED
        # - STOPPED
        # - FINISHED
        if transition == DPBenchmarkLifecycleTransition.RUN and self.current() in [
            DPBenchmarkLifecycleState.AVAILABLE,
            DPBenchmarkLifecycleState.FAILED,
            DPBenchmarkLifecycleState.STOPPED,
            DPBenchmarkLifecycleState.FINISHED,
        ]:
            return DPBenchmarkLifecycleState.RUNNING
        # OR any other peer is beyond the >=RUNNING state
        # and we are still AVAILABLE.
        if self._compare_lifecycle_states(
            self._peers_state(),
            DPBenchmarkLifecycleState.RUNNING,
        ) == 0 and self.current() in [
            DPBenchmarkLifecycleState.UNSET,
            DPBenchmarkLifecycleState.AVAILABLE,
        ]:
            return DPBenchmarkLifecycleState.RUNNING

        # Changes that takes us to COLLECTING:
        # the workload is in collecting state
        if self.config_manager.is_collecting():
            return DPBenchmarkLifecycleState.COLLECTING

        # Changes that takes us to UPLOADING:
        # the workload is in uploading state
        if self.config_manager.is_uploading():
            return DPBenchmarkLifecycleState.UPLOADING

        # Changes that takes us to FINISHED:
        # Workload has finished and we were in one of:
        # - RUNNING
        # - UPLOADING
        if (
            self.current()
            in [
                DPBenchmarkLifecycleState.RUNNING,
                DPBenchmarkLifecycleState.UPLOADING,
            ]
            and self.config_manager.workload.is_halted()
        ):
            return DPBenchmarkLifecycleState.FINISHED

        # We are in an incongruent state OR the transition does not make sense
        return None

    def _peers_state(self) -> DPBenchmarkLifecycleState | None:
        next_state = self.peers.unit_state(self.peers.this_unit()).lifecycle
        for unit in self.peers.units():
            neighbor = self.peers.unit_state(unit).lifecycle
            if neighbor is None:
                continue
            elif self._compare_lifecycle_states(neighbor, next_state) > 0:
                next_state = neighbor
        return next_state or DPBenchmarkLifecycleState.UNSET

    @property
    def status(self) -> StatusBase:
        """Return the status of the benchmark."""
        if self.current() == DPBenchmarkLifecycleState.UNSET:
            return WaitingStatus("Benchmark is unset")

        if self.current() == DPBenchmarkLifecycleState.PREPARING:
            return MaintenanceStatus("Preparing the benchmark")

        if self.current() == DPBenchmarkLifecycleState.AVAILABLE:
            return WaitingStatus("Benchmark prepared: call run to start")

        if self.current() == DPBenchmarkLifecycleState.RUNNING:
            return ActiveStatus("Benchmark is running")

        if self.current() == DPBenchmarkLifecycleState.FAILED:
            return BlockedStatus("Benchmark failed execution")

        if self.current() == DPBenchmarkLifecycleState.COLLECTING:
            return ActiveStatus("Benchmark is collecting data")

        if self.current() == DPBenchmarkLifecycleState.UPLOADING:
            return ActiveStatus("Benchmark is uploading data")

        if self.current() == DPBenchmarkLifecycleState.FINISHED:
            return ActiveStatus("Benchmark finished")

        # if self.current() == DPBenchmarkLifecycleState.STOPPED:
        return WaitingStatus("Benchmark is stopped")

    def _compare_lifecycle_states(  # noqa: C901
        self, neighbor: DPBenchmarkLifecycleState, this: DPBenchmarkLifecycleState
    ) -> int:
        """Compare the lifecycle, if the unit A is more advanced than unit B or vice-versa.

        neighbor - this: if values return greater than 0, then return greatest neighbor state
        else: return None (no changes should be considered)
        """
        if neighbor == this:
            return 0

        def _get_value(phase: DPBenchmarkLifecycleState) -> int:  # noqa: C901
            if phase == DPBenchmarkLifecycleState.UNSET:
                return 0
            if phase == DPBenchmarkLifecycleState.PREPARING:
                return 1
            if phase == DPBenchmarkLifecycleState.AVAILABLE:
                return 2
            if phase == DPBenchmarkLifecycleState.RUNNING:
                return 3
            if phase == DPBenchmarkLifecycleState.FAILED:
                return 4
            if phase == DPBenchmarkLifecycleState.COLLECTING:
                return 5
            if phase == DPBenchmarkLifecycleState.UPLOADING:
                return 6
            if phase == DPBenchmarkLifecycleState.FINISHED:
                return 7
            if phase == DPBenchmarkLifecycleState.STOPPED:
                return 8

        return _get_value(neighbor) - _get_value(this)
