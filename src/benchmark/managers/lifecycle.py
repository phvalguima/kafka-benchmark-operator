# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The lifecycle manager class."""

from abc import ABC, abstractmethod
from typing import Optional

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
            if not self.config_manager.is_stopped() and not self.config_manager.stop():
                return False
            # And clean up the workload
            if not self.config_manager.is_cleaned() and not self.config_manager.clean():
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
            if not self.config_manager.is_running() and not self.config_manager.run():
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
                and not self.config_manager.is_stopped()
                and not self.config_manager.stop()
            ):
                return False

        if new_state in [
            DPBenchmarkLifecycleState.FINISHED,
            DPBenchmarkLifecycleState.STOPPED,
        ]:
            # Stop the workload
            if not self.config_manager.is_stopped() and not self.config_manager.stop():
                return False

        self.peers.unit_state(self.peers.this_unit()).lifecycle = new_state.value
        return True

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> DPBenchmarkLifecycleState | None:
        """Return the next lifecycle state."""
        lifecycle_state = _LifecycleStateFactory().build(
            self,
            self.current(),
        )
        result = lifecycle_state.next(transition)
        return result.state if result else None

    def _peers_state(self) -> DPBenchmarkLifecycleState | None:
        next_state = self.peers.unit_state(self.peers.this_unit()).lifecycle
        for unit in self.peers.units():
            neighbor = self.peers.unit_state(unit).lifecycle
            if neighbor is None:
                continue
            elif self._compare_lifecycle_states(neighbor, next_state) > 0:
                next_state = neighbor
        return next_state or DPBenchmarkLifecycleState.UNSET

    def check_all_peers_in_state(self, state: DPBenchmarkLifecycleState) -> bool:
        """Check if the unit can run the workload.

        That happens if all the peers are set as state value.
        """
        for unit in self.peers.units():
            if state != self.peers.unit_state(unit).lifecycle:
                return False
        return True

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


class _LifecycleState(ABC):
    """The lifecycle state represents a single state and encapsulates the transition logic."""

    state: DPBenchmarkLifecycleState

    def __init__(self, manager: LifecycleManager):
        self.manager = manager

    from typing import Optional, Union

    @abstractmethod
    def next(
        self, transition: Optional[DPBenchmarkLifecycleTransition] = None
    ) -> Optional["_LifecycleState"]: ...


class _StoppedLifecycleState(_LifecycleState):
    """The stopped lifecycle state."""

    state = DPBenchmarkLifecycleState.STOPPED

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if self.manager.peers.test_name is None:
            # We have to roll back to UNSET
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            return _UnsetLifecycleState(self.manager)

        if self.manager.config_manager.is_running():
            return _RunningLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.RUN:
            return _RunningLifecycleState(self.manager)

        if self.manager.config_manager.is_failed():
            return _FailedLifecycleState(self.manager)

        return None


class _FailedLifecycleState(_LifecycleState):
    """The failed lifecycle state."""

    state = DPBenchmarkLifecycleState.FAILED

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if self.manager.peers.test_name is None:
            # We have to roll back to UNSET
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            return _UnsetLifecycleState(self.manager)

        if self.manager.config_manager.is_running():
            return _RunningLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.RUN:
            return _RunningLifecycleState(self.manager)

        return None


class _FinishedLifecycleState(_LifecycleState):
    """The finished lifecycle state."""

    state = DPBenchmarkLifecycleState.FINISHED

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if self.manager.peers.test_name is None:
            # We have to roll back to UNSET
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.STOP:
            return _StoppedLifecycleState(self.manager)

        if self.manager.config_manager.is_running():
            return _RunningLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.RUN:
            return _RunningLifecycleState(self.manager)

        if self.manager.config_manager.is_failed():
            return _FailedLifecycleState(self.manager)

        return None


class _RunningLifecycleState(_LifecycleState):
    """The running lifecycle state."""

    state = DPBenchmarkLifecycleState.RUNNING

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if self.manager.peers.test_name is None:
            # We have to roll back to UNSET
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.STOP:
            return _StoppedLifecycleState(self.manager)

        if (
            self.manager._compare_lifecycle_states(
                self.manager._peers_state(),
                DPBenchmarkLifecycleState.STOPPED,
            )
            == 0
        ):
            return _StoppedLifecycleState(self.manager)

        if self.manager.config_manager.is_failed():
            return _FailedLifecycleState(self.manager)

        if not self.manager.config_manager.is_running():
            # TODO: Collect state should be implemented here instead
            return _FinishedLifecycleState(self.manager)

        return None


class _AvailableLifecycleState(_LifecycleState):
    """The available lifecycle state."""

    state = DPBenchmarkLifecycleState.AVAILABLE

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if self.manager.peers.test_name is None:
            # We have to roll back to UNSET
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.RUN:
            return _RunningLifecycleState(self.manager)

        if (
            self.manager._compare_lifecycle_states(
                self.manager._peers_state(),
                DPBenchmarkLifecycleState.RUNNING,
            )
            == 0
        ):
            return _RunningLifecycleState(self.manager)

        return None


class _PreparingLifecycleState(_LifecycleState):
    """The preparing lifecycle state."""

    state = DPBenchmarkLifecycleState.PREPARING

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if self.manager.peers.test_name is None:
            # We have to roll back to UNSET
            return _UnsetLifecycleState(self.manager)

        if transition == DPBenchmarkLifecycleTransition.CLEAN:
            return _UnsetLifecycleState(self.manager)

        if self.manager.config_manager.is_failed():
            return _FailedLifecycleState(self.manager)

        if self.manager.config_manager.is_prepared():
            return _AvailableLifecycleState(self.manager)

        return None


class _UnsetLifecycleState(_LifecycleState):
    """The unset lifecycle state."""

    state = DPBenchmarkLifecycleState.UNSET

    def next(
        self, transition: DPBenchmarkLifecycleTransition | None = None
    ) -> Optional["_LifecycleState"]:
        if transition == DPBenchmarkLifecycleTransition.PREPARE:
            return _PreparingLifecycleState(self.manager)

        if (
            self.manager._compare_lifecycle_states(
                self.manager._peers_state(),
                DPBenchmarkLifecycleState.AVAILABLE,
            )
            == 0
        ):
            return _AvailableLifecycleState(self.manager)

        if (
            self.manager._compare_lifecycle_states(
                self.manager._peers_state(),
                DPBenchmarkLifecycleState.RUNNING,
            )
            == 0
        ):
            return _RunningLifecycleState(self.manager)

        return None


class _LifecycleStateFactory:
    """The lifecycle state factory."""

    def build(
        self, manager: LifecycleManager, state: DPBenchmarkLifecycleState
    ) -> _LifecycleState:
        """Build the lifecycle state."""
        if state == DPBenchmarkLifecycleState.UNSET:
            return _UnsetLifecycleState(manager)

        if state == DPBenchmarkLifecycleState.PREPARING:
            return _PreparingLifecycleState(manager)

        if state == DPBenchmarkLifecycleState.AVAILABLE:
            return _AvailableLifecycleState(manager)

        if state == DPBenchmarkLifecycleState.RUNNING:
            return _RunningLifecycleState(manager)

        if state == DPBenchmarkLifecycleState.FAILED:
            return _FailedLifecycleState(manager)

        if state == DPBenchmarkLifecycleState.FINISHED:
            return _FinishedLifecycleState(manager)

        if state == DPBenchmarkLifecycleState.STOPPED:
            return _StoppedLifecycleState(manager)

        raise ValueError("Unknown state")
