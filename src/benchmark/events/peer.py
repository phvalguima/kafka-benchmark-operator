# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The peer event class."""

from abc import abstractmethod

from ops.framework import Object
from ops.model import Unit, WaitingStatus

from benchmark.core.models import PeerState
from benchmark.literals import (
    DPBenchmarkLifecycleState,
    DPBenchmarkLifecycleTransition,
    Scope,
)


class PeerRelationHandler(Object):
    """Listens to all the peer-related events and react to them.

    This class will provide the charm with the necessary data to connect to the peer as
    well as the current relation status.
    """

    def __init__(self, charm, relation_name):
        super().__init__(charm, None)
        self.charm = charm
        self.relation = self.charm.model.get_relation(relation_name)
        self.relation_name = relation_name
        self.state = PeerState(self.charm.unit, self.relation)
        self.framework.observe(
            self.charm.on[self.relation_name].relation_changed,
            self._on_peer_changed,
        )

        self.framework.observe(
            self.charm.on[self.relation_name].relation_joined,
            self._on_new_peer_unit,
        )
        self.framework.observe(
            self.charm.on[self.relation_name].relation_departed,
            self._on_new_peer_unit,
        )

    @abstractmethod
    def peers(self) -> list[str]:
        """Return the peers' IPs or any other relevant reference."""
        ...

    def _on_peer_changed(self, _):
        """Handle the relation-changed event."""
        if (next_state := self.charm.lifecycle.next(None)) and self.charm.lifecycle.current() != next_state:
            self.charm.lifecycle.make_transition(next_state)

    def _on_new_peer_unit(self, _):
        """Handle the relation-joined and relation-departed events."""
        # We have a new unit coming in. We need to stop the benchmark if running.
        if self.charm.lifecycle.current() not in [
            DPBenchmarkLifecycleState.UNSET,
            DPBenchmarkLifecycleState.PREPARING,
            DPBenchmarkLifecycleState.AVAILABLE,
        ]:
            if not (state := self.charm.lifecycle.next(DPBenchmarkLifecycleTransition.STOP)):
                return
            self.charm.lifecycle.make_transition(state)
            self.unit.status = WaitingStatus("Stopping the benchmark: peer unit count changed.")

    def units(self) -> list[Unit]:
        """Return the peer units."""
        return self.relation.units

    def this_unit(self) -> Unit:
        """Return the current unit."""
        return self.charm.unit

    def unit_state(self, unit: Unit) -> PeerState:
        """Return the unit data."""
        return PeerState(
            component=unit,
            relation=self.relation,
            scope=Scope.UNIT,
        )

    def app_state(self) -> PeerState:
        """Return the app data."""
        return PeerState(
            component=self.relation.app,
            relation=self.relation,
            scope=Scope.APP,
        )
