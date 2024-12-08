# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The peer event class."""

from ops.framework import Object
from ops.model import Unit

from abc import abstractmethod

from benchmark.core.models import PeerState
from benchmark.literals import Scope


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
        self.framework.observe(
            self.charm.on[self.relation_name].relation_changed,
            self._on_peer_changed,
        )

    @abstractmethod
    def workers(self) -> list[str]:
        """Return the peer workers."""
        ...

    def _on_peer_changed(self, _):
        """Handle the relation-changed event."""
        self.charm.lifecycle_update()

    def units(self) -> list[Unit]:
        """Return the peer units."""
        return self.relation.units

    def this_unit(self) -> Unit:
        """Return the current unit."""
        return self.relation.unit

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
