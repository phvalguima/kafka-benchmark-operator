# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""The peer event class."""
from typing import Any

from ops.framework import Object
from ops.model import Unit
from benchmark.core.models import PeerState


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

    def _on_peer_changed(self, _):
        """Handle the relation-changed event."""
        self.charm.lifecycle_update()

    def units(self) -> list[Unit]:
        """Return the peer unit."""
        return self.relation.units

    def this_unit(self) -> Unit:
        """Return the current unit."""
        return self.charm.unit

    def unit_state(self, unit: Unit) -> PeerState:
        """Return the unit data."""
        return self.relation.data[unit]

    def app_state(self) -> PeerState:
        """Return the app data."""
        return self.relation.data[self.charm.app]