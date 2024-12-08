# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module represents the relation handler class."""

import logging
from abc import abstractmethod

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource, Object

from benchmark.core.models import RelationState

logger = logging.getLogger(__name__)


class RelationHandler(Object):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """
    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
    ):
        super().__init__(charm, None)
        self.charm = charm
        self.relation = self.charm.model.get_relation(relation_name)
        self.relation_name = relation_name

    @property
    @abstractmethod
    def state(self) -> RelationState:
        """Returns the state of the database."""
        ...
