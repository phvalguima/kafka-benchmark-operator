# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module abstracts the different DBs and provide a single API set.

The DatabaseRelationHandler listens to DB events and manages the relation lifecycles.
The charm interacts with the manager and requests data + listen to some key events such
as changes in the configuration.
"""

import logging
from typing import Any
from overrides import override

from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource

from benchmark.literals import DPBenchmarkMissingOptionsError
from benchmark.core.models import DatabaseState, RelationState
from benchmark.events.handler import RelationHandler

logger = logging.getLogger(__name__)


class DatabaseConfigUpdateNeededEvent(EventBase):
    """informs the charm that we have an update in the DB config."""


class DatabaseHandlerEvents(CharmEvents):
    """Events used by the Database Relation Manager to communicate with the charm."""

    db_config_update = EventSource(DatabaseConfigUpdateNeededEvent)


class DatabaseRelationHandler(RelationHandler):
    """Listens to all the DB-related events and react to them.

    This class will provide the charm with the necessary data to connect to the DB as
    well as the current relation status.
    """

    on = DatabaseHandlerEvents()  # pyright: ignore [reportGeneralTypeIssues]

    def __init__(
        self,
        charm: CharmBase,
        relation_name: str,
    ):
        super().__init__(charm, relation_name)
        self.database_key = "database"

        self.framework.observe(
            self.charm.on[self.relation_name].relation_joined,
            self._on_endpoints_changed,
        )
        self.framework.observe(
            self.charm.on[self.relation_name].relation_changed, self._on_endpoints_changed
        )
        self.framework.observe(
            self.charm.on[self.relation_name].relation_broken, self._on_endpoints_changed
        )

    @property
    @override
    def state(self) -> RelationState:
        """Returns the state of the database."""
        return DatabaseState(
            component=self.charm.app,
            relation=self.relation,
        )

    def _on_endpoints_changed(self, event: EventBase) -> None:
        """Handles the endpoints_changed event."""
        try:
            if self.state.get():
                self.on.db_config_update.emit()
        except DPBenchmarkMissingOptionsError as e:
            logger.warning(f"Missing options: {e}")
            pass

    @property
    def client(self) -> Any:
        """Returns the data_interfaces client corresponding to the database."""
        ...
