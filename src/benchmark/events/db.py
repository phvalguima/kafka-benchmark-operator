# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module abstracts the different DBs and provide a single API set.

The DatabaseRelationHandler listens to DB events and manages the relation lifecycles.
The charm interacts with the manager and requests data + listen to some key events such
as changes in the configuration.
"""

import logging

from charms.data_platform_libs.v0.data_interfaces import DatabaseRequires
from ops.charm import CharmBase, CharmEvents
from ops.framework import EventBase, EventSource

from benchmark.events.handler import RelationHandler
from benchmark.literals import DPBenchmarkMissingOptionsError

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

    # @property
    # def username(self) -> str|None:
    #     """Returns the username to connect to the database."""
    #     return (self._secret_user or {}).get("username")

    # @property
    # def password(self) -> str|None:
    #     """Returns the password to connect to the database."""
    #     return (self._secret_user or {}).get("password")

    # @property
    # def tls(self) -> str|None:
    #     """Returns the TLS to connect to the database."""
    #     tls = (self._secret_tls or {}).get("tls")
    #     if not tls or tls == "disabled":
    #         return None
    #     return tls

    # @property
    # def tls_ca(self) -> str|None:
    #     """Returns the TLS CA to connect to the database."""
    #     tls_ca = (self._secret_user or {}).get("tls_ca")
    #     if not tls_ca or tls_ca == "disabled":
    #         return None
    #     return tls_ca

    # @property
    # def _secret_user(self) -> dict[str, str]|None:
    #     if not (secret_id := self.client.fetch_relation_data()[self.relation.id].get("secret-user")):
    #         return None
    #     return self.charm.framework.model.get_secret(id=secret_id).get_content()

    # @property
    # def _secret_tls(self) -> dict[str, str]|None:
    #     if not (secret_id := self.client.fetch_relation_data()[self.relation.id].get("secret-tls")):
    #         return None
    #     return self.charm.framework.model.get_secret(id=secret_id).get_content()

    def _on_endpoints_changed(self, event: EventBase) -> None:
        """Handles the endpoints_changed event."""
        try:
            if self.state.get():
                self.on.db_config_update.emit()
        except DPBenchmarkMissingOptionsError as e:
            logger.warning(f"Missing options: {e}")
            pass

    @property
    def client(self) -> DatabaseRequires:
        """Returns the data_interfaces client corresponding to the database."""
        ...
