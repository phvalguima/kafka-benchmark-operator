# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module abstracts the different DBs and provide a single API set.

The DatabaseRelationManager listens to DB events and manages the relation lifecycles.
The charm interacts with the manager and requests data + listen to some key events such
as changes in the configuration.
"""

import logging
from typing import Any, Optional

from ops.charm import CharmEvents
from ops.framework import EventBase, EventSource
from ops.model import Application, Relation, Unit
from pydantic import BaseModel, root_validator

from benchmark.literals import (
    DPBenchmarkMissingOptionsError,
    Scope,
    Substrate,
)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


logger = logging.getLogger(__name__)


class DatabaseConfigUpdateNeededEvent(EventBase):
    """informs the charm that we have an update in the DB config."""


class DatabaseHandlerEvents(CharmEvents):
    """Events used by the Database Relation Manager to communicate with the charm."""

    db_config_update = EventSource(DatabaseConfigUpdateNeededEvent)


class DPBenchmarkExecutionExtraConfigsModel(BaseModel):
    """Holds all the details of the sysbench execution extra config.

    This model defines a basic conversion to a string of extra options to be considered.
    """

    extra_config: dict[str, Any] = {}

    def __str__(self):
        """Returns a string of extra options to be considered."""
        cfg = ""
        for key, val in self.extra_config.items():
            prefix = "--" if len(key) > 1 else "-"
            if val is None:
                cfg += f"{prefix}{key} "
            else:
                cfg += f"{prefix}{key}={val} "
        return cfg


class DPBenchmarkBaseDatabaseModel(BaseModel):
    """Benchmark database model.

    Holds all the details of the sysbench database.
    """

    hosts: Optional[list[str]]
    unix_socket: Optional[str]
    username: str
    password: str
    db_name: str

    @root_validator(pre=False, skip_on_failure=True)
    @classmethod
    def validate_if_missing_params(cls, field_values):
        """Validate if missing params."""
        missing_param = []
        # Check if the required fields are present
        for f in ["username", "password"]:
            if f not in field_values or field_values[f] is None:
                missing_param.append(f)
        if missing_param:
            raise DPBenchmarkMissingOptionsError(f"{missing_param}")

        # Check if we have the correct endpoint
        if not field_values.get("hosts") and not field_values.get("unix_socket"):
            raise DPBenchmarkMissingOptionsError("Missing endpoint as unix_socket OR host:port")
        return field_values


class DPBenchmarkExecutionModel(BaseModel):
    """Benchmark execution model.

    Holds all the details of the sysbench execution.
    """

    threads: int
    duration: int
    clients: int
    db_info: DPBenchmarkBaseDatabaseModel
    workload_name: str
    workload_params: dict[str, str]
    extra: DPBenchmarkExecutionExtraConfigsModel = DPBenchmarkExecutionExtraConfigsModel()


class RelationState:
    """Relation state object."""

    def __init__(
        self,
        component: Application | Unit,
        relation: Relation | None,
        substrate: Substrate | None = Substrate.VM,
        scope: Scope = Scope.UNIT,
    ):
        self.relation = relation
        self.substrate = substrate
        self.component = component
        self.scope = scope

    @property
    def relation_data(self) -> dict[str, str]:
        """Returns the relation data."""
        return self.relation.data[self.component]

    @property
    def remote_data(self) -> dict[str, str]:
        """Returns the remote relation data."""
        if self.scope == Scope.APP:
            return self.relation.data[self.relation.app]
        return self.relation.data[self.relation.unit]

    def __bool__(self) -> bool:
        """Boolean evaluation based on the existence of self.relation."""
        try:
            return bool(self.relation)
        except AttributeError:
            return False

    def get(self) -> Any:
        """Returns the value of the key."""
        ...

    def set(self, items: dict[str, str]) -> None:
        """Writes to relation_data."""
        delete_fields = [key for key in items if not items[key]]
        update_content = {k: items[k] for k in items if k not in delete_fields}

        self.relation_data.update(update_content)

        for field in delete_fields:
            del self.relation_data[field]


class PeerState(RelationState):
    """State collection for the database relation.

    The following items are managed by this state:
    * is_prepared: bool
      used to define if the database has been loaded with any warmup data.
      It must be true before starting the benchmark.
    """

    def __init__(self, component: Application | Unit, relation: Relation | None):
        super().__init__(
            relation=relation,
            component=component,
            scope=Scope.UNIT,
        )

    @property
    def is_prepared(self) -> bool:
        """Returns the value of the key."""
        return self.relation_data.get("is_prepared", "false") == "true"

    @is_prepared.setter
    def is_prepared(self, prepared: bool):
        """Returns the value of the key."""
        if prepared:
            self.set({"is_prepared": "true"})
        else:
            self.set({"is_prepared": None})


class DatabaseState(RelationState):
    """State collection for the database relation."""

    def __init__(self, component: Application | Unit, relation: Relation | None):
        self.database_key = "database"
        super().__init__(
            relation=relation,
            component=component,
            scope=Scope.APP,
        )

    def get(self) -> DPBenchmarkBaseDatabaseModel | None:
        """Returns the value of the key."""
        if not self.relation:
            return None
        endpoints = self.remote_data.get("endpoints")

        unix_socket = None
        if endpoints.startswith("file://"):
            unix_socket = endpoints[7:]

        return DPBenchmarkBaseDatabaseModel(
            hosts=endpoints.split(),
            unix_socket=unix_socket,
            username=self.remote_data.get("username"),
            password=self.remote_data.get("password"),
            db_name=self.remote_data.get(self.database_key),
        )
