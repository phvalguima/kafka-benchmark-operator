# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module abstracts the different DBs and provide a single API set.

The DatabaseRelationManager listens to DB events and manages the relation lifecycles.
The charm interacts with the manager and requests data + listen to some key events such
as changes in the configuration.
"""

import logging
from typing import Any, Optional

from ops.model import Application, Relation, Unit
from pydantic import BaseModel, error_wrappers, root_validator

from benchmark.literals import (
    DPBenchmarkLifecyclePhase,
    DPBenchmarkMissingOptionsError,
    Scope,
    Substrate,
)

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


logger = logging.getLogger(__name__)


class SosreportCLIArgsModel(BaseModel):
    """Holds the data specific to connect with the target application."""

    plugins: Optional[list[str]] = []
    plugin_options: Optional[list[str]] = []
    batch: bool = True
    clean: bool = True
    tmp_dir: Optional[str] = "/tmp"
    pack: bool = True

    def __str__(self):
        """Return the string representation of the model."""
        result = ""
        if self.plugins:
            result += (
                "--only-plugins "
                + ",".join(x for x in self.plugins)
                + " --enable-plugins "
                + ",".join(x for x in self.plugins)
            )
        if self.plugin_options:
            result += " ".join([f"-k {opt}" for opt in self.plugin_options])
        if self.batch:
            result += " --batch"
        if self.clean:
            result += " --clean"
        if self.tmp_dir:
            result += f" --tmp-dir {self.tmp_dir}"
        if self.pack:
            result += " -z gzip"
        return result


class DPBenchmarkBaseDatabaseModel(BaseModel):
    """Holds the data specific to connect with the target application."""

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


class DPBenchmarkConfigModelBase(BaseModel):
    """Benchmark config model contains all the information needed to render the workload.

    This model is used to render the workload config files.

    This class is abstract as each benchmark charm must inherit from it and provide the
    necessary implementation.
    """

    workload_profile: str = "default"


class DPBenchmarkWrapperModel(BaseModel):
    """Benchmark execution model.

    This class contains all the config info needed to pass to the benchmark tool wrapper,
    that will be managing the workload.
    """

    test_name: str
    parallel_processes: int
    threads: int
    duration: int
    run_count: int
    db_info: DPBenchmarkBaseDatabaseModel
    workload_name: str
    report_interval: int


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
    """State collection for the database relation."""

    LIFECYCLE_KEY = "lifecycle"
    STOP_KEY = "stop"

    def __init__(self, component: Application | Unit, relation: Relation | None):
        super().__init__(
            relation=relation,
            component=component,
            scope=Scope.UNIT,
        )

    def get(self) -> Any:
        """Returns the value of the key."""
        return self.relation_data.get(
            self.LIFECYCLE_KEY,
            None,
        )

    @property
    def lifecycle(self) -> DPBenchmarkLifecyclePhase | None:
        """Returns the value of the lifecycle key."""
        return self.get(self.LIFECYCLE_KEY)

    @lifecycle.setter
    def lifecycle(self, status: DPBenchmarkLifecyclePhase):
        """Sets the lifecycle key value."""
        self.set({self.LIFECYCLE_KEY: status})

    @property
    def stop(self) -> bool:
        """Returns the value of the stop key."""
        return self.relation_data.get(self.STOP_KEY, False)

    @stop.setter
    def stop(self, switch: bool) -> bool:
        """Toggles the stop key value."""
        self.set({self.STOP_KEY: switch})


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
        if not self.relation or not (endpoints := self.remote_data.get("endpoints")):
            return None

        unix_socket = None
        if endpoints.startswith("file://"):
            unix_socket = endpoints[7:]
        try:
            return DPBenchmarkBaseDatabaseModel(
                hosts=endpoints.split(),
                unix_socket=unix_socket,
                username=self.remote_data.get("username"),
                password=self.remote_data.get("password"),
                db_name=self.remote_data.get(self.database_key),
            )
        except error_wrappers.ValidationError as e:
            logger.warning(f"Failed to validate the database model: {e}")
            entries = [entry.get("loc")[0] for entry in e.errors()]
            raise DPBenchmarkMissingOptionsError(f"{entries}")
        return None
