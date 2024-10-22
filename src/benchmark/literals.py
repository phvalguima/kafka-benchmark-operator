# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

from enum import Enum

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


class Substrate(Enum):
    """Substrate of the benchmark."""

    VM = "vm"
    K8S = "k8s"


class Scope(Enum):
    """Scope of the benchmark."""

    UNIT = "unit"
    APP = "app"


METRICS_PORT = 8088
COS_AGENT_RELATION = "cos-agent"
PEER_RELATION = "benchmark-peer"


class DPBenchmarkError(Exception):
    """Benchmark error."""


class DPBenchmarkUnitNotReadyError(DPBenchmarkError):
    """Unit not ready error."""


class DPBenchmarkStatusError(DPBenchmarkError):
    """Unit not ready error."""

    def __init__(self, status: str):
        self.status = status
        super().__init__(f"Status: {status}")


class DPBenchmarkExecError(DPBenchmarkError):
    """Sysbench failed to execute a command."""


class DPBenchmarkServiceError(DPBenchmarkExecError):
    """Sysbench service error."""


class DPBenchmarkMultipleRelationsToDBError(DPBenchmarkError):
    """Multiple relations to the same or multiple DBs exist."""


class DPBenchmarkExecFailedError(DPBenchmarkError):
    """Sysbench execution failed error."""


class DPBenchmarkMissingOptionsError(DPBenchmarkError):
    """Sysbench missing options error."""


class DPBenchmarkDBRelationNotAvailableError(DPBenchmarkError):
    """Sysbench failed to execute a command."""


class DatabaseRelationState(Enum):
    """Represents the different status of the database relation.

    The ERROR in this case corresponds to the case, for example, more than one
    relation exists for a given DB, or for multiple DBs.
    """

    NOT_AVAILABLE = "not_available"
    AVAILABLE = "available"
    CONFIGURED = "configured"
    ERROR = "error"


class BenchmarkServiceState(Enum):
    """Benchmark service status representation.

    The status are:
    * NOT_PRESENT: the service is not present
    * AVAILABLE: the service is present and ready to be started
    * RUNNING: the service is running
    * FINISHED: the service has finished
    * FAILED: the service has failed
    """

    NOT_PRESENT = "not_present"
    AVAILABLE = "available"
    RUNNING = "running"
    FINISHED = "finished"
    FAILED = "failed"


class DatabaseRelationStatus(Enum):
    """Represents the different status of the database relation.

    The ERROR in this case corresponds to the case, for example, more than one
    relation exists for a given DB, or for multiple DBs.
    """

    NOT_AVAILABLE = "not_available"
    AVAILABLE = "available"
    CONFIGURED = "configured"
    ERROR = "error"


class DPBenchmarkExecStatus(Enum):
    """Benchmark execution status.

    The state-machine is the following:
    UNSET -> PREPARED -> RUNNING -> STOPPED -> UNSET

    ERROR can be set after any state apart from UNSET, PREPARED, STOPPED.

    UNSET means waiting for prepare to be executed. STOPPED means the sysbench is ready
    but the service is not running.
    """

    UNSET = "unset"
    PREPARED = "prepared"
    RUNNING = "running"
    STOPPED = "stopped"
    ERROR = "error"


class DPBenchmarkIsInWrongStateError(DPBenchmarkError):
    """DPBenchmark is in wrong state error."""

    def __init__(self, unit_state: DPBenchmarkExecStatus, app_state: DPBenchmarkExecStatus):
        self.unit_state = unit_state
        self.app_state = app_state
        super().__init__(f"Unit state: {unit_state}, App state: {app_state}")
