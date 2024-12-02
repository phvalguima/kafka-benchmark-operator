# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

from enum import StrEnum

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


# Peer relation keys
LIFECYCLE_KEY = "lifecycle"
STOP_KEY = "stop"


class Substrate(StrEnum):
    """Substrate of the benchmark."""

    VM = "vm"
    K8S = "k8s"


class Scope(StrEnum):
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


class DPBenchmarkExecFailedError(DPBenchmarkError):
    """Sysbench execution failed error."""


class DPBenchmarkMissingOptionsError(DPBenchmarkError):
    """Sysbench missing options error."""


class DPBenchmarkDBRelationNotAvailableError(DPBenchmarkError):
    """Sysbench failed to execute a command."""


class DPBenchmarkLifecycleState(StrEnum):
    """Benchmark lifecycle representation.

    The status are:
    * UNSET: the service is not set yet
    * PREPARING: the service is uploading data to the database in its "prepare" phase
    * AVAILABLE: the service is present and ready to be started
    * RUNNING: the service is running
    * FAILED: the service has failed
    * COLLECTING: the service is collecting data
    * UPLOADING: once the RUNNING phase is finished, it moves to "UPLOADING" whilst the data is
                 copied to the S3 endpoint
    * FINISHED: the service has finished
    * STOPPED: the service has been stopped by the user
    """

    UNSET = "unset"
    PREPARING = "preparing"
    AVAILABLE = "available"
    RUNNING = "running"
    FAILED = "failed"
    COLLECTING = "collecting"
    UPLOADING = "uploading"
    FINISHED = "finished"
    STOPPED = "stopped"


class DPBenchmarkLifecycleTransition(StrEnum):
    """Benchmark lifecycle transition representation."""

    PREPARE = "prepare"
    RUN = "run"
    STOP = "stop"
    CLEAN = "clean"


class DPBenchmarkWorkloadLifecycleState(StrEnum):
    """Benchmark lifecycle state representation."""

    PREPARE = "prepare"
    RUN = "run"
    STOP = "stop"


class DPBenchmarkWorkloadState(StrEnum):
    """Represents the different states of the benchmark service."""

    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


class DPBenchmarkRelationLifecycle(StrEnum):
    """Represents the different status of a mandatory relation."""

    NOT_AVAILABLE = "not_available"
    AVAILABLE = "available"
    ERROR = "error"
    READY = "ready"
