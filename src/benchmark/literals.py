# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

from enum import StrEnum

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


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


class DPBenchmarkLifecyclePhase(StrEnum):
    """Benchmark lifecycle representation.

    The status are:
    * UNSET: the service is not set yet
    * AVAILABLE: the service is present and ready to be started
    * RUNNING: the service is running
    * FAILED: the service has failed
    * COLLECTING: the service is collecting data and will store it locally in a tarball
    * UPLOADING: once the RUNNING phase is finished, it moves to "UPLOADING" whilst the data is
                 copied to the S3 endpoint
    * FINISHED: the service has finished
    * STOPPED: the user explicitly demanded to stop the service
    """

    UNSET = "unset"
    AVAILABLE = "available"
    RUNNING = "running"
    FAILED = "failed"
    COLLECTING = "collecting"
    UPLOADING = "uploading"
    FINISHED = "finished"
    STOPPED = "stopped"


class DPBenchmarkServiceState(StrEnum):
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
