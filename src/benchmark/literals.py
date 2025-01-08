# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

from enum import Enum

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


# Peer relation keys
LIFECYCLE_KEY = "lifecycle"
STOP_KEY = "stop"


class Substrate(str, Enum):
    """Substrate of the benchmark."""

    VM = "vm"
    K8S = "k8s"


class Scope(str, Enum):
    """Scope of the benchmark."""

    UNIT = "unit"
    APP = "app"


METRICS_PORT = 8008
COS_AGENT_RELATION = "cos-agent"
PEER_RELATION = "benchmark-peer"


BENCHMARK_WORKLOAD_PATH = "/root/.benchmark/charmed_parameters"


class DPBenchmarkError(Exception):
    """Benchmark error."""


class DPBenchmarkMissingOptionsError(DPBenchmarkError):
    """Sysbench missing options error."""


class DPBenchmarkLifecycleState(str, Enum):
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


class DPBenchmarkLifecycleTransition(str, Enum):
    """Benchmark lifecycle transition representation."""

    PREPARE = "prepare"
    RUN = "run"
    STOP = "stop"
    CLEAN = "clean"


class DPBenchmarkWorkloadLifecycleState(str, Enum):
    """Benchmark lifecycle state representation."""

    PREPARE = "prepare"
    RUN = "run"
    STOP = "stop"


class DPBenchmarkWorkloadState(str, Enum):
    """Represents the different states of the benchmark service."""

    RUNNING = "running"
    STOPPED = "stopped"
    FAILED = "failed"


class DPBenchmarkRelationLifecycle(str, Enum):
    """Represents the different status of a mandatory relation."""

    NOT_AVAILABLE = "not_available"
    AVAILABLE = "available"
    ERROR = "error"
    READY = "ready"
