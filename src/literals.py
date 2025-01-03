# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module contains the constants and models used by the sysbench charm."""

VALID_LOG_LEVELS = ["info", "debug", "warning", "error", "critical"]


TOPIC_NAME = "benchmark_topic"
CLIENT_RELATION_NAME = "kafka"

JAVA_VERSION = "18"

METRICS_PORT = 8088
COS_AGENT_RELATION = "cos-agent"
PEER_RELATION = "benchmark-peer"

# TODO: This file must go away once Kafka starts sharing its certificates via client relation
TRUSTED_CA_RELATION = "trusted-ca"
TRUSTSTORE_LABEL = "trusted-ca-truststore"


class DPBenchmarkError(Exception):
    """Benchmark error."""


class DPBenchmarkExecError(DPBenchmarkError):
    """Sysbench failed to execute a command."""


class DPBenchmarkMultipleRelationsToDBError(DPBenchmarkError):
    """Multiple relations to the same or multiple DBs exist."""


class DPBenchmarkExecFailedError(DPBenchmarkError):
    """Sysbench execution failed error."""


class DPBenchmarkMissingOptionsError(DPBenchmarkError):
    """Sysbench missing options error."""
