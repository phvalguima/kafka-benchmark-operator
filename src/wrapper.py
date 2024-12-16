#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This script runs the benchmark tool, collects its output and forwards to prometheus."""

import argparse

from overrides import override
from pydantic import BaseModel

from benchmark.wrapper.core import (
    BenchmarkCommand,
    BenchmarkMetrics,
    MetricOptionsModel,
    ProcessModel,
    WorkloadCLIArgsModel,
)
from benchmark.wrapper.main import MainWrapper
from benchmark.wrapper.process import BenchmarkManager, BenchmarkProcess, WorkloadToProcessMapping


class KafkaBenchmarkSample(BaseModel):

    timestamp: float
    produce_rate: float  # in msgs / s
    produce_throughput: float  # in MB/s
    produce_error_rate: int  # in err/s
    produce_latency_avg: float  # in (ms)
    produce_latency_50: float
    produce_latency_99: float
    produce_latency_99_9: float
    produce_latency_max: float

    consume_rate: float  # in msgs / s
    consume_throughput: float  # in MB/s
    consume_error_rate: int  # in err/s
    consume_backlog: float  # in KB


class KafkaMainWrapper(MainWrapper):

    def __init__(self, args: WorkloadCLIArgsModel):
        super().__init__(args)
        metrics = BenchmarkMetrics(
            options=MetricOptionsModel(
                label="openmessaging",
                extra_labels=args.extra_labels.split(","),
                description="Kafka benchmark metric ",
            )
        )
        self.mapping = KafkaWorkloadToProcessMapping(args, metrics)


class KafkaBenchmarkProcess(BenchmarkProcess):
    """This class models one of the processes being executed in the benchmark."""
    @override
    def process_line(self, line: str) -> str | None:
        """Process the line and return the metric."""
        # Kafka has nothing to process
        return None


class KafkaBenchmarkManager(BenchmarkManager):
    """This class is in charge of managing all the processes in the benchmark run."""
    @override
    def process_line(self, line: str) -> str | None:
        """Process the output of the process."""
        ...


class KafkaWorkloadToProcessMapping(WorkloadToProcessMapping):
    """This class maps the workload model to the process."""

    @override
    def _map_prepare(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the prepare phase."""
        # Kafka has nothing to do on prepare
        return None, None

    @override
    def _map_run(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the run phase."""
        driver_path = f"bin/{self.args.workload}-driver"
        workload_path = f"bin/{self.args.workload}-workload"
        processes = [
            KafkaBenchmarkProcess(
                model=ProcessModel(
                    cmd=f"""sudo bin/benchmark --drivers {driver_path} {workload_path}""",
                ),
                args=self.args,
                metrics=self.metrics,
            ),
        ]
        manager = KafkaBenchmarkManager(
            model=ProcessModel(
                cmd=f"""sudo bin/benchmark --drivers {driver_path} {workload_path}""",
            ),
            args=self.args,
            metrics=self.metrics,
            unstarted_workers=processes,
        )
        return manager, processes

    @override
    def _map_clean(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the clean phase."""
        # Kafka has nothing to do on prepare
        return None, None


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="wrapper", description="Runs the benchmark command as an argument."
    )
    parser.add_argument("--test_name", type=str, help="Test name to be used")
    parser.add_argument("--command", type=str, help="Command to be executed", default="run")
    parser.add_argument(
        "--workload", type=str, help="Name of the workload to be executed", default="default"
    )
    parser.add_argument("--report_interval", type=int, default=10)
    parser.add_argument("--parallel_processes", type=int, default=1)
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--run_count", type=int, default=1)
    parser.add_argument(
        "--target_hosts", type=str, default="", help="comma-separated list of target hosts"
    )
    parser.add_argument(
        "--extra_labels",
        type=str,
        help="comma-separated list of extra labels to be used.",
        default="",
    )

    # Parse the arguments as dictionary, using the same logic as:
    # https://github.com/python/cpython/blob/ \
    #     47c5a0f307cff3ed477528536e8de095c0752efa/Lib/argparse.py#L134
    args = parser.parse_args()
    args.command = BenchmarkCommand(parser.parse_args().command.lower())
    KafkaMainWrapper(WorkloadCLIArgsModel.parse_obj(args.__dict__)).run()
