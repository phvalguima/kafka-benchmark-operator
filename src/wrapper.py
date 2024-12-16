#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This script runs the benchmark tool, collects its output and forwards to prometheus."""

import argparse

from benchmark.wrapper.core import (
    ProcessModel,
    WorkloadCLIArgsModel,
)
from benchmark.wrapper.main import MainWrapper
from benchmark.wrapper.process import BenchmarkManager, BenchmarkProcess, WorkloadToProcessMapping
from benchmark.wrapper.core import BenchmarkCommand


class KafkaMainWrapper(MainWrapper):

    def __init__(self, args: WorkloadCLIArgsModel):
        super().__init__(args)
        self.mapping = KafkaWorkloadToProcessMapping(args)


class KafkaWorkloadToProcessMapping(WorkloadToProcessMapping):
    """This class maps the workload model to the process."""

    def __init__(self, args: WorkloadCLIArgsModel):
        self.args = args
        self.manager = None

    def _map_prepare(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the prepare phase."""
        # Kafka has nothing to do on prepare
        return None, None

    def _map_run(self) -> tuple[BenchmarkManager, list[BenchmarkProcess] | None]:
        """Returns the mapping for the run phase."""
        driver_path = f"bin/{self.args.workload}-driver"
        workload_path = f"bin/{self.args.workload}-workload"
        processes = [
            BenchmarkProcess(),
        ]
        manager = BenchmarkManager(
            model=ProcessModel(
                cmd=f"""sudo bin/benchmark --drivers {driver_path} {workload_path}""",
            ),
            args=self.args,
            metrics=self.metrics,
            unstarted_workers=processes,
        )
        return manager, processes

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
    args = parser.parse_args().__dict__ | {"command": BenchmarkCommand(parser.parse_args().command)}
    KafkaMainWrapper(WorkloadCLIArgsModel.parse_obj(args)).run()
