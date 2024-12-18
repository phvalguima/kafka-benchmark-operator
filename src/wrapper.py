#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This script runs the benchmark tool, collects its output and forwards to prometheus."""
import os
import re
import time
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

    produce_rate: float  # in msgs / s
    produce_throughput: float  # in MB/s
    produce_error_rate: float  # in err/s

    produce_latency_avg: float  # in (ms)
    produce_latency_50: float
    produce_latency_99: float
    produce_latency_99_9: float
    produce_latency_max: float

    produce_delay_latency_avg: float  # in (us)
    produce_delay_latency_50: float
    produce_delay_latency_99: float
    produce_delay_latency_99_9: float
    produce_delay_latency_max: float

    consume_rate: float  # in msgs / s
    consume_throughput: float  # in MB/s
    consume_backlog: float  # in KB


class KafkaBenchmarkSampleMatcher(BaseModel):

    produce_rate: str = r'Pub rate\s+(.*?)\s+msg/s'
    produce_throughput: str = r'Pub rate\s+\d+.\d+\s+msg/s\s+/\s+(.*?)\s+MB/s'
    produce_error_rate: str = r'Pub err\s+(.*?)\s+err/s'
    produce_latency_avg: str = r'Pub Latency \(ms\) avg:\s+(.*?)\s+'
    # Match: Pub Latency (ms) avg: 1478.1 - 50%: 1312.6 - 99%: 4981.5 - 99.9%: 5104.7 - Max: 5110.5
    # Generates: [('1478.1', '1312.6', '4981.5', '5104.7', '5110.5')]
    produce_latency_percentiles: str = r'Pub Latency \(ms\) avg:\s+(.*?)\s+- 50%:\s+(.*?)\s+- 99%:\s+(.*?)\s+- 99.9%:\s+(.*?)\s+- Max:\s+(.*?)\s+'

    # Pub Delay Latency (us) avg: 21603452.9 - 50%: 21861759.0 - 99%: 23621631.0 - 99.9%: 24160895.0 - Max: 24163839.0
    # Generates: [('21603452.9', '21861759.0', '23621631.0', '24160895.0', '24163839.0')]
    produce_latency_delay_percentiles: str = r'Pub Delay Latency \(us\) avg:\s+(.*?)\s+- 50%:\s+(.*?)\s+- 99%:\s+(.*?)\s+- 99.9%:\s+(.*?)\s+- Max:\s+(\d+\.\d+)'

    consume_rate: str = r'Cons rate\s+(.*?)\s+msg/s'
    consume_throughput: str = r'Cons rate\s+\d+.\d+\s+msg/s\s+/\s+(.*?)\s+MB/s'
    consume_backlog: str = r'Backlog:\s+(.*?)\s+K'



class KafkaMainWrapper(MainWrapper):

    def __init__(self, args: WorkloadCLIArgsModel):
        # As seen in the openmessaging code:
        # https://github.com/openmessaging/benchmark/blob/ \
        #     b10b22767f8063321c90bc9ee1b0aadc5902c31a/benchmark-framework/ \
        #     src/main/java/io/openmessaging/benchmark/WorkloadGenerator.java#L352
        # The report interval is hardcoded to 10 seconds
        args.report_interval = 10
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
    def process_line(self, line: str) -> BaseModel | None:
        """Process the line and return the metric."""
        # Kafka has nothing to process, we never match it
        return None


class KafkaBenchmarkManager(BenchmarkManager):
    """This class is in charge of managing all the processes in the benchmark run."""

    matcher: KafkaBenchmarkSampleMatcher = KafkaBenchmarkSampleMatcher()

    @override
    def process_line(self, line: str) -> BaseModel | None:
        """Process the output of the process."""
        # First, check if we have a match:
        try:
            if not (pub_rate := re.findall(self.matcher.produce_rate, line)):
                # Nothing found, we can have an early return
                return None

            if not (prod_percentiles := re.findall(self.matcher.produce_latency_percentiles, line)):
                return None

            if not (delay_percentiles := re.findall(self.matcher.produce_latency_delay_percentiles, line)):
                return None

            return KafkaBenchmarkSample(
                produce_rate=float(pub_rate[0]),
                produce_throughput=float(re.findall(self.matcher.produce_throughput, line)[0]),
                produce_error_rate=float(re.findall(self.matcher.produce_error_rate, line)[0]),

                produce_latency_avg=float(prod_percentiles[0][0]),
                produce_latency_50=float(prod_percentiles[0][1]),
                produce_latency_99=float(prod_percentiles[0][2]),
                produce_latency_99_9=float(prod_percentiles[0][3]),
                produce_latency_max=float(prod_percentiles[0][4]),

                produce_delay_latency_avg=float(delay_percentiles[0][0]),
                produce_delay_latency_50=float(delay_percentiles[0][1]),
                produce_delay_latency_99=float(delay_percentiles[0][2]),
                produce_delay_latency_99_9=float(delay_percentiles[0][3]),
                produce_delay_latency_max=float(delay_percentiles[0][4]),

                consume_rate=float(re.findall(self.matcher.consume_rate, line)[0]),
                consume_throughput=float(re.findall(self.matcher.consume_throughput, line)[0]),
                consume_backlog=float(re.findall(self.matcher.consume_backlog, line)[0]),
            )
        except Exception as e:
            return None

    @override
    def start(self):
        """Start the benchmark tool."""
        for worker in self.workers:
            worker.start()
        if self.model:
            # In Kafka, we start the manager after the workers
            BenchmarkProcess.start(self)


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
        driver_path = "/root/.benchmark/charmed_parameters/worker_params.yaml"
        workload_path = "/root/.benchmark/charmed_parameters/dpe_benchmark.json"
        processes = [
            KafkaBenchmarkProcess(
                model=ProcessModel(
                    cmd=f"""sudo bin/benchmark-worker -p {peer.split(":")[1]} -sp {int(peer.split(":")[1]) + 1}""",
                    cwd=os.path.join(
                        os.path.dirname(os.path.abspath(__file__)),
                        "../openmessaging-benchmark/",
                    ),
                ),
                args=self.args,
                metrics=self.metrics,
            )
            for peer in self.args.peers.split(",")
        ]
        workers = ",".join([f"http://{peer}" for peer in self.args.peers.split(",")])

        manager = KafkaBenchmarkManager(
            model=ProcessModel(
                cmd=f"""sudo bin/benchmark --workers {workers} --drivers {driver_path} {workload_path}""",
                cwd=os.path.join(
                    os.path.dirname(os.path.abspath(__file__)),
                    "../openmessaging-benchmark/",
                ),
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
        "--peers", type=str, default="", help="comma-separated list of peers to be used."
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
    main_wrapper = KafkaMainWrapper(WorkloadCLIArgsModel.parse_obj(args.__dict__))
    main_wrapper.run()
