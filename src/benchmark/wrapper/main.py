#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This script runs the benchmark tool, collects its output and forwards to prometheus."""

import signal

from core import WorkloadCLIArgsModel
from process import WorkloadToProcessMapping
from prometheus_client import start_http_server


class MainWrapper:
    """Main class to manage the benchmark tool."""

    mapping: WorkloadToProcessMapping

    def __init__(self, args: WorkloadCLIArgsModel):
        self.args = args

    def run(self):
        """Prepares the workload and runs the benchmark."""
        manager, _ = self.mapping.map(self.args.command)

        def _exit(*args, **kwargs):
            manager.stop()

        signal.signal(signal.SIGINT, _exit)
        signal.signal(signal.SIGTERM, _exit)
        start_http_server(8008)

        # Start the manager and process the output
        manager.start()
        # Now, start the event loop to monitor the processes:
        manager.run()


# EXAMPLE
# The code below is an example usage of the main function + the wrapper classes
#
# if __name__ == "__main__":
#     parser = argparse.ArgumentParser(
#         prog="wrapper", description="Runs the benchmark command as an argument."
#     )
#     parser.add_argument("--test_name", type=str, help="Test name to be used")
#     parser.add_argument("--command", type=str, help="Command to be executed", default="run")
#     parser.add_argument(
#         "--workload", type=str, help="Name of the workload to be executed", default="default"
#     )
#     parser.add_argument("--report_interval", type=int, default=10)
#     parser.add_argument("--parallel_processes", type=int, default=1)
#     parser.add_argument("--threads", type=int, default=1)
#     parser.add_argument("--duration", type=int, default=0)
#     parser.add_argument("--run_count", type=int, default=1)
#     parser.add_argument(
#         "--target_hosts", type=str, default="", help="comma-separated list of target hosts"
#     )
#     parser.add_argument(
#         "--extra_labels",
#         type=str,
#         help="comma-separated list of extra labels to be used.",
#         default="",
#     )
#     # Parse the arguments as dictionary, using the same logic as:
#     # https://github.com/python/cpython/blob/ \
#     #     47c5a0f307cff3ed477528536e8de095c0752efa/Lib/argparse.py#L134
#     args = parser.parse_args().__dict__ | {"command": BenchmarkCommand(parser.parse_args().command)}
#     MainWrapper(WorkloadCLIArgsModel.parse_obj(args)).run()
