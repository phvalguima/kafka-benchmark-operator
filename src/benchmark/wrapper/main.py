#!/usr/bin/python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This script runs the benchmark tool, collects its output and forwards to prometheus."""

import argparse
import signal

from prometheus_client import start_http_server


def main(args: WorkloadCLIArgsModel):
    """Prepares the workload and runs the benchmark."""
    manager, _ = WorkloadToProcessMapping(args).map()

    def _exit(*args, **kwargs):
        manager.stop()

    signal.signal(signal.SIGINT, _exit)
    signal.signal(signal.SIGTERM, _exit)
    start_http_server(8088)

    # Start the manager and process the output
    manager.start()
    # Now, start the event loop to monitor the processes:
    manager.run()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog="osb_svc", description="Runs the benchmark command as an argument."
    )
    parser.add_argument("--command", type=str, help="Command to be executed", default="run")
    parser.add_argument("--target_hosts", type=str, help="comma-separated list of target hosts")
    parser.add_argument("--workload", type=str, help="Name of the workload to be executed")
    parser.add_argument("--threads", type=int, default=1)
    parser.add_argument("--clients", type=int, default=16)
    parser.add_argument("--db_user", type=str)
    parser.add_argument("--db_password", type=str)
    parser.add_argument("--duration", type=int, default=0)
    parser.add_argument("--workload_params", type=str, default=None)
    parser.add_argument(
        "--extra_labels", type=str, help="comma-separated list of extra labels to be used.", default=""
    )
    parser.add_argument(
        "--test_mode", action='store_true', help="Whether to run in test mode",
    )

    args = parser.parse_args()

    main(WorkloadCLIArgsModel(args))