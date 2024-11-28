#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

import getpass
from unittest.mock import MagicMock

import pytest

import benchmark.wrapper.core as core
import benchmark.wrapper.process as process


class TestBenchmarkProcess(process.BenchmarkProcess):
    def process_line(self, line: str) -> str:
        return line


class TestBenchmarkManager(process.BenchmarkManager):
    def process_line(self, line: str) -> str:
        return line


ARGS = core.WorkloadCLIArgsModel(
    test_name="test",
    command=core.BenchmarkCommand.RUN,
    parallel_processes=1,
    threads=1,
    duration=0,
    run_count=1,
    target_hosts=["localhost"],
    report_interval=10,
)


@pytest.fixture
def manager():
    proc = core.ProcessModel(
        cmd="echo test && sleep 30s",
        user=getpass.getuser(),
    )
    mgr_proc = core.ProcessModel(
        cmd="sleep 30s",
        user=getpass.getuser(),
    )
    metrics = core.BenchmarkMetrics(
        options=core.MetricOptionsModel(
            label="test",
            extra_labels=["test"],
            description="test",
        )
    )
    test_process = TestBenchmarkProcess(
        model=proc,
        args=ARGS,
        metrics=metrics,
    )
    test = TestBenchmarkManager(
        model=mgr_proc,
        args=ARGS,
        metrics=metrics,
        unstarted_workers=[test_process],
    )
    return test


def test_exec(manager):
    try:
        manager.workers[0].process_line = MagicMock()
        manager.workers[0].metrics.add = MagicMock()
        manager.process_line = MagicMock()
        manager.metrics.add = MagicMock()

        manager.start()
        assert manager.workers[0].status() == core.ProcessStatus.RUNNING
        manager.run()
        manager.workers[0].process_line.assert_called_once_with("test\n")
        manager.workers[0].metrics.add.assert_called()
        assert manager.workers[0].status() == core.ProcessStatus.STOPPED
    except Exception:
        # Cleaning up zombies and leftovers
        manager.stop()
        raise
