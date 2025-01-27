#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import MagicMock

from benchmark.literals import DPBenchmarkLifecycleState, DPBenchmarkLifecycleTransition
from benchmark.managers.lifecycle import LifecycleManager


class TestLifecycleManager(LifecycleManager):
    def __init__(self, peers, config_manager):
        self.peers = peers
        self.config_manager = config_manager
        self.config_manager.workload.is_failed = MagicMock(return_value=False)


class MockPeerState:
    def __init__(self, lifecycle):
        self.lifecycle = lifecycle


def test_next_state_clean():
    config = MagicMock()
    peers = MagicMock()
    peers.unit_state.lifecycle = DPBenchmarkLifecycleState.STOPPED
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)

    assert (
        lifecycle_manager.next(DPBenchmarkLifecycleTransition.CLEAN)
        == DPBenchmarkLifecycleState.UNSET
    )


def test_next_state_stop():
    config = MagicMock()
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.STOPPED))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)

    assert (
        lifecycle_manager.next(DPBenchmarkLifecycleTransition.STOP)
        == DPBenchmarkLifecycleState.STOPPED
    )
    # Check the other condition
    assert lifecycle_manager.next(None) == DPBenchmarkLifecycleState.STOPPED


def test_next_state_prepare():
    config = MagicMock()
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.UNSET))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)

    assert (
        lifecycle_manager.next(DPBenchmarkLifecycleTransition.PREPARE)
        == DPBenchmarkLifecycleState.PREPARING
    )


def test_next_state_prepare_but_peer_already_prepared():
    config = MagicMock()
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.AVAILABLE))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)

    # Return None as there are peers in the AVAILABLE or higher state.
    assert lifecycle_manager.next(DPBenchmarkLifecycleTransition.PREPARE) is None


def test_next_state_prepare_available_as_leader():
    config = MagicMock()
    config.is_prepared = MagicMock(return_value=True)
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.UNSET))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)
    lifecycle_manager.current = MagicMock(return_value=DPBenchmarkLifecycleState.PREPARING)

    assert lifecycle_manager.next(None) == DPBenchmarkLifecycleState.AVAILABLE


def test_next_state_prepare_available_as_follower():
    config = MagicMock()
    config.is_prepared = MagicMock(return_value=False)
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.AVAILABLE))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)
    lifecycle_manager.current = MagicMock(return_value=DPBenchmarkLifecycleState.UNSET)

    assert lifecycle_manager.next(None) == DPBenchmarkLifecycleState.AVAILABLE


def test_next_state_run_as_leader():
    config = MagicMock()
    config.is_prepared = MagicMock(return_value=False)
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.AVAILABLE))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)
    lifecycle_manager.current = MagicMock(return_value=DPBenchmarkLifecycleState.AVAILABLE)

    assert (
        lifecycle_manager.next(DPBenchmarkLifecycleTransition.RUN)
        == DPBenchmarkLifecycleState.RUNNING
    )


def test_next_state_run_as_follower():
    config = MagicMock()
    config.is_prepared = MagicMock(return_value=False)
    peers = MagicMock()
    peers.unit_state = MagicMock(return_value=MockPeerState(DPBenchmarkLifecycleState.RUNNING))
    peers.units = MagicMock(return_value=[])
    lifecycle_manager = TestLifecycleManager(peers, config)
    lifecycle_manager.current = MagicMock(return_value=DPBenchmarkLifecycleState.AVAILABLE)

    assert lifecycle_manager.next(None) == DPBenchmarkLifecycleState.RUNNING
