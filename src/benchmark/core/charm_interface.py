# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

"""This module describes the basic benchmark interface."""

from abc import ABC, abstractmethod


class DPBenchmarkCharmInterface(ABC):
    """Each benchmark charm must be able to implement the methods below.

    This class allows to separate the actual tasks and service handling from the charm itself.
    Methods related to actions or relation events will call the methods below.
    """

    @abstractmethod
    def supported_workloads(self) -> list[str]:
        """List the supported workloads."""
        raise NotImplementedError

    @abstractmethod
    def execute_benchmark_cmd(self, extra_labels, command: str):
        """Execute the benchmark command."""
        raise NotImplementedError

    @abstractmethod
    def setup_db_relation(self, relation_names: list[str]):
        """Setup the database relation."""
        raise NotImplementedError

    @abstractmethod
    def prepare(self):
        """Prepares the database and sets the state.

        Raises:
            DPBenchmarkExecError
        """
        raise NotImplementedError

    @abstractmethod
    def run(self):
        """Run the benchmark service.

        Raises:
            DPBenchmarkError
        """
        raise NotImplementedError

    @abstractmethod
    def stop(self):
        """Stop the benchmark service.

        Raises:
            DPBenchmarkServiceError: Returns an error if the service fails to stop.
        """
        raise NotImplementedError

    @abstractmethod
    def clean_up(self):
        """Clean up the database and the unit.

        We recheck the service status, as we do not want to make any distinctions between the different steps.

        Raises:
            DPBenchmarkError
        """
        raise NotImplementedError
