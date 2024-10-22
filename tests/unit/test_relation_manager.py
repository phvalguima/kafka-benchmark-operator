#!/usr/bin/env python3
# Copyright 2024 Canonical Ltd.
# See LICENSE file for licensing details.

from unittest.mock import PropertyMock, patch

import pytest
from ops.testing import Harness

from benchmark.core.models import (
    DPBenchmarkBaseDatabaseModel,
)
from charm import OpenSearchBenchmarkOperator


@pytest.fixture
def harness():
    harness = Harness(OpenSearchBenchmarkOperator)
    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.begin()

    return harness


@pytest.fixture
def harness_with_db_relation():
    harness = Harness(OpenSearchBenchmarkOperator)
    identity = harness.add_relation("opensearch", "opensearch")
    relation = harness.model.get_relation("opensearch")
    harness.add_relation_unit(identity, "opensearch/0")
    harness.update_relation_data(
        identity,
        relation.app.name,
        {
            "index": "test",
            "endpoints": "localhost",
            "username": "user",
            "password": "pass",
        },
    )
    with patch("ops.model.Model.name", new_callable=PropertyMock) as mock_name:
        mock_name.return_value = "test_model"
        harness.begin()

    return (harness, identity)


def test_relation_status_not_available(harness):
    assert not harness.charm.database.state.get()


def test_relation_status_working(harness_with_db_relation):
    harness, _ = harness_with_db_relation
    assert harness.charm.database.state.get() == DPBenchmarkBaseDatabaseModel(
        hosts=["https://localhost"],
        unix_socket=None,
        username="user",
        password="pass",
        db_name="test",
    )
