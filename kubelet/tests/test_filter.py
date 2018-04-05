# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import sys
import os

import mock
import pytest
import json

from datadog_checks.kubelet import ContainerFilter

# Skip the whole tests module on Windows
pytestmark = pytest.mark.skipif(sys.platform == 'win32', reason='tests for linux only')

# Constants
HERE = os.path.abspath(os.path.dirname(__file__))


def mock_from_file(fname):
    with open(os.path.join(HERE, 'fixtures', fname)) as f:
        return f.read()


def test_container_filter(monkeypatch):
    is_excluded = mock.Mock(return_value=False)
    monkeypatch.setattr('datadog_checks.kubelet.common.is_excluded', is_excluded)

    long_cid = "docker://a335589109ce5506aa69ba7481fc3e6c943abd23c5277016c92dac15d0f40479"
    short_cid = "a335589109ce5506aa69ba7481fc3e6c943abd23c5277016c92dac15d0f40479"
    ctr_name = "datadog-agent"
    ctr_image = "datadog/agent-dev:haissam-tagger-pod-entity"

    pods = json.loads(mock_from_file('pods.txt'))
    filter = ContainerFilter(pods)

    assert filter is not None
    assert len(filter.containers) == 5 * 2
    assert long_cid in filter.containers
    assert short_cid in filter.containers
    is_excluded.assert_not_called()

    # Test non-existing container
    is_excluded.reset_mock()
    assert filter.is_excluded("invalid") is True
    is_excluded.assert_not_called()

    # Test existing unfiltered container
    is_excluded.reset_mock()
    assert filter.is_excluded(short_cid) is False
    is_excluded.assert_called_once()
    is_excluded.assert_called_with(ctr_name, ctr_image)

    # Test existing filtered container
    is_excluded.reset_mock()
    is_excluded.return_value = True
    assert filter.is_excluded(short_cid) is True
    is_excluded.assert_called_once()
    is_excluded.assert_called_with(ctr_name, ctr_image)
