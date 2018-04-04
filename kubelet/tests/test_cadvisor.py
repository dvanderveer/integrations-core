# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under a 3-clause BSD style license (see LICENSE)
import sys

import pytest
import requests_mock
from requests.exceptions import HTTPError

from datadog_checks.kubelet import KubeletCheck

# Skip the whole tests module on Windows
pytestmark = pytest.mark.skipif(sys.platform == 'win32', reason='tests for linux only')


@requests_mock.mock()
def test_detect_cadvisor_nominal(m):
    m.head('http://kubelet:4192/api/v1.3/subcontainers/', text='{}')
    url = KubeletCheck.detect_cadvisor("http://kubelet:10250", 4192)
    assert url == "http://kubelet:4192/api/v1.3/subcontainers/"


@requests_mock.mock()
def test_detect_cadvisor_404(m):
    m.head('http://kubelet:4192/api/v1.3/subcontainers/', status_code=404)
    with pytest.raises(HTTPError):
        url = KubeletCheck.detect_cadvisor("http://kubelet:10250", 4192)
        assert url == ""


def test_detect_cadvisor_port_zero():
    with pytest.raises(ValueError):
        url = KubeletCheck.detect_cadvisor("http://kubelet:10250", 0)
        assert url == ""
