# (C) Datadog, Inc. 2018
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

from tagger import get_tags

try:
    from container import is_excluded
except ImportError:
    # Don't fail on < 6.2
    def is_excluded(name, image):
        return False


SOURCE_TYPE = 'kubelet'

CADVISOR_DEFAULT_PORT = 0

# Suffixes per
# https://github.com/kubernetes/kubernetes/blob/8fd414537b5143ab039cb910590237cabf4af783/pkg/api/resource/suffix.go#L108
FACTORS = {
    'n': float(1) / (1000 * 1000 * 1000),
    'u': float(1) / (1000 * 1000),
    'm': float(1) / 1000,
    'k': 1000,
    'M': 1000 * 1000,
    'G': 1000 * 1000 * 1000,
    'T': 1000 * 1000 * 1000 * 1000,
    'P': 1000 * 1000 * 1000 * 1000 * 1000,
    'E': 1000 * 1000 * 1000 * 1000 * 1000 * 1000,
    'Ki': 1024,
    'Mi': 1024 * 1024,
    'Gi': 1024 * 1024 * 1024,
    'Ti': 1024 * 1024 * 1024 * 1024,
    'Pi': 1024 * 1024 * 1024 * 1024 * 1024,
    'Ei': 1024 * 1024 * 1024 * 1024 * 1024 * 1024,
}


def tags_for_pod(pod_id, cardinality):
    return get_tags('kubernetes_pod://%s' % pod_id, cardinality)


def tags_for_docker(cid, cardinality):
    return get_tags('docker://%s' % cid, cardinality)


class ContainerFilter:
    def __init__(self, podlist):
        self.containers = {}

        for pod in podlist.get('items', []):
            for ctr in pod['status'].get('containerStatuses', []):
                cid = ctr.get('containerID')
                if not cid:
                    continue
                self.containers[cid] = ctr
                if "://" in cid:
                    # cAdvisor pushes cids without orchestrator scheme
                    # re-register without the scheme
                    short_cid = cid.split("://", 1)[-1]
                    self.containers[short_cid] = ctr

    def is_excluded(self, cid):
        if cid not in self.containers:
            # Filter out metrics not comming from a container (system slices)
            return True
        ctr = self.containers[cid]
        if not ("name" in ctr and "image" in ctr):
            # Filter out invalid containers
            return True

        return is_excluded(ctr.get("name"), ctr.get("image"))
