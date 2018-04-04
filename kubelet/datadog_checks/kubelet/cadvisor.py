# (C) Datadog, Inc. 2010-2018
# All rights reserved
# Licensed under Simplified BSD License (see LICENSE)

"""kubernetes check
Collects metrics from cAdvisor instance
"""
# stdlib
from fnmatch import fnmatch
import numbers
import re
from urlparse import urlparse

# 3p
import requests

# project
from datadog_checks.config import is_affirmative

# check
from .common import FACTORS, tags_for_docker

NAMESPACE = "kubernetes"
DEFAULT_MAX_DEPTH = 10
DEFAULT_PUBLISH_ALIASES = False
DEFAULT_ENABLED_RATES = [
    'diskio.io_service_bytes.stats.total',
    'network.??_bytes',
    'cpu.*.total']
DEFAULT_ENABLED_GAUGES = [
    'memory.usage',
    'filesystem.usage']

NET_ERRORS = ['rx_errors', 'tx_errors', 'rx_dropped', 'tx_dropped']

LEGACY_CADVISOR_METRICS_PATH = '/api/v1.3/subcontainers/'


class CadvisorScraper():
    @staticmethod
    def detect_cadvisor(kubelet_url, cadvisor_port):
        if cadvisor_port == 0:
            raise ValueError("cAdvisor port set to 0 in configuration")
        kubelet_hostname = urlparse(kubelet_url).hostname
        if not kubelet_hostname:
            raise ValueError("kubelet hostname empty")
        url = "http://{}:{}{}".format(kubelet_hostname, cadvisor_port,
                                      LEGACY_CADVISOR_METRICS_PATH)

        # Test the endpoint is present
        r = requests.head(url, timeout=1)
        r.raise_for_status()

        return url

    def retrieve_cadvisor_metrics(self, timeout=10):
        return requests.get(self.cadvisor_legacy_url, timeout=timeout).json()

    def process_cadvisor(self, instance):
        self.max_depth = instance.get('max_depth', DEFAULT_MAX_DEPTH)
        enabled_gauges = instance.get('enabled_gauges', DEFAULT_ENABLED_GAUGES)
        self.enabled_gauges = ["{0}.{1}".format(NAMESPACE, x) for x in enabled_gauges]
        enabled_rates = instance.get('enabled_rates', DEFAULT_ENABLED_RATES)
        self.enabled_rates = ["{0}.{1}".format(NAMESPACE, x) for x in enabled_rates]
        self.publish_aliases = is_affirmative(instance.get('publish_aliases', DEFAULT_PUBLISH_ALIASES))

        self._update_metrics(instance)

    def _update_metrics(self, instance):
        def parse_quantity(s):
            number = ''
            unit = ''
            for c in s:
                if c.isdigit() or c == '.':
                    number += c
                else:
                    unit += c
            return float(number) * FACTORS.get(unit, 1)

        metrics = self.retrieve_cadvisor_metrics()

        if not metrics:
            raise Exception('No metrics retrieved cmd=%s' % self.metrics_cmd)

        for subcontainer in metrics:
            c_id = subcontainer.get('id')
            if 'aliases' not in subcontainer:
                # it means the subcontainer is about a higher-level entity than a container
                continue
            try:
                self._update_container_metrics(instance, subcontainer)
            except Exception as e:
                self.log.error("Unable to collect metrics for container: {0} ({1})".format(c_id, e))

    def _publish_raw_metrics(self, metric, dat, tags, depth=0):
        if depth >= self.max_depth:
            self.log.warning('Reached max depth on metric=%s' % metric)
            return

        if isinstance(dat, numbers.Number):
            if self.enabled_rates and any([fnmatch(metric, pat) for pat in self.enabled_rates]):
                self.rate(metric, float(dat), tags)
            elif self.enabled_gauges and any([fnmatch(metric, pat) for pat in self.enabled_gauges]):
                self.gauge(metric, float(dat), tags)

        elif isinstance(dat, dict):
            for k, v in dat.iteritems():
                self._publish_raw_metrics(metric + '.%s' % k.lower(), v, tags, depth + 1)

        elif isinstance(dat, list):
            self._publish_raw_metrics(metric, dat[-1], tags, depth + 1)

    def _update_container_metrics(self, instance, subcontainer):
        tags = tags_for_docker(subcontainer.get('id'), True)

        if not tags:
            self.log.debug("Subcontainer doesn't have tags, skipping.")
            return

        tags = list(set(tags + instance.get('tags', [])))

        stats = subcontainer['stats'][-1]  # take the latest
        self._publish_raw_metrics(NAMESPACE, stats, tags)

        if subcontainer.get("spec", {}).get("has_filesystem") and stats.get('filesystem', []) != []:
            fs = stats['filesystem'][-1]
            fs_utilization = float(fs['usage']) / float(fs['capacity'])
            self.gauge(NAMESPACE + '.filesystem.usage_pct', fs_utilization, tags=tags)

        if subcontainer.get("spec", {}).get("has_network"):
            net = stats['network']
            self.rate(NAMESPACE + '.network_errors',
                      sum(float(net[x]) for x in NET_ERRORS),
                      tags=tags)

        return tags
