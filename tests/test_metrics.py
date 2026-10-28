import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from prometheus_client import generate_latest
from prometheus_client.parser import text_string_to_metric_families

from fire_n_forget.daemon import Daemon
from fire_n_forget.job import Job
from fire_n_forget.metrics import DaemonMetrics, WorkerMetrics
from fire_n_forget.resource import Cluster, CPUResource, GPUResource, NetworkMetadata, RAMResources


def value(registry, name, **labels):
    for family in text_string_to_metric_families(generate_latest(registry).decode()):
        for sample in family.samples:
            if sample.name == name and sample.labels == labels:
                return sample.value
    raise AssertionError(f'Missing metric {name} {labels}')


class MetricsTests(unittest.TestCase):
    def test_scheduler_metrics_follow_jobs_and_worker(self):
        with tempfile.TemporaryDirectory() as directory, patch('fire_n_forget.daemon.notify_completion'):
            metrics = DaemonMetrics()
            d = Daemon(completion_file=Path(directory)/'jobs.json', metrics=metrics, sender=lambda *args: True)
            cluster = Cluster(NetworkMetadata('localhost', 1), CPUResource(1, 1000), GPUResource(False), RAMResources(4), [])
            worker = d.register(cluster)
            first = d.schedule(Job('first'))
            second = d.schedule(Job('second'))
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_jobs', state='running'), 1)
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_jobs', state='waiting'), 1)
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_reserved', resource='cpu_cores'), 1)
            self.assertTrue(d.complete(first, '', worker))
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_jobs_completed_total'), 1)
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_jobs', state='waiting'), 0)
            self.assertTrue(d.deregister(worker))
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_jobs_requeued_total'), 1)
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_workers'), 0)
            self.assertEqual(value(metrics.registry, 'fire_forget_daemon_jobs', state='waiting'), 1)
            self.assertEqual(d.queue[0], second)

    def test_worker_outcome_series_exist_before_execution(self):
        metrics = WorkerMetrics()
        for outcome in ('success', 'failure', 'error'):
            self.assertEqual(value(metrics.registry, 'fire_forget_worker_jobs_finished_total', outcome=outcome), 0)


if __name__ == '__main__':
    unittest.main()
