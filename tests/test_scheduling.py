import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from fire_n_forget.daemon import Daemon, handle_message
from fire_n_forget.job import Job, JobStatus
from fire_n_forget.resource import Cluster, CPUResource, GPUResource, NetworkMetadata, RAMResources


def worker(cores=3, ram=8):
    return Cluster(NetworkMetadata('127.0.0.1', 5000), CPUResource(cores, 4000),
                   GPUResource(False), RAMResources(ram), [])


class SchedulingTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.sent = []
        self.daemon = Daemon(completion_file=Path(self.temp.name) / 'jobs.json',
                             sender=lambda cluster, cid, jid, job: self.sent.append((cid, jid)) or True)
        self.notify = patch('fire_n_forget.daemon.notify_completion')
        self.notify.start()
        self.addCleanup(self.notify.stop)

    def test_concurrency_and_completion_releases_cores(self):
        d = self.daemon
        cluster = d.register(worker())
        ids = [d.schedule(Job(str(n))) for n in range(4)]
        self.assertEqual([d.job_status(i) for i in ids], [JobStatus.RUNNING]*3 + [JobStatus.WAITING])
        self.assertTrue(d.complete(ids[1], 'done', cluster))
        self.assertEqual(d.job_status(ids[3]), JobStatus.RUNNING)
        self.assertEqual(self.sent, [(cluster, i) for i in ids])
        self.assertFalse(d.complete(ids[1], 'duplicate', cluster))

    def test_multicore_and_memory_capacity(self):
        d = self.daemon
        cluster = d.register(worker(cores=4, ram=8))
        first = d.schedule(Job('first', cpu_req=CPUResource(2, 0), ram_res=RAMResources(6)))
        second = d.schedule(Job('second', cpu_req=CPUResource(2, 0), ram_res=RAMResources(3)))
        third = d.schedule(Job('third', cpu_req=CPUResource(2, 0), ram_res=RAMResources(2)))
        self.assertEqual(d.job_status(second), JobStatus.WAITING)
        self.assertEqual(d.job_status(third), JobStatus.RUNNING)
        self.assertTrue(d.complete(first, '', cluster))
        self.assertEqual(d.job_status(second), JobStatus.RUNNING)

    def test_deregister_reschedules_all_and_rejects_stale_completion(self):
        d = self.daemon
        old = d.register(worker())
        ids = [d.schedule(Job(str(n))) for n in range(3)]
        self.assertTrue(d.deregister(old))
        self.assertEqual(list(d.queue), ids)
        new = d.register(worker())
        self.assertTrue(all(d.job_status(i) == JobStatus.RUNNING for i in ids))
        self.assertFalse(d.complete(ids[0], '', old))
        self.assertTrue(d.complete(ids[0], '', new))

    def test_failed_send_keeps_capacity_and_zero_cores_rejected(self):
        d = self.daemon
        d.sender = lambda *args: False
        d.register(worker())
        job = d.schedule(Job('waiting'))
        self.assertEqual(d.job_status(job), JobStatus.WAITING)
        with self.assertRaises(ValueError):
            handle_message(d, {'action': 'submit', 'command': 'x', 'requirements': {'cpu': {'cores': 0, 'max_freq': 0}}})
        d.sender = lambda *args: True
        d.dispatch()
        self.assertEqual(d.job_status(job), JobStatus.RUNNING)


if __name__ == '__main__':
    unittest.main()
