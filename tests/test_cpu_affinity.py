import os
import unittest

from fire_n_forget.client import worker_cpus


class WorkerCPUTests(unittest.TestCase):
    def test_explicit_allowed_cpus(self):
        cpus = sorted(os.sched_getaffinity(0))[:2]
        self.assertEqual(worker_cpus({'cpu': [len(cpus), 0], 'cpu_ids': cpus}), cpus)

    def test_automatic_cpus_are_distinct_and_allowed(self):
        allowed = os.sched_getaffinity(0)
        count = min(len(allowed), 4)
        selected = worker_cpus({'cpu': [count, 0]})
        self.assertEqual(len(set(selected)), count)
        self.assertTrue(set(selected) <= allowed)

    def test_invalid_configuration_rejected(self):
        allowed = sorted(os.sched_getaffinity(0))
        for config in ({'cpu': [0, 0]}, {'cpu': [len(allowed) + 1, 0]},
                       {'cpu': [2, 0], 'cpu_ids': [allowed[0], allowed[0]]},
                       {'cpu': [1, 0], 'cpu_ids': [max(allowed) + 1]}):
            with self.subTest(config=config), self.assertRaises(ValueError):
                worker_cpus(config)


if __name__ == '__main__':
    unittest.main()
