"""Prometheus metrics for the scheduler and worker processes."""
from prometheus_client import CollectorRegistry, Counter, Gauge, Histogram, ProcessCollector, PlatformCollector, GCCollector


def registry():
    result = CollectorRegistry()
    ProcessCollector(registry=result)
    PlatformCollector(registry=result)
    GCCollector(registry=result)
    return result


class DaemonMetrics:
    def __init__(self):
        self.registry = registry()
        kw = {'registry': self.registry}
        self.workers = Gauge('fire_forget_daemon_workers', 'Registered workers', **kw)
        self.jobs = Gauge('fire_forget_daemon_jobs', 'Current jobs by state', ['state'], **kw)
        for state in ('waiting', 'running'):
            self.jobs.labels(state).set(0)
        self.capacity = Gauge('fire_forget_daemon_capacity', 'Registered resource capacity', ['resource'], **kw)
        self.reserved = Gauge('fire_forget_daemon_reserved', 'Resources reserved by running jobs', ['resource'], **kw)
        for resource in ('cpu_cores', 'ram_gib'):
            self.capacity.labels(resource).set(0)
            self.reserved.labels(resource).set(0)
        self.submitted = Counter('fire_forget_daemon_jobs_submitted', 'Accepted job submissions', **kw)
        self.completed = Counter('fire_forget_daemon_jobs_completed', 'Job completions persisted', **kw)
        self.requeued = Counter('fire_forget_daemon_jobs_requeued', 'Running jobs requeued on worker deregistration', **kw)
        self.dispatch = Counter('fire_forget_daemon_dispatch', 'Dispatch attempts by outcome', ['outcome'], **kw)
        for outcome in ('accepted', 'failed'):
            self.dispatch.labels(outcome)
        self.dispatch_seconds = Histogram('fire_forget_daemon_dispatch_seconds', 'Time spent sending a job to a worker', **kw)
        self.requests = Counter('fire_forget_daemon_requests', 'API requests by action and outcome', ['action', 'outcome'], **kw)

    def update(self, daemon):
        self.workers.set(len(daemon.pool))
        self.jobs.labels('waiting').set(len(daemon.queue))
        running = [job for cluster, job in daemon.jobs.values() if cluster is not None]
        self.jobs.labels('running').set(len(running))
        self.capacity.labels('cpu_cores').set(sum(c.cpu.cores for c in daemon.pool.values()))
        self.capacity.labels('ram_gib').set(sum(c.ram.size for c in daemon.pool.values()))
        self.reserved.labels('cpu_cores').set(sum(j.cpu_req.cores if j.cpu_req else 1 for j in running))
        self.reserved.labels('ram_gib').set(sum(j.ram_res.size if j.ram_res else 0 for j in running))


class WorkerMetrics:
    def __init__(self):
        self.registry = registry()
        kw = {'registry': self.registry}
        self.registered = Gauge('fire_forget_worker_registered', 'Worker registration acknowledged by daemon', **kw)
        self.running = Gauge('fire_forget_worker_jobs_running', 'Jobs currently executing', **kw)
        self.started = Counter('fire_forget_worker_jobs_started', 'Jobs accepted for execution', **kw)
        self.finished = Counter('fire_forget_worker_jobs_finished', 'Commands finished by outcome', ['outcome'], **kw)
        for outcome in ('success', 'failure', 'error'):
            self.finished.labels(outcome)
        self.duration = Histogram('fire_forget_worker_job_duration_seconds', 'Command execution duration', **kw)
        self.reports = Counter('fire_forget_worker_completion_reports', 'Completion reports by outcome', ['outcome'], **kw)
        for outcome in ('accepted', 'rejected', 'error'):
            self.reports.labels(outcome)
