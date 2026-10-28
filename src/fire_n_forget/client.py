"""A worker that receives jobs from the daemon and reports their completion."""

import argparse
import base64
import json
import os
from pathlib import Path
import shutil
import subprocess
import tempfile
import threading
import time

import zmq
from prometheus_client import start_http_server
from fire_n_forget.metrics import WorkerMetrics


def worker_cpus(cluster: dict) -> list[int]:
    """Pick allowed logical CPUs, preferring different physical cores."""
    allowed = os.sched_getaffinity(0)
    count = cluster['cpu'][0]
    if type(count) is not int or count < 1:
        raise ValueError('Worker CPU count must be a positive integer')
    selected = cluster.get('cpu_ids')
    if selected is not None:
        if (not isinstance(selected, list) or len(selected) != count
                or any(type(cpu) is not int for cpu in selected)
                or len(set(selected)) != count or not set(selected) <= allowed):
            raise ValueError('cpu_ids must contain one distinct allowed CPU per advertised core')
        return selected[:]
    if count > len(allowed):
        raise ValueError('Worker CPU count exceeds its allowed CPU affinity')
    physical, siblings, seen = [], [], set()
    for cpu in sorted(allowed):
        topology = Path(f'/sys/devices/system/cpu/cpu{cpu}/topology')
        try:
            core = ((topology / 'physical_package_id').read_text().strip(),
                    (topology / 'core_id').read_text().strip())
        except OSError:
            core = ('unknown', str(cpu))
        if core in seen:
            siblings.append(cpu)
        else:
            seen.add(core)
            physical.append(cpu)
    return (physical + siblings)[:count]


def tail_output(file, lines: int = 10) -> str:
    """Read the latest lines without changing the subprocess's file position."""
    end = os.fstat(file.fileno()).st_size
    offset = end
    chunks = []
    newlines = 0
    while offset > 0 and newlines <= lines:
        size = min(8192, offset)
        offset -= size
        chunk = os.pread(file.fileno(), size, offset)
        chunks.append(chunk)
        newlines += chunk.count(b"\n")
    text = b"".join(reversed(chunks)).decode("utf-8", errors="replace")
    return "".join(text.splitlines(keepends=True)[-lines:])


def run_worker(endpoint: str, cluster: dict, metrics: WorkerMetrics | None = None) -> None:
    cpus = worker_cpus(cluster)
    taskset = shutil.which('taskset')
    if taskset is None:
        raise RuntimeError('taskset is required to pin worker jobs')
    context = zmq.Context()
    listener = context.socket(zmq.REP)
    listener.bind(f"tcp://*:{int(cluster['network'][1])}")
    registration = context.socket(zmq.REQ)
    registration.connect(endpoint)
    registration.send_json({"action": "register", "cluster": cluster})
    poller = zmq.Poller()
    poller.register(listener, zmq.POLLIN)
    poller.register(registration, zmq.POLLIN)
    cluster_id = None
    outputs = {}
    allocations = {}
    outputs_lock = threading.Lock()

    def execute(job: dict, output, assigned_cpus: list[int]) -> None:
        started_at = time.monotonic()
        try:
            result = subprocess.run([taskset, '-c', ','.join(map(str, assigned_cpus)),
                                     '/bin/sh', '-c', job['command']],
                                    stdout=output, stderr=subprocess.STDOUT)
        except OSError:
            if metrics:
                metrics.finished.labels('error').inc()
                metrics.running.dec()
                metrics.duration.observe(time.monotonic() - started_at)
            with outputs_lock:
                outputs.pop((job['cluster_id'], job['job_id']), None)
                allocations.pop((job['cluster_id'], job['job_id']), None)
                output.close()
            return
        with outputs_lock:
            allocations.pop((job['cluster_id'], job['job_id']), None)
        if metrics:
            metrics.finished.labels('success' if result.returncode == 0 else 'failure').inc()
            metrics.running.dec()
            metrics.duration.observe(time.monotonic() - started_at)
        output.seek(0)
        stdout = output.read().decode("utf-8", errors="replace")
        socket = context.socket(zmq.REQ)
        socket.connect(endpoint)
        try:
            socket.send_json(
                {
                    "action": "complete",
                    "cluster_id": job["cluster_id"],
                    "job_id": job["job_id"],
                    "stdout": stdout,
                }
            )
            reply = socket.recv_json()
            if metrics:
                metrics.reports.labels('accepted' if isinstance(reply, dict) and reply.get('ok') else 'rejected').inc()
        except (zmq.ZMQError, ValueError):
            if metrics:
                metrics.reports.labels('error').inc()
        finally:
            socket.close(linger=0)
            with outputs_lock:
                outputs.pop((job["cluster_id"], job["job_id"]), None)
                output.close()

    try:
        while True:
            events = dict(poller.poll())
            if registration in events:
                reply = registration.recv_json()
                assert isinstance(reply, dict)
                if not reply.get("ok"):
                    raise RuntimeError(reply.get("error", "Registration failed"))
                cluster_id = reply["cluster_id"]
                if metrics:
                    metrics.registered.set(1)
                poller.unregister(registration)
                registration.close(linger=0)
            if listener in events:
                try:
                    job = listener.recv_json()
                    if (isinstance(job, dict) and job.get("action") == "stream"
                        and type(job.get("cluster_id")) is int and type(job.get("job_id")) is int
                        and type(job.get("offset")) is int and job["offset"] >= 0):
                        with outputs_lock:
                            output = outputs.get((job["cluster_id"], job["job_id"]))
                            if output is None:
                                reply = {"ok": False, "error": "Job output unavailable"}
                            else:
                                data = os.pread(output.fileno(), 65536, job["offset"])
                                reply = {"ok": True, "data": base64.b64encode(data).decode("ascii")}
                        listener.send_json(reply)
                        continue
                    if (isinstance(job, dict) and job.get("action") == "output"
                        and type(job.get("cluster_id")) is int
                        and type(job.get("job_id")) is int):
                        with outputs_lock:
                            output = outputs.get((job["cluster_id"], job["job_id"]))
                            reply = ({"ok": True, "stdout": tail_output(output)} if output is not None
                                     else {"ok": False, "error": "Job output unavailable"})
                        listener.send_json(reply)
                        continue
                    valid = (
                        isinstance(job, dict)
                        and job.get("action") == "schedule"
                        and type(job.get("cluster_id")) is int
                        and type(job.get("job_id")) is int
                        and isinstance(job.get("command"), str)
                        and type(job.get("cores")) is int and job["cores"] > 0
                    )
                except ValueError:
                    valid = False
                    job = None
                if valid:
                    with outputs_lock:
                        key = (job['cluster_id'], job['job_id'])
                        if key in outputs:
                            listener.send_json({"ok": True})
                            continue
                        occupied = {cpu for allocation in allocations.values() for cpu in allocation}
                        available = [cpu for cpu in cpus if cpu not in occupied]
                        assigned_cpus = available[:job['cores']]
                        if len(assigned_cpus) < job['cores']:
                            listener.send_json({"ok": False, "error": "Not enough available CPUs"})
                            continue
                        output = tempfile.TemporaryFile()
                        outputs[key] = output
                        allocations[key] = assigned_cpus
                    if metrics:
                        metrics.started.inc()
                        metrics.running.inc()
                    listener.send_json({"ok": True})
                    threading.Thread(target=execute, args=(job, output, assigned_cpus), daemon=True).start()
                else:
                    listener.send_json({"ok": False})
    finally:
        if metrics:
            metrics.registered.set(0)
        if cluster_id is not None:
            socket = context.socket(zmq.REQ)
            socket.setsockopt(zmq.LINGER, 0)
            socket.setsockopt(zmq.SNDTIMEO, 500)
            socket.setsockopt(zmq.RCVTIMEO, 500)
            socket.connect(endpoint)
            try:
                socket.send_json({"action": "deregister", "cluster_id": cluster_id})
                socket.recv_json()
            except zmq.ZMQError:
                pass
            socket.close()
        listener.close(linger=0)
        registration.close(linger=0)
        context.destroy(linger=0)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a Fire and Forget worker")
    parser.add_argument(
        "cluster_json", help="Path to a JSON file describing this worker's resources"
    )
    parser.add_argument("--daemon", default="tcp://127.0.0.1:4015")
    parser.add_argument('--metrics-host', default='127.0.0.1')
    parser.add_argument('--metrics-port', type=int, default=9109)
    args = parser.parse_args()
    with open(args.cluster_json, encoding="utf-8") as file:
        cluster = json.load(file)
    metrics = WorkerMetrics()
    http_server, _ = start_http_server(args.metrics_port, addr=args.metrics_host, registry=metrics.registry)
    try:
        run_worker(args.daemon, cluster, metrics)
    finally:
        http_server.shutdown()
        http_server.server_close()


if __name__ == "__main__":
    main()
