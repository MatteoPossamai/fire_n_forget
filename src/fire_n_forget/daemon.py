from dataclasses import dataclass, field
from collections import deque
import json
import logging
from pathlib import Path
import threading
from typing import Callable
import zmq
from fire_n_forget.job import Job, satisfies, JobStatus
from fire_n_forget.json_utils import get_completion, next_job_id, save_completion
from fire_n_forget.resource import Cluster, CPUResource, GPUResource, RAMResources, DatasetResource
from fire_n_forget.notify import notify

PORT: int = 0xFAF  # 4015 for the meme
logger = logging.getLogger(__name__)


def notify_completion(job_id: int, command: str, stdout: str) -> None:
    """Report completion without delaying the daemon's scheduler."""
    def send() -> None:
        try:
            notify(f"Job {job_id} completed\nCommand: {command[:500]}\nOutput:\n{stdout[:1000]}")
        except Exception as exc:
            # HTTP errors can contain the bot token in their URL.
            logger.error("Telegram notification failed (%s)", type(exc).__name__)

    threading.Thread(target=send, daemon=True).start()


def send_job(cluster: Cluster, cluster_id: int, job_id: int, job: Job) -> bool:
    """Send an assignment to a worker and wait briefly for its acknowledgement."""
    context = zmq.Context.instance()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, 1000)
    socket.setsockopt(zmq.RCVTIMEO, 1000)
    try:
        socket.connect(f"tcp://{cluster.network.ip}:{cluster.network.port}")
        socket.send_json({"action": "schedule", "cluster_id": cluster_id,
                          "job_id": job_id, "command": job.command})
        reply = socket.recv_json()
        return isinstance(reply, dict) and reply.get("ok") is True
    except (zmq.ZMQError, ValueError):
        return False
    finally:
        socket.close()


def request_output(cluster: Cluster, cluster_id: int, job_id: int) -> dict:
    socket = zmq.Context.instance().socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, 1000)
    socket.setsockopt(zmq.RCVTIMEO, 1000)
    try:
        socket.connect(f"tcp://{cluster.network.ip}:{cluster.network.port}")
        socket.send_json({"action": "output", "cluster_id": cluster_id, "job_id": job_id})
        reply = socket.recv_json()
        if not isinstance(reply, dict) or not isinstance(reply.get("stdout"), str):
            return {"ok": False, "error": "Worker output unavailable"}
        return {"ok": True, "stdout": reply["stdout"]}
    except (zmq.ZMQError, ValueError):
        return {"ok": False, "error": "Worker output unavailable"}
    finally:
        socket.close()


@dataclass(slots=True)
class Daemon:
    pool: dict[int, Cluster] = field(default_factory=dict)
    queue: deque[int] = field(default_factory=deque)
    free_cluster: set = field(default_factory=set)
    used_cluster: set = field(default_factory=set)
    jobs: dict[int, tuple[int | None, Job]] = field(default_factory=dict)
    _cluster_counter: int = 0
    _run_counter: int = 0
    completion_file: Path = field(default_factory=lambda: Path("completed_jobs.json"))
    sender: Callable[[Cluster, int, int, Job], bool] = send_job

    def __post_init__(self) -> None:
        self._run_counter = max(self._run_counter, next_job_id(self.completion_file))

    def register(self, cluster: Cluster) -> int:
        self.pool[self._cluster_counter] = cluster
        self.free_cluster.add(self._cluster_counter)
        self._cluster_counter += 1
        self.dispatch()
        return self._cluster_counter - 1

    def deregister(self, idx: int) -> bool:
        if idx not in self.pool:
            return False
        del self.pool[idx]
        self.free_cluster.discard(idx)
        self.used_cluster.discard(idx)
        for job_id, (cluster_id, job) in self.jobs.items():
            if cluster_id == idx:
                self.jobs[job_id] = (None, job)
                self.queue.appendleft(job_id)
        self.dispatch()
        return True

    def schedule(self, job: Job) -> int:
        job_id = self._run_counter
        self._run_counter += 1
        self.jobs[job_id] = (None, job)
        self.queue.append(job_id)
        self.dispatch()
        return job_id

    def dispatch(self) -> None:
        """Try the oldest compatible waiting job on each available worker."""
        for cluster_id in sorted(self.free_cluster):
            cluster = self.pool[cluster_id]
            for job_id in self.queue:
                _, job = self.jobs[job_id]
                if not satisfies(cluster, job):
                    continue
                if self.sender(cluster, cluster_id, job_id, job):
                    self.queue.remove(job_id)
                    self.jobs[job_id] = (cluster_id, job)
                    self.free_cluster.remove(cluster_id)
                    self.used_cluster.add(cluster_id)
                break

    def complete(self, job_id: int, stdout: str, cluster_id: int | None = None) -> bool:
        if job_id not in self.jobs:
            return False
        assigned, job = self.jobs[job_id]
        if assigned is None or (cluster_id is not None and assigned != cluster_id):
            return False
        save_completion(self.completion_file, job_id, job.command, stdout)
        del self.jobs[job_id]
        if assigned in self.pool:
            self.used_cluster.discard(assigned)
            self.free_cluster.add(assigned)
        self.dispatch()
        notify_completion(job_id, job.command, stdout)
        return True

    def job_status(self, job_id: int) -> JobStatus:
        if job_id not in self.jobs:
            if self.get_completed_job(job_id) is not None:
                return JobStatus.COMPLETED
            return JobStatus.UNKNOWN
        elif self.jobs[job_id][0] is None:
            return JobStatus.WAITING
        else:
            return JobStatus.RUNNING

    def get_completed_job(self, job_id: int) -> dict | None:
        return get_completion(self.completion_file, job_id)


def handle_message(daemon: Daemon, message: object) -> dict:
    """Handle one JSON request and return a JSON-serializable reply."""
    if not isinstance(message, dict):
        raise ValueError("Request must be a JSON object")
    action = message.get("action")
    if action == "submit":
        command = message.get("command")
        if not isinstance(command, str) or not command.strip():
            raise ValueError("command must be a non-empty string")
        requirements = message.get("requirements", {})
        if not isinstance(requirements, dict):
            raise ValueError("requirements must be an object")
        try:
            job = Job(command=command,
                      cpu_req=CPUResource(**requirements["cpu"]) if "cpu" in requirements else None,
                      gpu_req=GPUResource(**requirements["gpu"]) if "gpu" in requirements else None,
                      ram_res=RAMResources(**requirements["ram"]) if "ram" in requirements else None,
                      dataset_res=DatasetResource(**requirements["dataset"]) if "dataset" in requirements else None)
        except (TypeError, KeyError) as exc:
            raise ValueError("Invalid job requirements") from exc
        if (job.cpu_req is not None and
            (type(job.cpu_req.cores) is not int or type(job.cpu_req.max_freq) is not int or
             job.cpu_req.cores < 0 or job.cpu_req.max_freq < 0) or
            job.gpu_req is not None and type(job.gpu_req.present) is not bool or
            job.ram_res is not None and (type(job.ram_res.size) is not int or job.ram_res.size < 0) or
            job.dataset_res is not None and
            (not isinstance(job.dataset_res.name, str) or not isinstance(job.dataset_res.path, str))):
            raise ValueError("Invalid job requirements")
        return {"ok": True, "job_id": daemon.schedule(job)}
    if action == "register":
        data = message.get("cluster")
        if not isinstance(data, dict):
            raise ValueError("cluster must be an object")
        try:
            cluster = Cluster.from_json(data)
        except (AssertionError, TypeError, ValueError, IndexError, KeyError) as exc:
            raise ValueError("Invalid cluster resources") from exc
        return {"ok": True, "cluster_id": daemon.register(cluster)}
    if action == "deregister":
        cluster_id = message.get("cluster_id")
        if type(cluster_id) is not int or cluster_id < 0:
            raise ValueError("cluster_id must be a non-negative integer")
        return {"ok": daemon.deregister(cluster_id)}
    if action in ("complete", "status", "result", "output"):
        job_id = message.get("job_id")
        if type(job_id) is not int or job_id < 0:
            raise ValueError("job_id must be a non-negative integer")
        if action == "complete":
            stdout = message.get("stdout")
            if not isinstance(stdout, str):
                raise ValueError("stdout must be a string")
            cluster_id = message.get("cluster_id")
            if type(cluster_id) is not int or cluster_id < 0:
                raise ValueError("cluster_id must be a non-negative integer")
            return {"ok": daemon.complete(job_id, stdout, cluster_id)}
        if action == "status":
            return {"ok": True, "status": daemon.job_status(job_id).value}
        if action == "output":
            if job_id in daemon.jobs:
                cluster_id, _ = daemon.jobs[job_id]
                if cluster_id is None:
                    return {"ok": False, "error": "Job is waiting"}
                return request_output(daemon.pool[cluster_id], cluster_id, job_id)
            result = daemon.get_completed_job(job_id)
            if result is None:
                return {"ok": False, "error": "Unknown job"}
            return {"ok": True, "stdout": "".join(result["stdout"].splitlines(keepends=True)[-10:])}
        return {"ok": True, "result": daemon.get_completed_job(job_id)}
    raise ValueError("Unknown action")


def daemon_main():
    daemon = Daemon()
    context = zmq.Context()

    socket = context.socket(zmq.REP)
    socket.bind(f"tcp://*:{PORT}")

    try:
        while True:
            if not socket.poll(1000, zmq.POLLIN):
                daemon.dispatch()
                continue
            raw_message = socket.recv()
            try:
                message = json.loads(raw_message)
                reply = handle_message(daemon, message)
            except (ValueError, UnicodeDecodeError) as exc:
                reply = {"ok": False, "error": str(exc)}
            except OSError:
                reply = {"ok": False, "error": "Could not access the completion file"}
            socket.send_json(reply)
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    daemon_main()
