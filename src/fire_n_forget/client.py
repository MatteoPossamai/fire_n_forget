"""A worker that receives jobs from the daemon and reports their completion."""
import argparse
import json
import subprocess
import threading

import zmq


def run_worker(endpoint: str, cluster: dict) -> None:
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

    def execute(job: dict) -> None:
        result = subprocess.run(job["command"], shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.STDOUT)
        socket = context.socket(zmq.REQ)
        socket.connect(endpoint)
        try:
            socket.send_json({"action": "complete", "cluster_id": job["cluster_id"],
                              "job_id": job["job_id"],
                              "stdout": result.stdout.decode("utf-8", errors="replace")})
            socket.recv_json()
        except zmq.ZMQError:
            pass
        finally:
            socket.close(linger=0)

    try:
        while True:
            events = dict(poller.poll())
            if registration in events:
                reply = registration.recv_json()
                if not reply.get("ok"):
                    raise RuntimeError(reply.get("error", "Registration failed"))
                cluster_id = reply["cluster_id"]
                poller.unregister(registration)
                registration.close(linger=0)
            if listener in events:
                try:
                    job = listener.recv_json()
                    valid = (isinstance(job, dict) and job.get("action") == "schedule"
                             and type(job.get("cluster_id")) is int
                             and type(job.get("job_id")) is int
                             and isinstance(job.get("command"), str))
                except ValueError:
                    valid = False
                listener.send_json({"ok": valid})
                if valid:
                    threading.Thread(target=execute, args=(job,), daemon=True).start()
    finally:
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
    parser.add_argument("cluster_json", help="Path to a JSON file describing this worker's resources")
    parser.add_argument("--daemon", default="tcp://127.0.0.1:4015")
    args = parser.parse_args()
    with open(args.cluster_json, encoding="utf-8") as file:
        cluster = json.load(file)
    run_worker(args.daemon, cluster)


if __name__ == "__main__":
    main()
