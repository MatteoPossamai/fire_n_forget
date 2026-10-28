"""Command-line client for the daemon's JSON API."""

import argparse
import json
import sys
from pathlib import Path

import zmq


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Submit and inspect Fire and Forget jobs"
    )
    parser.add_argument(
        "--daemon", default="tcp://127.0.0.1:4015", help="daemon ZeroMQ endpoint"
    )
    commands = parser.add_subparsers(dest="action", required=True)
    submit = commands.add_parser("submit", help="submit a shell command")
    submit.add_argument(
        "command", nargs="?", help="shell command (prompted for if omitted)"
    )
    submit.add_argument("--cores", type=int, default=1, help="minimum CPU cores")
    submit.add_argument(
        "--cpu-mhz", type=int, default=0, help="minimum CPU frequency in MHz"
    )
    submit.add_argument("--ram", type=int, default=0, help="minimum RAM in GiB")
    submit.add_argument(
        "--gpu", default=False, action="store_true", help="require a GPU"
    )
    submit.add_argument(
        "--dataset",
        nargs=2,
        metavar=("NAME", "PATH"),
        help="required dataset name and path",
    )
    for action in ("status", "result"):
        command = commands.add_parser(action, help=f"get job {action}")
        command.add_argument("job_id", type=int)
    complete = commands.add_parser(
        "complete", help="report completion for an assigned job"
    )
    complete.add_argument("cluster_id", type=int)
    complete.add_argument("job_id", type=int)
    complete.add_argument(
        "--stdout-file", type=Path, help="read output from a file; defaults to stdin"
    )
    args = parser.parse_args()
    request = {"action": args.action}
    try:
        if args.action == "submit":
            command = args.command
            if command is None:
                if not sys.stdin.isatty():
                    parser.error("provide a command, or run interactively")
                command = input("Command: ")
            request["command"] = command
            requirements = {}
            if args.cores is not None or args.cpu_mhz:
                requirements["cpu"] = {
                    "cores": args.cores or 0,
                    "max_freq": args.cpu_mhz,
                }
            if args.ram is not None:
                requirements["ram"] = {"size": args.ram}
            if args.gpu:
                requirements["gpu"] = {"present": True}
            if args.dataset:
                requirements["dataset"] = dict(zip(("name", "path"), args.dataset))
            if requirements:
                request["requirements"] = requirements
        elif args.action in ("status", "result"):
            request["job_id"] = args.job_id
        else:
            request.update(
                cluster_id=args.cluster_id,
                job_id=args.job_id,
                stdout=args.stdout_file.read_text(encoding="utf-8")
                if args.stdout_file
                else sys.stdin.read(),
            )
    except (OSError, ValueError, EOFError) as exc:
        print(f"Error: {exc}", file=sys.stderr)
        return 1

    context = zmq.Context()
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    try:
        socket.connect(args.daemon)
        socket.send_json(request)
        reply = socket.recv_json()
        print(json.dumps(reply, indent=2))
        return 0 if isinstance(reply, dict) and reply.get("ok") is True else 1
    except (zmq.ZMQError, ValueError) as exc:
        print(f"Daemon request failed: {exc}", file=sys.stderr)
        return 1
    finally:
        socket.close()
        context.term()


if __name__ == "__main__":
    sys.exit(main())
