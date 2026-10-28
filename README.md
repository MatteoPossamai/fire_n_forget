# Fire and Forget

Simple distributed task executor on arbitrary
hardware.

## Setup

From the repository directory, run `uv sync` to create `.venv`. Configure
`TELEGRAM_TOKEN` and `CHAT_ID` as exported variables in `.env` for completion
notifications. Create a worker configuration as a JSON object in the format
shown in the register example below; `local/cluster.json` describes this machine.

Start the daemon and then the local worker:

```sh
scripts/run_daemon.sh
scripts/run_client.sh
```

Both scripts start background processes. The daemon listens on port 4015;
the worker listens on the address and port in its configuration. The worker's
address must be reachable from the daemon. For this local setup, both use
`127.0.0.1`. After restarting the daemon, restart the worker too so it
registers again. Restarting the daemon loses its queued and running job state.

## CLI

Run `scripts/fire-n-forget --help` to see all commands. The script uses the
project's `.venv` (create it with `uv sync`); it can also be symlinked into a
directory on `PATH`.

```sh
scripts/fire-n-forget submit --cores 1 'echo Hi'
scripts/fire-n-forget submit --cores 1  # prompts for the command
scripts/fire-n-forget status 0
scripts/fire-n-forget result 0
scripts/fire-n-forget output 0  # last 10 lines, including while running
scripts/fire-n-forget jobs
scripts/fire-n-forget watch
```

Replies are JSON for use in scripts. Set `--daemon tcp://host:4015` before the
subcommand to connect to a different daemon. `complete` exposes the worker
completion API; use `complete --help` for its arguments.
In the monitor, use the arrow keys and Enter (or click a row) to open output;
press Escape to return and `q` to quit. Output refreshes automatically.

For an automated job, quote the entire shell command and use an absolute path
for the working directory and any required environment setup:

```sh
fire-n-forget submit --cores 1 'cd /absolute/project/path && ./run-experiment'
fire-n-forget status 0
fire-n-forget result 0
```

Use the `job_id` returned by submission in place of `0`. The worker runs the
command in its own working directory with its own environment. Resource values
select a suitable worker. Each worker pins a job and its child processes to
the requested number of logical CPUs using `taskset` (required on the worker).
The worker assigns separate CPUs to concurrent jobs; other processes on the
machine can still use those CPUs, and jobs can change their own affinity.
Priority and other resource limits are not changed.
By default, the worker selects CPUs from its allowed affinity, preferring
different physical cores before sibling threads. Optionally add `"cpu_ids":
[0, 1, 2, 3]` to the worker configuration to choose the CPUs explicitly; the
list must contain one distinct, allowed CPU per advertised core. If there are
too few allowed CPUs, the worker fails to start instead of oversubscribing.
The daemon reserves requested CPU cores and RAM while jobs run, so a worker
can execute multiple jobs at once up to its registered capacity. Jobs without
CPU requirements reserve one core; specify at least one core when supplying
CPU requirements. GPU availability is checked but GPUs are not reserved.
The `result` contains the combined stdout and stderr once the command exits.
Completion means the process ended; its exit code is not recorded, so check
the output or artifacts to determine whether it succeeded. Telegram delivery
is best effort. This tool cannot attach to an already-running process.

## Prometheus metrics

The daemon exposes `http://127.0.0.1:9108/metrics` and each worker exposes
`http://127.0.0.1:9109/metrics`. Counters reset when the process restarts.
The daemon reports registered workers, waiting and running jobs, total and
reserved CPU cores and RAM, submissions, completions, requeues, dispatch
attempts and API requests. Reservations are scheduler accounting, not actual
CPU or memory use. Worker metrics include running and finished jobs, command
duration, exit outcome and completion report outcomes. Standard Python and
process metrics are also exposed; process metrics describe the daemon or worker
process, not its child jobs. Worker registration indicates the daemon accepted
registration, not that the connection is currently healthy.

For Prometheus, add these scrape targets (use the actual hostnames when remote):

```yaml
scrape_configs:
  - job_name: fire_n_forget_daemon
    static_configs:
      - targets: ['127.0.0.1:9108']
  - job_name: fire_n_forget_worker
    static_configs:
      - targets: ['127.0.0.1:9109']
```

Use `--metrics-host` and `--metrics-port` with either launch script or Python
module to change the listener. Use a distinct metrics port for each worker on
the same machine. The default listeners are local-only; exposing them on a
network makes metrics accessible to anyone who can reach them.

## API

### Client -> Daemon Server

- `register`: allows client to register with self-supplied resources, that is then promising to guarantee to the cluster
- `deregister`: client removes itself. Its running jobs go back into the queue.
- `complete`: client has finished to execute a job. Sends back results

### Daemon Server -> Client

- `schedule`: daemon sends a compatible job to an available worker.

### Wire format

The daemon listens on TCP port 4015 using a ZeroMQ REP socket. Send one
JSON object per request with a REQ socket; the daemon replies with a JSON
object before accepting the next request.

| Request | Reply |
| --- | --- |
| `{"action":"register","cluster":{"network":["127.0.0.1",5000],"cpu":[4,3000],"gpu":[false],"ram":[16],"datasets":[]}}` | `{"ok":true,"cluster_id":0}` |
| `{"action":"deregister","cluster_id":0}` | `{"ok":true}` if found, otherwise `{"ok":false}` |
| `{"action":"submit","command":"echo hello"}` | `{"ok":true,"job_id":0}` |
| `{"action":"complete","cluster_id":0,"job_id":0,"stdout":"output"}` | `{"ok":true}` if assigned to that worker, otherwise `{"ok":false}` |
| `{"action":"status","job_id":0}` | `{"ok":true,"status":"completed"}` (or `running`, `waiting`, `unknown`) |
| `{"action":"result","job_id":0}` | `{"ok":true,"result":{...}}`, or `null` if not completed |
| `{"action":"output","job_id":0}` | `{"ok":true,"stdout":"..."}` with the last 10 lines |
| `{"action":"jobs"}` | `{"ok":true,"jobs":[...]}` with job IDs, commands and statuses |
| `{"action":"stream","job_id":0,"offset":0}` | `{"ok":true,"data":"..."}` with up to 65536 output bytes encoded as base64 |

Malformed requests return `{"ok":false,"error":"..."}`. Completed records
are saved in `completed_jobs.json` in the daemon's working directory.

Submit may include `requirements`, with `cpu` (`cores`, `max_freq`), `gpu`
(`present`), `ram` (`size`) and `dataset` (`name`, `path`) objects. The daemon
sends compatible queued jobs to workers with sufficient available resources via their registered
network address, using `{"action":"schedule","cluster_id":0,"job_id":0,"command":"echo hello","cores":1}`;
the worker replies `{"ok":true}`. It executes the command with a shell and
reports combined standard output and standard error. Start a worker with
`python -m fire_n_forget.client cluster.json --daemon tcp://127.0.0.1:4015`,
where `cluster.json` contains the cluster object from the register example.
Output is available as the command writes it; programs that buffer output may
need an unbuffered mode for timely updates.

Queued and running jobs are held in memory and do not survive a daemon restart.
Jobs wait until a compatible worker is available. Failed dispatches are retried.
Unexpected worker loss is not detected;
explicit deregistration requeues its job, which may cause it to run twice if the
original worker is still executing it. A lost dispatch acknowledgement can also
cause duplicate execution. There is no authentication; anyone able
to reach the daemon can submit shell commands. Use only on a trusted network.
