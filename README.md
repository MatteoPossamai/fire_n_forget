# Fire and Forget

Simple distributed task executor on arbitrary
hardware.

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
```

Replies are JSON for use in scripts. Set `--daemon tcp://host:4015` before the
subcommand to connect to a different daemon. `complete` exposes the worker
completion API; use `complete --help` for its arguments.

## API

### Client -> Daemon Server

- `register`: allows client to register with self-supplied resources, that is then promising to guarantee to the cluster
- `deregister`: client removes itself. Its running job goes back into the queue.
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

Malformed requests return `{"ok":false,"error":"..."}`. Completed records
are saved in `completed_jobs.json` in the daemon's working directory.

Submit may include `requirements`, with `cpu` (`cores`, `max_freq`), `gpu`
(`present`), `ram` (`size`) and `dataset` (`name`, `path`) objects. The daemon
sends the oldest compatible job to each available worker via its registered
network address, using `{"action":"schedule","cluster_id":0,"job_id":0,"command":"echo hello"}`;
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
