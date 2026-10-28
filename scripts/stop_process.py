"""Stop this project's daemon or local client without matching other projects."""
import os
from pathlib import Path
import signal
import sys
import time

root = Path(__file__).resolve().parent.parent
name = sys.argv[1]
if name not in {"daemon", "client"}:
    raise SystemExit("Expected daemon or client")

pids = []
for proc in Path('/proc').iterdir():
    if not proc.name.isdigit():
        continue
    try:
        if proc.stat().st_uid != os.getuid() or (proc / 'cwd').resolve() != root:
            continue
        args = (proc / 'cmdline').read_bytes().split(b'\0')
        if not args or not Path(os.fsdecode(args[0])).name.startswith('python'):
            continue
        module = f'fire_n_forget.{name}'.encode()
        script = os.fsencode(root / 'src' / 'fire_n_forget' / f'{name}.py')
        if (len(args) > 2 and args[1:3] == [b'-m', module]) or (len(args) > 1 and args[1] == script):
            pids.append(int(proc.name))
    except (OSError, ValueError):
        continue

for pid in pids:
    try:
        os.kill(pid, signal.SIGTERM)
    except ProcessLookupError:
        pass

deadline = time.monotonic() + 5
for pid in pids:
    while Path(f'/proc/{pid}').exists():
        if time.monotonic() >= deadline:
            raise SystemExit(f'{name} process {pid} did not stop')
        time.sleep(0.1)
print(f'Stopped {len(pids)} {name} process(es).')
