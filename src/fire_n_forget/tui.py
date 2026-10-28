"""Interactive job monitor."""
import base64
import codecs
import curses
import time

import zmq


def request(context, endpoint, message):
    socket = context.socket(zmq.REQ)
    socket.setsockopt(zmq.LINGER, 0)
    socket.setsockopt(zmq.SNDTIMEO, 1500)
    socket.setsockopt(zmq.RCVTIMEO, 1500)
    try:
        socket.connect(endpoint)
        socket.send_json(message)
        reply = socket.recv_json()
        if not isinstance(reply, dict) or not reply.get('ok'):
            return None, reply.get('error', 'Request failed') if isinstance(reply, dict) else 'Invalid daemon reply'
        return reply, None
    except (zmq.ZMQError, ValueError) as exc:
        return None, f'Daemon unavailable: {exc}'
    finally:
        socket.close()


def run(endpoint):
    context = zmq.Context()
    try:
        curses.wrapper(monitor, context, endpoint)
    finally:
        context.term()


def monitor(screen, context, endpoint):
    curses.curs_set(0)
    screen.keypad(True)
    screen.timeout(200)
    try:
        curses.mousemask(curses.BUTTON1_CLICKED)
    except curses.error:
        pass
    jobs, selected, selected_id, page = [], 0, None, 'jobs'
    output, offset, error = '', 0, None
    decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
    refresh_at = 0.0

    def start_output():
        nonlocal page, selected_id, output, offset, decoder, refresh_at
        if jobs:
            selected_id = jobs[selected]['job_id']
            page, output, offset = 'output', '', 0
            decoder = codecs.getincrementaldecoder('utf-8')(errors='replace')
            refresh_at = 0

    while True:
        if time.monotonic() >= refresh_at:
            old_id = jobs[selected]['job_id'] if jobs and selected < len(jobs) else None
            reply, error = request(context, endpoint, {'action': 'jobs'})
            if reply is not None:
                jobs = reply['jobs']
                selected = next((i for i, job in enumerate(jobs) if job['job_id'] == old_id), min(selected, max(0, len(jobs)-1)))
                if page == 'output':
                    job = next((job for job in jobs if job['job_id'] == selected_id), None)
                    if job and job['status'] != 'waiting':
                        chunk, output_error = request(context, endpoint, {'action': 'stream', 'job_id': selected_id, 'offset': offset})
                        if chunk is not None:
                            try:
                                data = base64.b64decode(chunk['data'], validate=True)
                                offset += len(data)
                                output = (output + decoder.decode(data))[-200000:]
                            except (ValueError, KeyError):
                                error = 'Invalid output from daemon'
                        else:
                            error = output_error
            refresh_at = time.monotonic() + 1
        screen.erase()
        height, width = screen.getmaxyx()
        def line(y, text, attr=0):
            if 0 <= y < height and width > 1:
                text = ''.join(c if c.isprintable() else ' ' for c in str(text))
                try:
                    screen.addnstr(y, 0, text, width - 1, attr)
                except curses.error:
                    pass
        line(0, 'Fire and Forget  |  ' + endpoint, curses.A_BOLD)
        if page == 'jobs':
            line(1, 'ID       STATUS      COMMAND', curses.A_BOLD)
            visible = max(0, height - 4)
            start = max(0, selected - visible + 1)
            for index in range(start, min(len(jobs), start + visible)):
                job = jobs[index]
                line(index-start+2, f"{job['job_id']:<8} {job['status']:<11} {job['command']}", curses.A_REVERSE if index == selected else 0)
            if not jobs and error is None:
                line(2, 'No jobs yet.')
            elif not jobs:
                line(2, 'Could not load jobs from the daemon.')
            line(height-2, error or 'Up/Down: select  Enter/click: output  q: quit')
        else:
            job = next((job for job in jobs if job['job_id'] == selected_id), None)
            line(1, f"Job {selected_id}  |  {job['status'] if job else 'unknown'}  |  {job['command'] if job else ''}")
            shown = output.splitlines()[-max(0, height-4):] if height > 4 else []
            for i, text in enumerate(shown):
                line(i+2, text)
            line(height-2, error or ('Waiting for a worker' if job and job['status'] == 'waiting' else 'Live output  |  Esc/backspace: jobs  q: quit'))
        screen.refresh()
        key = screen.getch()
        if key in (ord('q'), ord('Q')):
            return
        if page == 'output':
            if key in (27, curses.KEY_BACKSPACE, 127):
                page = 'jobs'
                error = None
        elif key in (curses.KEY_UP, ord('k')):
            selected = max(0, selected-1)
        elif key in (curses.KEY_DOWN, ord('j')):
            selected = min(max(0, len(jobs)-1), selected+1)
        elif key in (10, 13, curses.KEY_ENTER):
            start_output()
        elif key == curses.KEY_MOUSE:
            try:
                _, _, y, _, state = curses.getmouse()
                index = start + y - 2
                if state & curses.BUTTON1_CLICKED and 0 <= index < len(jobs) and 2 <= y < 2 + visible:
                    selected = index
                    start_output()
            except curses.error:
                pass
