#!/usr/bin/env python3
"""
circuit-heal — autoheal replacement with backoff circuit breaker.

Per-container state machine:
  NORMAL  : restart every INTERVAL; after FAIL_THRESHOLD failures → BACKOFF
  BACKOFF : wait BACKOFF_DURATION seconds; on expiry → RESUMED
  RESUMED : restart every INTERVAL forever (no ceiling)

  Any state + container becomes healthy → reset to NORMAL
"""

import docker
import json
import logging
import os
import signal
import sys
import time
import urllib.request
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum

# ── Config ───────────────────────────────────────────────────────────────────
INTERVAL         = int(os.getenv('AUTOHEAL_INTERVAL',             '60'))
START_PERIOD     = int(os.getenv('AUTOHEAL_START_PERIOD',         '300'))
STOP_TIMEOUT     = int(os.getenv('AUTOHEAL_DEFAULT_STOP_TIMEOUT', '10'))
CONTAINER_LABEL  = os.getenv('AUTOHEAL_CONTAINER_LABEL',          'autoheal')
FAIL_THRESHOLD   = int(os.getenv('AUTOHEAL_FAIL_THRESHOLD',       '3'))
BACKOFF_DURATION = int(os.getenv('AUTOHEAL_BACKOFF_DURATION',     '1800'))
PUSHOVER_TOKEN   = os.getenv('PUSHOVER_TOKEN', '')
PUSHOVER_USER    = os.getenv('PUSHOVER_USER',  '')
WEBHOOK_URL      = os.getenv('WEBHOOK_URL',    '')

# ── Logging ──────────────────────────────────────────────────────────────────
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s %(levelname)-5s %(message)s',
    datefmt='%Y-%m-%dT%H:%M:%SZ',
    stream=sys.stdout,
)
log = logging.getLogger('circuit-heal')

# ── State ────────────────────────────────────────────────────────────────────
class Phase(str, Enum):
    NORMAL  = 'normal'
    BACKOFF = 'backoff'
    RESUMED = 'resumed'

@dataclass
class State:
    name:           str
    phase:          Phase = Phase.NORMAL
    fail_count:     int   = 0
    backoff_until:  float = 0.0
    total_restarts: int   = 0

_states:   dict[str, State] = {}
_shutdown: bool = False

# ── Signal handling ───────────────────────────────────────────────────────────
def _on_signal(signum, _frame):
    global _shutdown
    log.info('signal %d received — shutting down cleanly', signum)
    _shutdown = True

signal.signal(signal.SIGTERM, _on_signal)
signal.signal(signal.SIGINT, _on_signal)

# ── Notifications ─────────────────────────────────────────────────────────────
def _post(url: str, payload: dict, timeout: int = 10):
    data = json.dumps(payload).encode()
    req = urllib.request.Request(
        url, data=data,
        headers={'Content-Type': 'application/json'},
        method='POST',
    )
    urllib.request.urlopen(req, timeout=timeout)

def notify(title: str, message: str, priority: int = 0):
    if PUSHOVER_TOKEN and PUSHOVER_USER:
        try:
            _post('https://api.pushover.net/1/messages.json', {
                'token':    PUSHOVER_TOKEN,
                'user':     PUSHOVER_USER,
                'title':    title,
                'message':  message,
                'priority': priority,
            })
        except Exception as exc:
            log.warning('pushover failed: %s', exc)
    if WEBHOOK_URL:
        try:
            _post(WEBHOOK_URL, {'title': title, 'message': message})
        except Exception as exc:
            log.warning('webhook failed: %s', exc)

# ── Helpers ───────────────────────────────────────────────────────────────────
def _health(c) -> str:
    return ((c.attrs.get('State') or {}).get('Health') or {}).get('Status', 'none')

def _in_start_period(c) -> bool:
    started = ((c.attrs.get('State') or {}).get('StartedAt') or '')
    if not started:
        return False
    try:
        t = datetime.fromisoformat(started.replace('Z', '+00:00'))
        return (datetime.now(timezone.utc) - t).total_seconds() < START_PERIOD
    except ValueError:
        return False

def _monitored(c) -> bool:
    if CONTAINER_LABEL == 'all':
        return True
    return (c.labels or {}).get('autoheal') == 'true'

# ── Core loop ─────────────────────────────────────────────────────────────────
def _tick(client):
    now = time.monotonic()

    try:
        containers = [
            c for c in client.containers.list()
            if _monitored(c) and (c.attrs.get('State') or {}).get('Health') is not None
        ]
    except docker.errors.APIError as exc:
        log.warning('docker list error: %s', exc)
        return

    live_ids = {c.id for c in containers}
    for stale in list(_states):
        if stale not in live_ids:
            del _states[stale]

    for c in containers:
        try:
            c.reload()
        except docker.errors.NotFound:
            continue

        status = _health(c)
        state  = _states.setdefault(c.id, State(name=c.name))

        # ── Healthy: reset ────────────────────────────────────────────────────
        if status == 'healthy':
            if state.fail_count > 0 or state.phase != Phase.NORMAL:
                log.info('%s recovered (phase=%s restarts=%d)',
                         c.name, state.phase, state.total_restarts)
                notify(
                    f'circuit-heal: {c.name} recovered',
                    f'Container healthy after {state.total_restarts} restart(s).',
                )
                _states[c.id] = State(name=c.name)
            continue

        if status != 'unhealthy':
            continue  # starting / none

        if _in_start_period(c):
            log.debug('%s unhealthy within start_period — skipping', c.name)
            continue

        # ── Backoff: hold or expire ───────────────────────────────────────────
        if state.phase == Phase.BACKOFF:
            remaining = state.backoff_until - now
            if remaining > 0:
                log.info('%s in backoff — %.0fs remaining', c.name, remaining)
                continue
            log.warning('%s backoff expired — resuming normal retries', c.name)
            notify(
                f'circuit-heal: {c.name} backoff expired',
                f'Resuming normal retries after {BACKOFF_DURATION // 60}m backoff. '
                f'Container may need manual attention.',
                priority=1,
            )
            state.phase = Phase.RESUMED
            state.fail_count = 0

        # ── Restart (NORMAL or RESUMED) ───────────────────────────────────────
        log.info('restarting %s (phase=%s fail=%d)', c.name, state.phase, state.fail_count)
        try:
            c.stop(timeout=STOP_TIMEOUT)
            c.start()
            state.total_restarts += 1
        except Exception as exc:
            log.warning('restart failed for %s: %s', c.name, exc)

        # ── Threshold check (NORMAL only) ─────────────────────────────────────
        if state.phase == Phase.NORMAL:
            state.fail_count += 1
            if state.fail_count >= FAIL_THRESHOLD:
                log.warning(
                    '%s hit fail_threshold (%d) — entering %ds backoff',
                    c.name, FAIL_THRESHOLD, BACKOFF_DURATION,
                )
                notify(
                    f'circuit-heal: {c.name} circuit open',
                    f'{FAIL_THRESHOLD} consecutive failures. '
                    f'Backing off {BACKOFF_DURATION // 60}m to allow system recovery.',
                    priority=1,
                )
                state.phase = Phase.BACKOFF
                state.backoff_until = now + BACKOFF_DURATION


def main():
    client = docker.from_env()
    log.info(
        'started — interval=%ds start_period=%ds fail_threshold=%d '
        'backoff=%ds label=%s',
        INTERVAL, START_PERIOD, FAIL_THRESHOLD, BACKOFF_DURATION, CONTAINER_LABEL,
    )
    while not _shutdown:
        _tick(client)
        deadline = time.monotonic() + INTERVAL
        while not _shutdown and time.monotonic() < deadline:
            time.sleep(1)
    log.info('stopped')
    client.close()


if __name__ == '__main__':
    main()
