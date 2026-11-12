"""Live stream: Postgres LISTEN/NOTIFY as a wake-up hint, the metrics table as the source.

There is no per-subscriber event queue. Each subscriber owns a cursor (last seq sent)
and re-reads `seq > cursor` from Postgres whenever it is woken up or every POLL_S.
So a slow client cannot overflow anything, and a lost NOTIFY only adds latency.
"""
import asyncio
import json
import logging
import time
import uuid
from collections import defaultdict

import asyncpg

from . import auth
from .db import DATABASE_URL

log = logging.getLogger("soup.stream")

POLL_S = 2.0
# A real event (not an SSE comment) so the page can run a liveness watchdog on it.
PING_S = 5.0
PAGE = 500
CHANNEL = "run_events"
# test-only control messages, broadcast to every app replica through Postgres
CONTROL_CHANNEL = "soup_control"


class Hub:
    def __init__(self) -> None:
        self._subs: dict[uuid.UUID, set[asyncio.Event]] = defaultdict(set)
        self._task: asyncio.Task | None = None
        self.listener_connects = 0
        # bumping this makes every open stream end (test hook for "connection drops")
        self.generation = 0

    def subscribe(self, run_id: uuid.UUID) -> asyncio.Event:
        ev = asyncio.Event()
        self._subs[run_id].add(ev)
        return ev

    def unsubscribe(self, run_id: uuid.UUID, ev: asyncio.Event) -> None:
        s = self._subs.get(run_id)
        if s is not None:
            s.discard(ev)
            if not s:
                del self._subs[run_id]

    def subscriber_count(self) -> int:
        return sum(len(s) for s in self._subs.values())

    def _wake(self, run_id: uuid.UUID | None = None) -> None:
        targets = self._subs.values() if run_id is None else [self._subs.get(run_id, ())]
        for s in targets:
            for ev in s:
                ev.set()

    def _on_notify(self, _conn, _pid, _channel, payload: str) -> None:
        try:
            self._wake(uuid.UUID(payload))
        except ValueError:
            log.warning("bad notify payload")

    def _on_control(self, _conn, _pid, _channel, payload: str) -> None:
        if payload == "drop-streams":
            self.generation += 1
            self._wake()

    async def start(self) -> None:
        self._task = asyncio.create_task(self._listen_forever())

    async def stop(self) -> None:
        if self._task:
            self._task.cancel()

    async def _listen_forever(self) -> None:
        while True:
            conn = None
            try:
                conn = await asyncpg.connect(DATABASE_URL, server_settings={"application_name": "soup-listener"})
                await conn.add_listener(CHANNEL, self._on_notify)
                await conn.add_listener(CONTROL_CHANNEL, self._on_control)
                self.listener_connects += 1
                log.info("listener connected (#%d)", self.listener_connects)
                # anything committed while we were disconnected: wake everyone to re-read
                self._wake()
                while not conn.is_closed():
                    await asyncio.sleep(1.0)
                    await conn.execute("SELECT 1")  # detects a dead socket
            except asyncio.CancelledError:
                if conn is not None:
                    await conn.close()
                raise
            except Exception as e:  # noqa: BLE001 - keep the listener alive no matter what
                log.warning("listener lost: %s; reconnecting", type(e).__name__)
            finally:
                if conn is not None and not conn.is_closed():
                    conn.terminate()
            await asyncio.sleep(0.5)


def _sse(seq: int, row: asyncpg.Record) -> str:
    data = {
        "seq": seq, "key": row["key"], "step": row["step"], "value": row["value"],
        "ts": row["ts"].isoformat() if row["ts"] else None,
    }
    return f"id: {seq}\nevent: metric\ndata: {json.dumps(data, allow_nan=False)}\n\n"


async def event_stream(pool: asyncpg.Pool, hub: Hub, principal: auth.Principal,
                       run_id: uuid.UUID, after: int, is_disconnected):
    ev = hub.subscribe(run_id)
    gen = hub.generation
    cursor = after
    last_ping = time.monotonic()
    try:
        yield "retry: 1000\n\n"
        while True:
            # clear BEFORE reading, so a notify that lands during the read is not lost
            ev.clear()
            rows = await pool.fetch(
                "SELECT seq, key, step, value, ts FROM metrics WHERE run_id = $1 AND seq > $2 ORDER BY seq LIMIT $3",
                run_id, cursor, PAGE,
            )
            if rows:
                yield "".join(_sse(r["seq"], r) for r in rows)
                cursor = rows[-1]["seq"]
                if len(rows) == PAGE:
                    continue
            try:
                await asyncio.wait_for(ev.wait(), timeout=POLL_S)
            except asyncio.TimeoutError:
                pass
            if hub.generation != gen or await is_disconnected():
                return
            # a long-lived stream must not outlive its key
            if not await auth.key_still_valid(pool, principal.key_id):
                log.info("stream closed: key %s revoked", principal.prefix)
                yield "event: revoked\ndata: {}\n\n"
                return
            now = time.monotonic()
            if now - last_ping > PING_S:
                last_ping = now
                yield "event: ping\ndata: {}\n\n"
    finally:
        hub.unsubscribe(run_id, ev)
