import json
import os
import threading
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import psycopg
import pytest

BASE = os.environ.get("BASE_URL", "http://localhost:8000")
DB = os.environ.get("DATABASE_URL", "postgresql://soup:soup@localhost:5432/soup")
ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")
RESULTS = Path(os.environ.get("RESULTS_DIR", "/results" if Path("/results").exists() else "results"))
RESULTS.mkdir(exist_ok=True)
_KEYS_FILE = RESULTS / "keys_issued.txt"
_keys_lock = threading.Lock()


def record_key(k: str) -> None:
    """Every raw key a test sees is recorded; scripts/check_logs.sh greps app logs for them."""
    with _keys_lock, _KEYS_FILE.open("a") as f:
        f.write(k + "\n")


@dataclass
class Project:
    id: str
    key: str
    http: httpx.Client = field(repr=False)

    def h(self, key: str | None = None) -> dict:
        return {"authorization": f"Bearer {key or self.key}"}

    def new_run(self, name: str = "run") -> str:
        r = self.http.post(f"{BASE}/runs", json={"name": name}, headers=self.h())
        assert r.status_code == 201, r.text
        return r.json()["id"]

    def post(self, run_id: str, points: list[dict], **kw) -> httpx.Response:
        return self.http.post(f"{BASE}/runs/{run_id}/metrics", json={"points": points},
                              headers={**self.h(), **kw.pop("headers", {})}, **kw)

    def rotate(self, grace: int = 0) -> str:
        r = self.http.post(f"{BASE}/keys/rotate", json={"grace_seconds": grace}, headers=self.h())
        assert r.status_code == 200, r.text
        new = r.json()["api_key"]
        record_key(new)
        return new


@pytest.fixture(scope="session")
def http():
    with httpx.Client(timeout=30) as c:
        yield c


@pytest.fixture
def make_project(http):
    def _make(name: str = "p") -> Project:
        r = http.post(f"{BASE}/admin/projects", json={"name": f"{name}-{uuid.uuid4().hex[:6]}"},
                      headers={"x-admin-token": ADMIN})
        assert r.status_code == 201, r.text
        d = r.json()
        record_key(d["api_key"])
        return Project(d["project_id"], d["api_key"], http)
    return _make


@pytest.fixture
def db():
    with psycopg.connect(DB, autocommit=True) as c:
        yield c


def db_points(db, run_id: str) -> list[tuple]:
    return db.execute("SELECT seq, key, step, value FROM metrics WHERE run_id = %s ORDER BY seq", (run_id,)).fetchall()


class SSEReader:
    """Minimal SSE client over httpx. Records every metric event; can be told to drop."""

    def __init__(self, url: str, headers: dict, after: int = 0):
        self.url, self.headers = url, headers
        self.events: list[dict] = []
        self.last_id = after
        self.status: int | None = None
        self.control: list[str] = []   # non-metric events, e.g. "revoked"
        self.ended = threading.Event()

    def read_once(self, max_events: int | None = None, timeout: float = 30, slow: float = 0.0) -> None:
        """One connection: read until server ends, `max_events` received, or timeout."""
        try:
            self._read(max_events, timeout, slow)
        except httpx.ReadTimeout:
            pass

    def _read(self, max_events, timeout, slow) -> None:
        h = {**self.headers, "last-event-id": str(self.last_id)}
        got = 0
        deadline = time.monotonic() + timeout
        with httpx.Client(timeout=httpx.Timeout(10, read=timeout)) as c, c.stream("GET", self.url, headers=h) as r:
            self.status = r.status_code
            if r.status_code != 200:
                return
            ev, data, eid = None, None, None
            for line in r.iter_lines():
                if line == "":
                    if ev == "metric":
                        d = json.loads(data)
                        self.events.append(d)
                        self.last_id = int(eid)
                        got += 1
                        if slow:
                            time.sleep(slow)
                        if max_events and got >= max_events:
                            return
                    elif ev and ev != "ping":
                        self.control.append(ev)
                    ev = data = eid = None
                    if time.monotonic() > deadline:
                        return
                    continue
                if line.startswith(":"):
                    continue
                k, _, v = line.partition(":")
                v = v[1:] if v.startswith(" ") else v
                if k == "event":
                    ev = v
                elif k == "data":
                    data = v
                elif k == "id":
                    eid = v
        self.ended.set()
