"""Crash test: SIGKILL an app replica, then SIGKILL Postgres, while 8 writers and a live
reader are running. Run from the host against the replicas setup:

  docker compose -f docker-compose.yml -f docker-compose.replicas.yml up -d --build --wait
  uv run --python 3.12 --with httpx --with 'psycopg[binary]' scripts/crash_test.py

Each batch writes its own metric key with n points, so afterwards every key must hold
exactly 0 or n points (no partial batch), every acknowledged batch must hold n, seq must be
1..last_seq with no gaps, and the live reader must have received exactly that sequence.
"""
import json
import random
import subprocess
import threading
import time
import uuid
from collections import Counter

import httpx
import psycopg

BASE = "http://localhost:8000"
DB = "postgresql://soup:soup@localhost:5432/soup"
errors: Counter = Counter()
lock = threading.Lock()


def log(msg):
    print(f"{time.strftime('%H:%M:%S')} {msg}", flush=True)


def docker(*args):
    subprocess.run(["docker", *args], check=True, capture_output=True)


def wait_healthy(url, timeout=60):
    t0 = time.monotonic()
    while time.monotonic() - t0 < timeout:
        try:
            if httpx.get(url, timeout=2).status_code == 200:
                return time.monotonic() - t0
        except httpx.HTTPError:
            pass
        time.sleep(0.2)
    raise RuntimeError("not healthy")


def main():
    wait_healthy(f"{BASE}/healthz")
    d = httpx.post(f"{BASE}/admin/projects", json={"name": f"crash-{uuid.uuid4().hex[:6]}"},
                   headers={"x-admin-token": "dev-admin-token"}).json()
    h = {"authorization": f"Bearer {d['api_key']}"}
    run = httpx.post(f"{BASE}/runs", json={"name": "crash"}, headers=h).json()["id"]
    stop = threading.Event()
    acked: dict[str, int] = {}
    attempted: dict[str, int] = {}
    seqs: list[int] = []

    def writer(w):
        rnd = random.Random(w)
        i = 0
        with httpx.Client(timeout=5) as c:
            while not stop.is_set():
                tag, n = f"w{w}b{i}", rnd.randint(5, 50)
                attempted[tag] = n
                body = {"points": [{"key": tag, "step": s, "value": float(s)} for s in range(n)]}
                while True:  # retry the identical batch until acknowledged
                    try:
                        r = c.post(f"{BASE}/runs/{run}/metrics", json=body, headers=h)
                        if r.status_code == 200:
                            break
                        with lock:
                            errors[f"http_{r.status_code}"] += 1
                    except httpx.HTTPError as e:
                        with lock:
                            errors[type(e).__name__] += 1
                    time.sleep(0.2)
                acked[tag] = n
                i += 1
                time.sleep(rnd.uniform(0.01, 0.05))

    def reader():
        last = 0
        while not stop.is_set() or last < final[0]:
            try:
                with httpx.Client(timeout=httpx.Timeout(5, read=10)) as c, \
                        c.stream("GET", f"{BASE}/runs/{run}/stream", headers={**h, "last-event-id": str(last)}) as r:
                    ev = None
                    for line in r.iter_lines():
                        if line.startswith("event:"):
                            ev = line[7:]
                        elif line.startswith("data:") and ev == "metric":
                            seqs.append(json.loads(line[6:])["seq"])
                            last = seqs[-1]
                        if stop.is_set() and last >= final[0]:
                            return
            except httpx.HTTPError as e:
                with lock:
                    errors[f"reader_{type(e).__name__}"] += 1
                time.sleep(0.3)

    final = [10**12]
    ws = [threading.Thread(target=writer, args=(w,)) for w in range(8)]
    rd = threading.Thread(target=reader)
    for t in [*ws, rd]:
        t.start()

    time.sleep(4)
    log(f"SIGKILL app-1 (acked batches so far: {len(acked)})")
    docker("kill", "-s", "KILL", "soup_tz-app-1")
    time.sleep(4)
    docker("start", "soup_tz-app-1")
    log(f"app-1 restarted (acked: {len(acked)})")
    time.sleep(4)
    log(f"SIGKILL postgres (acked: {len(acked)})")
    docker("kill", "-s", "KILL", "soup_tz-db-1")
    time.sleep(3)
    docker("start", "soup_tz-db-1")
    log("postgres restarted")
    t_rec = time.monotonic()
    n0 = len(acked)
    while len(acked) == n0:
        time.sleep(0.1)
    log(f"first batch acked {time.monotonic() - t_rec:.1f}s after postgres start")
    time.sleep(5)
    stop.set()
    for t in ws:
        t.join()
    with psycopg.connect(DB) as db:
        final[0] = db.execute("SELECT last_seq FROM runs WHERE id=%s", (run,)).fetchone()[0]
        rows = db.execute("SELECT seq, key FROM metrics WHERE run_id=%s ORDER BY seq", (run,)).fetchall()
    rd.join(timeout=60)

    per_key = Counter(k for _, k in rows)
    partial = {k: (per_key.get(k, 0), n) for k, n in attempted.items() if per_key.get(k, 0) not in (0, n)}
    lost_acked = {k: n for k, n in acked.items() if per_key.get(k, 0) != n}
    res = {
        "batches_attempted": len(attempted), "batches_acked": len(acked), "rows": len(rows), "last_seq": final[0],
        "partial_batches": len(partial), "acked_but_missing": len(lost_acked),
        "seq_gap_free": [s for s, _ in rows] == list(range(1, final[0] + 1)),
        "reader_events": len(seqs), "reader_exact": seqs == list(range(1, final[0] + 1)),
        "reader_unique_exact": sorted(set(seqs)) == list(range(1, final[0] + 1)),
        "client_errors_seen": dict(errors),
    }
    print(json.dumps(res, indent=2))
    # a DB outage must surface as retryable 503, never as 500 (see main.py DB_UNAVAILABLE)
    ok = (not partial and not lost_acked and res["seq_gap_free"] and res["reader_exact"]
          and "http_500" not in errors)
    print("CRASH TEST:", "PASS" if ok else "FAIL")
    raise SystemExit(0 if ok else 1)


if __name__ == "__main__":
    main()
