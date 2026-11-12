"""Failures that would not raise an error anywhere unless you look for them."""
import random
import threading
import time
from concurrent.futures import ThreadPoolExecutor

import httpx

from conftest import BASE, SSEReader, db_points


def pts(key, steps, f=lambda s: s * 0.5):
    return [{"key": key, "step": s, "value": float(f(s))} for s in steps]


# ------------------------------------------------------------------ duplicates

def test_same_batch_sent_concurrently_is_stored_once(make_project, db):
    a = make_project()
    run = a.new_run()
    batch = pts("loss", range(100))
    with ThreadPoolExecutor(10) as ex:
        res = list(ex.map(lambda _: a.post(run, batch).json(), range(10)))
    assert sum(r["accepted"] for r in res) == 100
    assert sum(r["duplicates"] for r in res) == 900
    rows = db_points(db, run)
    assert [r[0] for r in rows] == list(range(1, 101))  # gap-free seq
    assert len({(r[1], r[2]) for r in rows}) == 100


def test_conflicting_resend_keeps_first_value_and_reports_it(make_project, db):
    a = make_project()
    run = a.new_run()
    a.post(run, [{"key": "loss", "step": 1, "value": 1.0}])
    r = a.post(run, [{"key": "loss", "step": 1, "value": 2.0}, {"key": "loss", "step": 1, "value": 3.0}]).json()
    assert r == {"accepted": 0, "duplicates": 2, "conflicts": 2, "last_seq": 1}
    assert [p[3] for p in db_points(db, run)] == [1.0]


# ------------------------------------------------------------------ partial writes

def test_invalid_point_rejects_whole_batch(make_project, db):
    a = make_project()
    run = a.new_run()
    bad = pts("loss", range(10)) + [{"key": "loss", "step": 99, "value": "nan"}]
    assert a.post(run, bad).status_code == 422
    assert db_points(db, run) == []


def test_failure_mid_write_rolls_back_everything_and_leaves_no_seq_gap(make_project, db):
    a = make_project()
    run = a.new_run()
    r = a.post(run, pts("loss", range(50)), headers={"x-test-fault": "after_insert"})
    assert r.status_code == 500
    assert db_points(db, run) == []
    assert db.execute("SELECT last_seq FROM runs WHERE id=%s", (run,)).fetchone()[0] == 0
    # client retries the same batch: succeeds, seq starts at 1
    r = a.post(run, pts("loss", range(50))).json()
    assert r["accepted"] == 50 and [p[0] for p in db_points(db, run)] == list(range(1, 51))


def test_oversized_batch_rejected_without_writing(make_project, db):
    a = make_project()
    run = a.new_run()
    assert a.post(run, pts("loss", range(1001))).status_code == 413
    assert db_points(db, run) == []


# ------------------------------------------------------------------ lost events

def test_no_event_lost_with_concurrent_writers_and_reconnecting_reader(make_project, db):
    """The commit-order race: concurrent writers + a reader that keeps dropping and
    resuming from Last-Event-ID. Every committed point must reach the reader exactly once."""
    a = make_project()
    run = a.new_run()
    stop = threading.Event()
    reader = SSEReader(f"{BASE}/runs/{run}/stream", a.h())
    reconnects = 0

    def read_loop():
        nonlocal reconnects
        while not stop.is_set() or reader.last_id < final_seq[0]:
            reader.read_once(max_events=random.randint(5, 60), timeout=5)
            reconnects += 1

    final_seq = [10**9]
    t = threading.Thread(target=read_loop)
    t.start()

    def writer(w):
        rnd = random.Random(w)
        steps = list(range(w * 1000, w * 1000 + 300))
        rnd.shuffle(steps)
        i = 0
        while i < len(steps):
            n = rnd.randint(1, 25)
            chunk = steps[i:i + n]
            assert a.post(run, pts(f"m{w % 3}", chunk)).status_code == 200
            if rnd.random() < 0.3:  # retry storm: resend the same chunk
                assert a.post(run, pts(f"m{w % 3}", chunk)).status_code == 200
            i += n

    with ThreadPoolExecutor(8) as ex:
        list(ex.map(writer, range(8)))
    final_seq[0] = db.execute("SELECT last_seq FROM runs WHERE id=%s", (run,)).fetchone()[0]
    stop.set()
    t.join(timeout=60)
    rows = db_points(db, run)
    seen = [e["seq"] for e in reader.events]
    print(f"\nRACE: db rows={len(rows)} reader events={len(seen)} reconnects={reconnects}")
    assert len(rows) == 8 * 300
    assert seen == list(range(1, len(rows) + 1)), "gap or duplicate in delivered seqs"
    assert [(e["seq"], e["key"], e["step"], e["value"]) for e in reader.events] == rows


def test_stream_survives_listener_connection_loss(make_project, db):
    """Kill the LISTEN connection in Postgres. NOTIFYs during the outage are lost;
    the poll fallback must still deliver, and the listener must come back."""
    a = make_project()
    run = a.new_run()
    before = httpx.get(f"{BASE}/healthz").json()["listener_connects"]
    killed = db.execute("SELECT count(pg_terminate_backend(pid)) FROM pg_stat_activity "
                        "WHERE application_name = 'soup-listener'").fetchone()[0]
    assert killed >= 1
    reader = SSEReader(f"{BASE}/runs/{run}/stream", a.h())
    t = threading.Thread(target=reader.read_once, kwargs={"max_events": 1, "timeout": 10})
    t.start()
    time.sleep(0.2)
    t0 = time.monotonic()
    a.post(run, pts("loss", [1]))
    t.join()
    latency = time.monotonic() - t0
    print(f"\nLISTENER KILLED: delivered after {latency:.2f}s")
    assert [e["step"] for e in reader.events] == [1]
    assert latency < 3.0
    deadline = time.monotonic() + 5
    while httpx.get(f"{BASE}/healthz").json()["listener_connects"] <= before:
        assert time.monotonic() < deadline, "listener did not reconnect"
        time.sleep(0.2)


# ------------------------------------------------------------------ slow client

def test_slow_client_gets_everything_and_does_not_slow_writers(make_project, db):
    a = make_project()
    run_slow, run_other = a.new_run("slow"), a.new_run("other")
    for i in range(10):
        a.post(run_slow, pts("loss", range(i * 1000, i * 1000 + 1000)))
    slow = SSEReader(f"{BASE}/runs/{run_slow}/stream", a.h())
    t = threading.Thread(target=slow.read_once, kwargs={"timeout": 120, "slow": 0.0005})
    t.start()
    lat = []
    for i in range(100):
        t0 = time.perf_counter()
        assert a.post(run_other, pts("x", [i])).status_code == 200
        lat.append((time.perf_counter() - t0) * 1000)
    lat.sort()
    deadline = time.monotonic() + 120
    while slow.last_id < 10_000 and time.monotonic() < deadline:
        time.sleep(0.2)
    print(f"\nSLOW CLIENT: got {len(slow.events)}/10000; other-run write p50={lat[50]:.1f}ms p99={lat[98]:.1f}ms")
    assert [e["seq"] for e in slow.events[:10_000]] == list(range(1, 10_001))
    assert lat[98] < 200


# ------------------------------------------------------------------ key rotation

def test_rotated_key_stops_working_everywhere(make_project):
    a = make_project()
    run = a.new_run()
    old = a.key
    browser = httpx.Client(base_url=BASE)
    assert browser.post("/session", json={"api_key": old}).status_code == 200

    reader = SSEReader(f"{BASE}/runs/{run}/stream", a.h(old))
    t = threading.Thread(target=reader.read_once, kwargs={"timeout": 15})
    t.start()
    time.sleep(0.5)

    new = a.rotate()
    t_rot = time.monotonic()
    # REST: immediately
    assert a.http.get(f"{BASE}/runs", headers=a.h(old)).status_code == 401
    assert a.post(run, pts("x", [1]), headers=a.h(old)).status_code == 401
    assert a.http.get(f"{BASE}/runs", headers=a.h(new)).status_code == 200
    # browser cookie holding the old key
    assert browser.get("/runs").status_code == 401
    # an already-open stream: closed by the server within one poll tick
    t.join(timeout=10)
    closed_after = time.monotonic() - t_rot
    print(f"\nROTATION: open stream closed {closed_after:.2f}s after rotation, control={reader.control}")
    assert not t.is_alive() and reader.control == ["revoked"]
    assert closed_after < 3.0
    # new stream with old key
    r2 = SSEReader(f"{BASE}/runs/{run}/stream", a.h(old))
    r2.read_once(timeout=2)
    assert r2.status == 401
    browser.close()


def test_rotation_with_grace_period_expires(make_project):
    a = make_project()
    old = a.key
    a.rotate(grace=2)
    assert a.http.get(f"{BASE}/runs", headers=a.h(old)).status_code == 200
    time.sleep(2.5)
    assert a.http.get(f"{BASE}/runs", headers=a.h(old)).status_code == 401
