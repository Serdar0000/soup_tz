"""Project A must not read, write, subscribe to, or discover anything of project B."""
import statistics
import threading
import time
import uuid

import httpx

from conftest import BASE, SSEReader, db_points


def _strip(r: httpx.Response) -> tuple:
    """Everything an attacker can observe, minus the per-request id."""
    hdrs = {k: v for k, v in r.headers.items() if k not in ("x-request-id", "date", "content-length")}
    return r.status_code, r.content, tuple(sorted(hdrs.items()))


def test_run_list_shows_only_own_runs(make_project):
    a, b = make_project("A"), make_project("B")
    ra = {a.new_run(f"a{i}") for i in range(3)}
    rb = {b.new_run(f"b{i}") for i in range(2)}
    got_a = {r["id"] for r in a.http.get(f"{BASE}/runs", headers=a.h()).json()["runs"]}
    got_b = {r["id"] for r in b.http.get(f"{BASE}/runs", headers=b.h()).json()["runs"]}
    assert got_a == ra and got_b == rb


def test_cannot_read_foreign_run_and_404_is_indistinguishable(make_project):
    a, b = make_project("A"), make_project("B")
    run_a = a.new_run()
    assert a.post(run_a, [{"key": "loss", "step": 1, "value": 1.0}]).status_code == 200
    fake = str(uuid.uuid4())
    for path in ("", "/metrics"):
        foreign = b.http.get(f"{BASE}/runs/{run_a}{path}", headers=b.h())
        missing = b.http.get(f"{BASE}/runs/{fake}{path}", headers=b.h())
        malformed = b.http.get(f"{BASE}/runs/not-a-uuid{path}", headers=b.h())
        assert foreign.status_code == 404
        assert _strip(foreign) == _strip(missing) == _strip(malformed)
        assert b"loss" not in foreign.content


def test_cannot_write_foreign_run(make_project, db):
    a, b = make_project("A"), make_project("B")
    run_a = a.new_run()
    a.post(run_a, [{"key": "loss", "step": 1, "value": 1.0}])
    before = db_points(db, run_a)
    r = b.post(run_a, [{"key": "loss", "step": 2, "value": 666.0}])
    missing = b.post(str(uuid.uuid4()), [{"key": "loss", "step": 2, "value": 666.0}])
    assert r.status_code == 404 and _strip(r) == _strip(missing)
    assert db_points(db, run_a) == before
    assert db.execute("SELECT last_seq FROM runs WHERE id=%s", (run_a,)).fetchone()[0] == 1


def test_cannot_subscribe_to_foreign_stream_header_or_cookie(make_project):
    a, b = make_project("A"), make_project("B")
    run_a = a.new_run()
    a.post(run_a, [{"key": "loss", "step": 1, "value": 1.0}])
    # header auth
    rd = SSEReader(f"{BASE}/runs/{run_a}/stream", b.h())
    rd.read_once(timeout=3)
    assert rd.status == 404 and rd.events == []
    # cookie auth (what the browser uses)
    with httpx.Client(base_url=BASE) as browser_b:
        assert browser_b.post("/session", json={"api_key": b.key}).status_code == 200
        with browser_b.stream("GET", f"/runs/{run_a}/stream") as r:
            assert r.status_code == 404
            assert b"loss" not in r.read()
        assert browser_b.get(f"/runs/{run_a}/metrics").status_code == 404
        # cookie must not authorize writes at all (CSRF)
        w = browser_b.post(f"/runs/{run_a}/metrics", json={"points": [{"key": "x", "step": 1, "value": 1}]})
        assert w.status_code == 401


def test_cache_does_not_leak_between_projects(make_project):
    """A warms the run cache; B's identical request right after must still be a 404."""
    a, b = make_project("A"), make_project("B")
    run_a = a.new_run("secret-name")
    for _ in range(3):
        assert a.http.get(f"{BASE}/runs/{run_a}", headers=a.h()).status_code == 200
    r = b.http.get(f"{BASE}/runs/{run_a}", headers=b.h())
    assert r.status_code == 404 and b"secret-name" not in r.content
    rd = SSEReader(f"{BASE}/runs/{run_a}/stream", b.h())
    rd.read_once(timeout=3)
    assert rd.status == 404
    # HTTP caches: responses must not be storable or shareable across credentials
    ok = a.http.get(f"{BASE}/runs/{run_a}", headers=a.h())
    assert ok.headers["cache-control"] == "no-store"
    assert "Authorization" in ok.headers["vary"] and "Cookie" in ok.headers["vary"]


def test_live_stream_never_carries_other_projects_events(make_project):
    a, b = make_project("A"), make_project("B")
    run_a, run_b = a.new_run(), b.new_run()
    rd = SSEReader(f"{BASE}/runs/{run_a}/stream", a.h())
    t = threading.Thread(target=rd.read_once, kwargs={"timeout": 5})
    t.start()
    time.sleep(0.5)
    for i in range(20):
        b.post(run_b, [{"key": "b_secret", "step": i, "value": float(i)}])
    a.post(run_a, [{"key": "a_metric", "step": 0, "value": 1.0}])
    t.join()
    assert [e["key"] for e in rd.events] == ["a_metric"]


def test_auth_required_and_bad_keys_rejected(make_project):
    a = make_project("A")
    run_a = a.new_run()
    for h in ({}, {"authorization": "Bearer soup_nope"}, {"authorization": "Basic xxx"}):
        assert a.http.get(f"{BASE}/runs/{run_a}", headers=h).status_code == 401
        assert a.http.get(f"{BASE}/runs", headers=h).status_code == 401


def test_ids_are_random_uuid4(make_project):
    a = make_project("A")
    ids = [uuid.UUID(a.new_run()) for _ in range(50)]
    assert all(i.version == 4 for i in ids)
    # no shared prefix / no ordering: neighbouring ids differ in many bits
    diffs = [bin(x.int ^ y.int).count("1") for x, y in zip(ids, ids[1:])]
    assert min(diffs) > 30, diffs


def test_timing_foreign_vs_missing(make_project):
    """Measurement, not proof: both paths run the same scoped query. Reported in the note."""
    a, b = make_project("A"), make_project("B")
    run_a = a.new_run()
    fake = str(uuid.uuid4())
    t_foreign, t_missing = [], []
    for i in range(300):
        for target, bucket in ((run_a, t_foreign), (fake, t_missing))[:: 1 if i % 2 else -1]:
            t0 = time.perf_counter()
            b.http.get(f"{BASE}/runs/{target}/metrics", headers=b.h())
            bucket.append((time.perf_counter() - t0) * 1000)
    mf, mm = statistics.median(t_foreign), statistics.median(t_missing)
    print(f"\nTIMING median foreign={mf:.3f}ms missing={mm:.3f}ms diff={mf - mm:+.3f}ms (n=300 each)")
    assert abs(mf - mm) < 1.0


def test_server_errors_do_not_leak_internals(make_project):
    a = make_project("A")
    run_a = a.new_run()
    r = a.post(run_a, [{"key": "k", "step": 1, "value": 1.0}], headers={"x-test-fault": "after_insert"})
    assert r.status_code == 500
    body = r.json()
    assert set(body) == {"detail", "request_id"} and body["detail"] == "internal error"
    for leak in ("Injected", "INSERT", "Traceback", "asyncpg", "metrics"):
        assert leak not in r.text


def test_validation_errors_do_not_echo_values(make_project):
    a = make_project("A")
    run_a = a.new_run()
    r = a.post(run_a, [{"key": "k", "step": -1, "value": "secret-payload"}])
    assert r.status_code == 422 and "secret-payload" not in r.text
