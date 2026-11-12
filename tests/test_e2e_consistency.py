"""End-to-end: real browser page vs. Postgres, under out-of-order, duplicated, late and
conflicting batches, flaky client requests, and three different connection drops."""
import json
import queue
import random
import socket
import struct
import threading
import time
from urllib.parse import urlparse

import httpx
from playwright.sync_api import sync_playwright

from conftest import BASE, RESULTS

KEYS = ["acc", "loss", "lr"]
STEPS = 300


def build_plan(rnd: random.Random):
    points = [{"key": k, "step": s, "value": rnd.uniform(-10, 10)} for k in KEYS for s in range(STEPS)]
    rnd.shuffle(points)                                     # out of order within and across batches
    batches, i = [], 0
    while i < len(points):
        n = rnd.randint(1, 8)
        batches.append(points[i:i + n])
        i += n
    rnd.shuffle(batches)
    n_late = len(batches) // 10
    late, main = batches[:n_late], batches[n_late:]         # late: sent after everything else
    plan = list(main)
    for b in rnd.sample(main, len(main) * 3 // 10):         # duplicates, re-sent later
        plan.insert(rnd.randint(plan.index(b) + 1, len(plan)), b)
    conflicts = []
    for b in rnd.sample(main, 5):                           # same (key, step), different value
        c = [{**p, "value": p["value"] + 1000} for p in b[:1]]
        plan.insert(rnd.randint(plan.index(b) + 1, len(plan)), c)
        conflicts.append(c[0])
    return points, plan, late, conflicts


class FlakyClient:
    """Some requests time out client-side after the server may already have applied them;
    the client cannot know, so it retries the identical batch until it gets a 200."""

    def __init__(self, key: str, run: str, rnd: random.Random):
        self.h = {"authorization": f"Bearer {key}"}
        self.url = f"{BASE}/runs/{run}/metrics"
        self.rnd = rnd
        self.stats = {"requests": 0, "client_timeouts": 0, "accepted": 0, "duplicates": 0, "conflicts": 0}
        self.lock = threading.Lock()

    def send(self, batch):
        while True:
            timeout = 0.003 if self.rnd.random() < 0.1 else 10
            with self.lock:
                self.stats["requests"] += 1
            try:
                r = httpx.post(self.url, json={"points": batch}, headers=self.h, timeout=timeout)
            except httpx.TimeoutException:
                with self.lock:
                    self.stats["client_timeouts"] += 1
                continue
            assert r.status_code == 200, r.text
            with self.lock:
                for k in ("accepted", "duplicates", "conflicts"):
                    self.stats[k] += r.json()[k]
            return


class CuttableProxy:
    """TCP proxy between the browser and the app, to break the browser's network for real.
      cut("reset")     every live connection gets a TCP RST; new ones are refused until restore()
      cut("blackhole") bytes on live connections silently vanish (dead Wi-Fi / NAT entry);
                       those connections stay dead forever, new connections work after restore()
    Needed because Playwright's context.set_offline() does NOT break an already-open
    EventSource in Chromium (observed: events kept arriving while "offline")."""

    def __init__(self, target_host: str, target_port: int):
        self.target = (target_host, target_port)
        self.srv = socket.create_server(("127.0.0.1", 0))
        self.port = self.srv.getsockname()[1]
        self.down = False
        self.pairs: list[dict] = []
        self.lock = threading.Lock()
        threading.Thread(target=self._accept, daemon=True).start()

    @staticmethod
    def _rst(s: socket.socket):
        # SO_LINGER=0 + close => RST. shutdown(SHUT_RD) first wakes a thread blocked in
        # recv() on this socket; otherwise close() is deferred and nothing is sent at all
        # (the first version of this proxy had exactly that bug).
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_LINGER, struct.pack("ii", 1, 0))
            s.shutdown(socket.SHUT_RD)
        except OSError:
            pass
        try:
            s.close()
        except OSError:
            pass

    def _accept(self):
        while True:
            c, _ = self.srv.accept()
            if self.down:
                self._rst(c)
                continue
            try:
                u = socket.create_connection(self.target)
            except OSError:
                self._rst(c)
                continue
            pair = {"c": c, "u": u, "black": False}
            with self.lock:
                self.pairs.append(pair)
            for a, b in ((c, u), (u, c)):
                threading.Thread(target=self._pump, args=(a, b, pair), daemon=True).start()

    def _pump(self, src, dst, pair):
        try:
            while data := src.recv(65536):
                if not pair["black"]:
                    dst.sendall(data)
        except OSError:
            pass

    def cut(self, mode: str = "reset"):
        with self.lock:
            pairs = list(self.pairs)
            if mode == "reset":
                self.pairs = []
        if mode == "reset":
            self.down = True
            for p in pairs:
                self._rst(p["c"])
                self._rst(p["u"])
        else:
            for p in pairs:
                p["black"] = True

    def restore(self):
        self.down = False


def page_state(page) -> dict:
    page.evaluate("document.querySelectorAll('details').forEach(d => d.open = true)")
    page.wait_for_timeout(600)  # tables are built on the next render tick after opening
    return page.evaluate("""() => {
        const rows = [];
        for (const box of document.querySelectorAll('section.metric')) {
            for (const tr of box.querySelectorAll('tr.pt')) {
                const tds = tr.querySelectorAll('td');
                rows.push({key: box.dataset.key, step: Number(tr.dataset.step), value: Number(tr.dataset.value),
                           seq: Number(tr.dataset.seq), shown_step: tds[0].innerText, shown_value: tds[1].innerText,
                           shown_seq: tds[2].innerText});
            }
        }
        const t = (id) => document.getElementById(id).innerText;
        return {rows, last_seq: Number(t('last-seq')), points: Number(t('n-points')), gaps: Number(t('gaps')),
                reconnects: Number(t('reconnects')), watchdog: Number(t('watchdog')), dupes: Number(t('dupes')), conn: t('conn'),
                summaries: Object.fromEntries([...document.querySelectorAll('section.metric')].map(b =>
                    [b.dataset.key, b.querySelector('[data-field="points"]').innerText]))};
    }""")


def test_page_matches_database(make_project, db):
    seed = int(time.time())
    rnd = random.Random(seed)
    proj = make_project("e2e")
    run = proj.new_run("e2e-run")
    points, plan, late, conflicts = build_plan(rnd)
    client = FlakyClient(proj.key, run, random.Random(seed + 1))
    log, events = [], {}

    def note(msg):
        log.append(f"{time.strftime('%H:%M:%S')} {msg}")
        print(log[-1])

    with sync_playwright() as pw:
        browser = pw.chromium.launch()
        u = urlparse(BASE)
        proxy = CuttableProxy(u.hostname, u.port or 80)
        # the browser talks to the app only through the proxy; the writers go direct
        ctx = browser.new_context(base_url=f"http://127.0.0.1:{proxy.port}")
        page = ctx.new_page()
        page.on("console", lambda m: note(f"browser console: {m.text}") if m.type == "error" else None)
        page.goto("/")
        page.fill("#key", proj.key)
        page.click("button[type=submit]")
        page.wait_for_selector(f"tr[data-run-id='{run}']")
        page.click(f"tr[data-run-id='{run}'] a")
        page.wait_for_function("window.__soup && window.__soup.conn === 'live'")
        note(f"seed={seed} points={len(points)} batches_in_plan={len(plan)} late_batches={len(late)} conflicts={len(conflicts)}")

        q = queue.Queue()
        for b in plan:
            q.put(b)
        done = [0]

        def sender():
            while True:
                try:
                    b = q.get_nowait()
                except queue.Empty:
                    return
                client.send(b)
                done[0] += 1
                # paced so the whole plan takes ~35 s and the disruptions land mid-stream
                time.sleep(rnd.uniform(0.3, 0.6))

        senders = [threading.Thread(target=sender) for _ in range(4)]
        for t in senders:
            t.start()

        def wait_progress(frac):
            while done[0] < len(plan) * frac:
                time.sleep(0.05)

        # disruption 1: server closes every stream (deploy / LB idle timeout)
        wait_progress(0.25)
        r0 = page.evaluate("window.__soup.reconnects")
        httpx.post(f"{BASE}/_test/drop-streams")
        page.wait_for_function(f"window.__soup.reconnects > {r0}", timeout=10_000)
        events["server_drop_reconnected"] = True
        events["progress_at_disruptions"] = [done[0]]
        note(f"disruption 1: server dropped streams at {done[0]}/{len(plan)}; page reconnected")

        # disruption 2: the browser's network dies for 3 s (RST + refused) while writes continue
        wait_progress(0.45)
        events["progress_at_disruptions"].append(done[0])
        db_seq = lambda: db.execute("SELECT last_seq FROM runs WHERE id=%s", (run,)).fetchone()[0]  # noqa: E731
        r1 = page.evaluate("window.__soup.reconnects")
        proxy.cut()
        time.sleep(0.3)
        seq_cut, db_cut = page.evaluate("window.__soup.lastSeq"), db_seq()
        time.sleep(2.7)
        seq_end, db_end = page.evaluate("window.__soup.lastSeq"), db_seq()
        conn_during = page.evaluate("window.__soup.conn")
        proxy.restore()
        page.wait_for_function(f"window.__soup.reconnects > {r1}", timeout=10_000)
        t_rec = time.monotonic()
        page.wait_for_function(f"window.__soup.lastSeq >= {db_end}", timeout=10_000)
        events["network_cut"] = {"page_seq_at_cut": seq_cut, "page_seq_at_restore": seq_end,
                                 "db_seq_at_cut": db_cut, "db_seq_at_restore": db_end,
                                 "conn_state_during": conn_during,
                                 "catchup_s": round(time.monotonic() - t_rec, 2)}
        note(f"disruption 2: network cut 3s at {done[0]}/{len(plan)}: {events['network_cut']}")

        # disruption 3: full page reload mid-stream (all client state lost, rebuilt from seq 0)
        wait_progress(0.6)
        events["progress_at_disruptions"].append(done[0])
        page.reload()
        page.wait_for_function("window.__soup && window.__soup.conn === 'live'")
        note(f"disruption 3: page reloaded at {done[0]}/{len(plan)}")

        # disruption 4: silent network death (no RST, no FIN). Only the page's watchdog can notice.
        wait_progress(0.8)
        events["progress_at_disruptions"].append(done[0])
        w0 = page.evaluate("window.__soup.watchdog")
        seq_bh = page.evaluate("window.__soup.lastSeq")
        t_bh = time.monotonic()
        proxy.cut("blackhole")
        page.wait_for_function(f"window.__soup.watchdog > {w0}", timeout=25_000)
        detect = time.monotonic() - t_bh
        page.wait_for_function("window.__soup.conn === 'live'", timeout=10_000)
        db_now = db_seq()
        page.wait_for_function(f"window.__soup.lastSeq >= {db_now}", timeout=10_000)
        events["blackhole"] = {"page_seq_at_cut": seq_bh, "detected_after_s": round(detect, 1),
                               "db_seq_at_detect": db_now}
        note(f"disruption 4: blackhole at {events['progress_at_disruptions'][-1]}/{len(plan)}: {events['blackhole']}")

        for t in senders:
            t.join()
        note("main plan sent; sending late batches")
        time.sleep(1.0)
        for b in late:
            client.send(b)

        final_seq = db.execute("SELECT last_seq FROM runs WHERE id=%s", (run,)).fetchone()[0]
        t0 = time.monotonic()
        page.wait_for_function(f"window.__soup.lastSeq >= {final_seq}", timeout=20_000)
        time.sleep(0.5)  # let the 200 ms render tick paint the final state
        note(f"page caught up to seq {final_seq} in {time.monotonic() - t0:.2f}s after last write")
        st = page_state(page)
        page.screenshot(path=str(RESULTS / "e2e_final_page.png"), full_page=True)
        browser.close()

    db_rows = db.execute("SELECT key, step, value, seq FROM metrics WHERE run_id=%s", (run,)).fetchall()
    api = httpx.get(f"{BASE}/runs/{run}/metrics", headers=proj.h()).json()
    db_set = {(k, s, v, q) for k, s, v, q in db_rows}
    page_set = {(r["key"], r["step"], r["value"], r["seq"]) for r in st["rows"]}
    api_set = {(p["key"], p["step"], p["value"], p["seq"]) for p in api["points"]}
    shown_mismatch = [r for r in st["rows"]
                      if int(r["shown_step"]) != r["step"] or int(r["shown_seq"]) != r["seq"]
                      or abs(float(r["shown_value"]) - r["value"]) > 1e-5 * max(1, abs(r["value"]))]
    by_key = {k: str(sum(1 for r in db_rows if r[0] == k)) for k in KEYS}

    report = {
        "seed": seed, "run_id": run, "client": client.stats, "events": events, "log": log,
        "db_rows": len(db_rows), "page_rows": len(st["rows"]), "api_rows": len(api_set),
        "expected_rows": len(points), "db_last_seq": final_seq, "page_last_seq": st["last_seq"],
        "page_gaps": st["gaps"], "page_dupes_ignored": st["dupes"], "page_reconnects_after_reload": st["reconnects"],
        "only_in_db": sorted(db_set - page_set)[:10], "only_on_page": sorted(page_set - db_set)[:10],
        "visible_text_mismatches": shown_mismatch[:10],
        "conflicts_page_shows_first_value": all(
            (c["key"], c["step"], c["value"]) not in {(r["key"], r["step"], r["value"]) for r in st["rows"]}
            for c in conflicts),
    }
    (RESULTS / "e2e_report.json").write_text(json.dumps(report, indent=2, default=str))
    print(json.dumps({k: v for k, v in report.items() if k != "log"}, indent=2, default=str))

    assert len(db_rows) == len(points) == STEPS * len(KEYS)
    assert page_set == db_set, "page differs from database"
    assert api_set == db_set
    assert sorted(r[3] for r in db_rows) == list(range(1, len(db_rows) + 1))
    assert st["last_seq"] == final_seq and st["points"] == len(db_rows)
    assert st["summaries"] == {k: f"points: {n}" for k, n in by_key.items()}
    assert not shown_mismatch
    assert report["conflicts_page_shows_first_value"]
    # A request that timed out client-side may have been applied; its retry then reports
    # "duplicates". So the client's own `accepted` sum is only an upper-bounded guess,
    # and exact only if no request timed out. (First run of this test asserted ==, and
    # failed with 882 != 900 for exactly this reason.)
    assert client.stats["accepted"] <= len(points)
    if client.stats["client_timeouts"] == 0:
        assert client.stats["accepted"] == len(points)
    assert events["server_drop_reconnected"]
    # the first version of this test sent everything before the first disruption; guard against that
    assert all(p < len(plan) for p in events["progress_at_disruptions"]), "a disruption did not land mid-stream"
    nc = events["network_cut"]
    assert nc["page_seq_at_restore"] == nc["page_seq_at_cut"], "page received data through a cut network?"
    assert nc["db_seq_at_restore"] > nc["page_seq_at_restore"], "no writes happened during the cut"
    assert st["gaps"] == 0, "server sent a seq gap (page self-healed, but the server is wrong)"
