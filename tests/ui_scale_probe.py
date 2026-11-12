"""How does the run page behave with a 1-hour run (36k points)? Measurement, not a test."""
import sys, time, httpx
from playwright.sync_api import sync_playwright
from conftest import BASE, ADMIN
N = int(sys.argv[1]) if len(sys.argv) > 1 else 36000
d = httpx.post(f"{BASE}/admin/projects", json={"name": "ui-scale"}, headers={"x-admin-token": ADMIN}).json()
h = {"authorization": f"Bearer {d['api_key']}"}
run = httpx.post(f"{BASE}/runs", json={"name": "big"}, headers=h).json()["id"]
keys = ["loss", "acc", "lr"]
pts = [{"key": k, "step": s, "value": s * 0.001} for s in range(N // 3) for k in keys]
for i in range(0, len(pts), 1000):
    assert httpx.post(f"{BASE}/runs/{run}/metrics", json={"points": pts[i:i+1000]}, headers=h, timeout=30).status_code == 200
with sync_playwright() as pw:
    b = pw.chromium.launch(); ctx = b.new_context(base_url=BASE); pg = ctx.new_page()
    pg.goto("/"); pg.fill("#key", d["api_key"]); pg.click("button[type=submit]"); pg.wait_for_timeout(300)
    t0 = time.monotonic(); pg.goto(f"/r/{run}")
    pg.wait_for_function(f"Number(document.getElementById('n-points').innerText) >= {N}", timeout=120_000)
    t_load = time.monotonic() - t0
    # cost of one render tick with everything loaded (force dirty, time render())
    tick = pg.evaluate("() => { const t = performance.now(); for (const k of state.byKey.keys()) dirtyKeys.add(k); dirty = true; render(); return performance.now() - t; }")
    # live: one new point -> how long until visible
    t1 = time.monotonic()
    httpx.post(f"{BASE}/runs/{run}/metrics", json={"points": [{"key": "loss", "step": 10**9, "value": 1}]}, headers=h)
    pg.wait_for_function(f"Number(document.getElementById('n-points').innerText) >= {N + 1}", timeout=60_000)
    print(f"UI_SCALE points={N} initial_load_s={t_load:.1f} render_tick_ms={tick:.0f} new_point_visible_s={time.monotonic()-t1:.2f}")
    b.close()
