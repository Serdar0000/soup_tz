"""Live demo: creates a project + run (or reuses .demo.json) and feeds a fake training curve.
  python3 scripts/demo_feed.py [seconds]
"""
import json, math, random, sys, time, urllib.request
from pathlib import Path

BASE = "http://localhost:8000"
state_file = Path(__file__).resolve().parent.parent / ".demo.json"


def call(method, path, body=None, headers=None):
    req = urllib.request.Request(BASE + path, method=method, data=json.dumps(body).encode() if body else None,
                                 headers={"content-type": "application/json", **(headers or {})})
    with urllib.request.urlopen(req, timeout=10) as r:
        return json.load(r)


if state_file.exists():
    st = json.loads(state_file.read_text())
else:
    p = call("POST", "/admin/projects", {"name": "live-demo"}, {"x-admin-token": "dev-admin-token"})
    h = {"authorization": f"Bearer {p['api_key']}"}
    run = call("POST", "/runs", {"name": "resnet-demo"}, h)
    st = {"api_key": p["api_key"], "run_id": run["id"], "step": 0}
    state_file.write_text(json.dumps(st))
    print("created project and run", run["id"], flush=True)

h = {"authorization": f"Bearer {st['api_key']}"}
end = time.time() + float(sys.argv[1] if len(sys.argv) > 1 else 300)
step = st["step"]
while time.time() < end:
    batch = []
    for _ in range(5):
        batch += [{"key": "loss", "step": step, "value": 2.5 * math.exp(-step / 150) + random.gauss(0, 0.05)},
                  {"key": "accuracy", "step": step, "value": 0.95 - 0.85 * math.exp(-step / 120) + random.gauss(0, 0.01)}]
        step += 1
    # retry the SAME batch with a pause until acknowledged: dropping it and moving on would
    # lose points on the client side (the first version of this script did exactly that)
    while True:
        try:
            r = call("POST", f"/runs/{st['run_id']}/metrics", {"points": batch}, h)
            print(f"step {step} last_seq {r['last_seq']}", flush=True)
            break
        except Exception as e:  # server restarting / network: retry after a pause
            print("send failed, retrying:", e, flush=True)
            time.sleep(0.5)
    st["step"] = step
    state_file.write_text(json.dumps(st))
    time.sleep(1)
