"""Load: N runs x R events/s for D seconds, one live subscriber per run.

Measures ingest latency, end-to-end delivery latency (send -> subscriber), errors, and
completeness (each subscriber must receive exactly seq 1..last_seq of its run).

  python load_test.py --runs 50 --rate 10 --batch 10 --duration 60
  --batch 10 => one POST per second per run carrying 10 points
  --batch 1  => ten POSTs per second per run (worst case: 500 req/s)
"""
import argparse
import asyncio
import json
import os
import statistics
import time
import uuid

import httpx
import psycopg

BASE = os.environ.get("BASE_URL", "http://localhost:8000")
DB = os.environ.get("DATABASE_URL", "postgresql://soup:soup@localhost:5432/soup")
ADMIN = os.environ.get("ADMIN_TOKEN", "dev-admin-token")


def pct(xs, p):
    if not xs:
        return float("nan")
    xs = sorted(xs)
    return xs[min(len(xs) - 1, int(len(xs) * p / 100))]


async def subscriber(client, run, h, stop, out):
    seqs, lat = [], []
    after = 0
    while not stop.is_set():
        try:
            async with client.stream("GET", f"{BASE}/runs/{run}/stream", headers={**h, "last-event-id": str(after)},
                                     timeout=httpx.Timeout(10, read=30)) as r:
                ev = data = None
                async for line in r.aiter_lines():
                    if line.startswith("event:"):
                        ev = line[7:]
                    elif line.startswith("data:"):
                        data = line[6:]
                    elif line == "" and ev == "metric":
                        d = json.loads(data)
                        seqs.append(d["seq"])
                        after = d["seq"]
                        lat.append(time.time() - d["value"])
                        ev = data = None
                    if stop.is_set():
                        break
        except (httpx.HTTPError, asyncio.CancelledError):
            out.setdefault("sub_reconnects", 0)
            out["sub_reconnects"] += 1
    out[run] = (seqs, lat)


async def writer(client, run, h, args, t_end, stats):
    step = 0
    interval = args.batch / args.rate
    nxt = time.monotonic()
    while time.monotonic() < t_end:
        pts = []
        for _ in range(args.batch):
            pts.append({"key": "lat", "step": step, "value": time.time()})
            step += 1
        t0 = time.perf_counter()
        try:
            r = await client.post(f"{BASE}/runs/{run}/metrics", json={"points": pts}, headers=h, timeout=10)
            stats["lat"].append((time.perf_counter() - t0) * 1000)
            if r.status_code != 200:
                stats["errors"][str(r.status_code)] = stats["errors"].get(str(r.status_code), 0) + 1
            else:
                stats["sent"] += args.batch
        except httpx.HTTPError as e:
            stats["errors"][type(e).__name__] = stats["errors"].get(type(e).__name__, 0) + 1
        nxt += interval
        await asyncio.sleep(max(0, nxt - time.monotonic()))
        if nxt < time.monotonic() - 1:
            stats["behind_schedule"] += 1


async def main(args):
    limits = httpx.Limits(max_connections=args.runs * 3, max_keepalive_connections=args.runs * 3)
    async with httpx.AsyncClient(limits=limits) as client:
        d = (await client.post(f"{BASE}/admin/projects", json={"name": f"load-{uuid.uuid4().hex[:6]}"},
                               headers={"x-admin-token": ADMIN})).json()
        h = {"authorization": f"Bearer {d['api_key']}"}
        runs = [(await client.post(f"{BASE}/runs", json={"name": f"load{i}"}, headers=h)).json()["id"]
                for i in range(args.runs)]
        stop = asyncio.Event()
        subs_out: dict = {}
        subs = [asyncio.create_task(subscriber(client, r, h, stop, subs_out)) for r in runs]
        await asyncio.sleep(1)
        stats = {"lat": [], "errors": {}, "sent": 0, "behind_schedule": 0}
        t0 = time.monotonic()
        await asyncio.gather(*(writer(client, r, h, args, t0 + args.duration, stats) for r in runs))
        elapsed = time.monotonic() - t0
        await asyncio.sleep(3)  # let subscribers drain
        stop.set()
        await asyncio.wait(subs, timeout=35)
        for s in subs:
            s.cancel()

    with psycopg.connect(DB) as db:
        last = dict(db.execute("SELECT id::text, last_seq FROM runs WHERE id = ANY(%s::uuid[])", (runs,)).fetchall())
        stored = db.execute("SELECT count(*) FROM metrics WHERE run_id = ANY(%s::uuid[])", (runs,)).fetchone()[0]
    incomplete, e2e = [], []
    for r in runs:
        seqs, lat = subs_out.get(r, ([], []))
        e2e += lat
        if seqs != list(range(1, last[r] + 1)):
            incomplete.append({"run": r[:8], "got": len(seqs), "db": last[r]})
    res = {
        "config": vars(args), "elapsed_s": round(elapsed, 1),
        "points_acked": stats["sent"], "points_stored": stored,
        "achieved_points_per_s": round(stored / elapsed, 1),
        "target_points_per_s": args.runs * args.rate,
        "requests": len(stats["lat"]), "errors": stats["errors"], "writer_fell_behind": stats["behind_schedule"],
        "ingest_ms": {"p50": round(pct(stats["lat"], 50), 1), "p95": round(pct(stats["lat"], 95), 1),
                      "p99": round(pct(stats["lat"], 99), 1), "max": round(max(stats["lat"]), 1)},
        "e2e_delivery_ms": {"p50": round(pct(e2e, 50) * 1000, 1), "p95": round(pct(e2e, 95) * 1000, 1),
                            "p99": round(pct(e2e, 99) * 1000, 1), "max": round(max(e2e) * 1000, 1)} if e2e else None,
        "subscribers_complete": len(runs) - len(incomplete), "subscribers_incomplete": incomplete[:5],
        "subscriber_reconnects": subs_out.get("sub_reconnects", 0),
    }
    print(json.dumps(res, indent=2))
    if args.out:
        with open(args.out, "w") as f:
            json.dump(res, f, indent=2)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--runs", type=int, default=50)
    ap.add_argument("--rate", type=float, default=10)
    ap.add_argument("--batch", type=int, default=10)
    ap.add_argument("--duration", type=float, default=60)
    ap.add_argument("--out")
    asyncio.run(main(ap.parse_args()))
