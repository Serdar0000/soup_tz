# Note: soup experiment tracker

Design: [DESIGN.md](DESIGN.md). Raw output: [results/](results/). Core invariant:
**Postgres is the source of truth; SSE is a resumable cursor over it.** A run-row
lock assigns gap-free `seq` values in commit order, while NOTIFY only wakes
readers and a 2-second poll covers missed notifications.

## 1. Correctness and isolation

The Chromium E2E test sent 900 shuffled points through four concurrent writers,
including retries, client timeouts, delayed batches, and five conflicting
re-sends. Four asserted mid-stream disruptions covered forced stream closure, a
3-second network cut, page reload, and a silent TCP blackhole. The page froze at
seq 431 while the DB advanced 447→546, then caught up in 0.01 s. The watchdog
detected the blackhole after 11.6 s. Final page, API, and DB matched exactly:
900/900 rows, no gaps; conflicts displayed the first stored value.

Eleven isolation tests show project B cannot list, read, write, or subscribe to
A's runs using headers or cookies. Foreign and missing IDs return identical
404s; cache warming does not leak access; streams do not cross projects; raw
keys appeared in 0 of 1727 log lines. Median foreign and missing lookup latency
was the same (0.730 ms, n=300). A cookie authorizes reads only, limiting CSRF.

Six mutation checks were all caught: cache without project ID, unscoped run
list, unscoped stream, stream ignoring key rotation, global sequence, and a
mid-batch commit. The global-sequence mutation silently lost 137, 57, and 71
rows in three 2400-row runs, demonstrating why the run lock matters.

Limits: final-state E2E cannot exclude every transient UI error or untested
interleaving; nginx was tested, not arbitrary buffering proxies/CDNs. Isolation
depends on application query scoping rather than PostgreSQL RLS, and there is no
per-project rate limit. Operators can see run IDs in logs.

## 2. Silent failures

| risk | defence and observed result |
|---|---|
| reconnect loss | DB cursor; 8 writers and 72 reader drops delivered 2400/2400 in order |
| duplicates/conflicts | `(run,key,step)` identity; stored once, first value kept |
| slow client | no subscriber queue; 10,000/10,000 delivered, unrelated writes p99 5 ms |
| partial batch | validation + one transaction; injected fault and `kill -9` produced no partial or acknowledged-but-missing batch (78,718 rows checked) |
| key rotation | no auth cache; REST rejected immediately, stream closed after 1.5 s |
| lost NOTIFY | polling and listener reconnect; delivery resumed after 0.4 s |
| silent TCP death | real ping + 12 s watchdog; a real bug found and fixed |
| DB outage | 503 + `Retry-After`; a real 500-response bug found and fixed |
| long-run UI cost | redraw changed metrics, lazy table, downsampled chart; 36k-point tick fell from 252 ms to 6 ms |

Remaining windows: a stream may survive revocation for up to two seconds; a
silently dead page may look current for up to 12 seconds. The client's
`accepted` total is unreliable after timed-out retries; server `last_seq` is
authoritative. Production monitoring should compare `last_seq` with row counts
and collect client-reported gaps.

## 3. Capacity verdict: SHIP with two replicas and batching

| setup | achieved | loss/errors | ingest latency |
|---|---:|---:|---:|
| 1 process, 50 runs, batches of 10 | 500 points/s | 0 | p50 3.5 / p99 42 ms |
| 1 process, 500 single-point requests/s | 500 req/s | 0 | p50 7.5 / p99 65 ms |
| 2 replicas + nginx, 500 req/s | 500 req/s | 0 | client p99 56 ms |
| 1 process, 200 runs, 4× batching | 1969/2000 points/s | 0 | p50 4.1 / p99 82 ms |

Deploy the tested two-replica setup first: one process has less than 2×
headroom and reconnects every stream if it fails. Then batch client points;
at 500 commits/s PostgreSQL used 52–63% CPU, so fewer commits matter more than
more app replicas. Add per-project rate limits and RLS next. Unmeasured risks
include PostgreSQL's commit-time NOTIFY lock and large concurrent batches to one
run, which the per-run lock must serialize.

## AI tools

I directed a Claude Code (Claude Opus) session that produced the implementation,
tests, and first report. Running the system exposed and corrected several
plausible-looking AI mistakes: disruptions scheduled after ingestion, an
offline simulation that did not affect EventSource, a faulty proxy reset, an
incorrect accepted-count assertion, a writer that ignored HTTP status, missing
nginx keepalive, and shell word splitting.

I then used Codex for an independent review. I traced the ordering, recovery,
and browser paths in `ingest.py`, `stream.py`, and `run.html`; opened the local
deployment with `.demo.json`; inspected the run list and lazy table; and
reloaded the page. Both 210-point series rebuilt from SSE (420 total,
`last_seq=420`) with zero reported gaps, duplicates, or stalls. I also checked
the saved artifacts (`22 passed`; E2E page = DB = API at 900 rows) and parsed
both Python modules. That pass changed this disclosure only; the evidence did
not justify a functional code change.
