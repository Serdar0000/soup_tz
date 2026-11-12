# Note: soup experiment tracker

Design: [DESIGN.md](DESIGN.md). Raw output: [results/](results/). The core idea in one line:
**Postgres is the only source of truth and the live stream is just a cursor over it.** Each
run has a gap-free `seq` that is assigned under a row lock, so commit order equals seq order;
the client resumes from `Last-Event-ID`; NOTIFY only wakes subscribers up, and a 2 s poll covers a lost NOTIFY.

## 1. Consistency: the E2E test (`tests/test_e2e_consistency.py`)

Real Chromium against the real stack. 900 points (3 metrics × 300 steps) are shuffled into
1–8-point batches and sent in random order by 4 concurrent senders. 30% are re-sent later, 10%
are held back to the end, 5 are re-sent with a different value, and 10% of requests time out on
the client side after the server may already have applied them (the client retries). Four
disruptions land **mid-stream** (asserted):

| disruption | what the page saw (final run) |
|---|---|
| server closes every stream | reconnected, resumed from Last-Event-ID |
| browser network cut 3 s (TCP RST + refused) | frozen at seq 431 while the DB went 447→546; caught up 0.01 s after restore |
| full page reload | rebuilt from seq 0 |
| silent network death (bytes vanish, no RST/FIN) | watchdog noticed after 11.6 s, reconnected, caught up |

Result: page = DB = API, 900/900 rows compared exactly as (key, step, value, seq). Visible
text matches. 0 gaps. Conflicting re-sends show the first, stored value. **Does not catch:** SSE-buffering
proxies/CDNs other than the nginx I tested; transient wrong states mid-stream (only the final state
is compared); browser tab freezing / mobile background; interleavings the random schedule didn't hit
(the targeted race test and the mutation checks cover the important one); multiple browsers
on one run. The E2E runs on one app process; the full suite also passes on 2 replicas behind nginx.

## 2. Isolation (`tests/test_isolation.py`, 11 tests)

B cannot list A's runs, read run/metrics, write (DB checked: unchanged), or open A's SSE
stream, whether with the header or with the browser cookie. A cookie never authorizes writes (CSRF).
Foreign, missing and malformed ids get byte-identical 404s. After A warms the run cache, B still
gets 404. `Cache-Control: no-store` and `Vary: Authorization, Cookie` are set. A's stream never carries
B's events. IDs are UUIDv4. 500/422 bodies echo neither internals nor submitted values. The 31 raw keys issued
in tests were grepped across 1727 log lines: **0 hits**. Timing: foreign 0.730 ms vs missing
0.730 ms median (n=300); it's the same scoped query.
**Could still leak:** run ids are in logs (operators see all projects); there is no *performance*
isolation (no per-project rate limit, so A can slow B, and B's latency becomes a side channel on A);
isolation depends on every query carrying `project_id` with nothing in the DB enforcing it
(Postgres RLS would be defense-in-depth); the cookie holds the raw key (HttpOnly, SameSite=Strict).

**Do the tests bite?** `scripts/mutation_check.sh` injects 6 known bugs (cache keyed by run id
only, unscoped run list, unscoped stream, streams ignore rotation, global sequence without the
run lock, commit inside a batch): **6/6 caught**. With the global sequence, 2400 rows were in the DB
and the reader silently missed 137, 57 and 71 of them in three runs. No error anywhere.

## 3. Silent failures

| failure | defence | how checked | result |
|---|---|---|---|
| lost on reconnect | DB cursor, gap-free seq, client gap check | 8 writers + retry storms, reader dropped 72×; E2E ×4 | 2400/2400 in order |
| duplicates | identity (run,key,step) under the run lock | same batch ×10 concurrently; E2E | stored once |
| slow-client overflow | no queue: each subscriber reads the DB at its own pace | 10 k backlog read slowly, writes elsewhere | 10 000/10 000; other writes p99 5 ms |
| partial writes | one transaction, validate first | invalid point; fault after INSERT before COMMIT; **`kill -9` of a replica and of Postgres under 8 writers** | 0 partial of ~2900 batches, 0 acked-but-missing (78 718 rows), reader exact |
| key alive after rotation | no auth cache; streams re-check ≤2 s | REST, cookie, open stream, grace | REST 401 at once; stream `revoked` after 1.5 s |
| lost NOTIFY | 2 s poll + listener reconnect | `pg_terminate_backend` on the listener | delivered after 0.4 s |
| **dead TCP looks alive** | ping event + 12 s watchdog | blackhole proxy | **real bug, fixed** |
| **DB outage returned 500** | now 503 + `Retry-After` | crash test | **real bug, fixed**: many SDKs don't retry a 500, so a DB blip would become client-side drops |
| **UI cost grows with run length** | redraw only changed metrics; table only when open; downsampled chart | 36 k points | **real bug: 252 ms per 200 ms tick → 6 ms** |

Remaining windows: a stream can outlive a rotated key by ≤2 s. A silently dead client shows stale
data for ≤12 s, and the server holds that subscriber until TCP gives up. The client's own `accepted` sum is
wrong after timeouts (retries come back as duplicates); the server's `last_seq` is the truth.

## 4. Verdict for 50 runs × 10 events/s: **SHIP**, on 2 replicas, with client batching

| setup / mode | achieved | errors / lost | server ingest p50/p99 | CPU |
|---|---|---|---|---|
| 1 process, 1 POST/s × 10 pts per run | 500 pts/s | 0 / 0 (50/50 streams exact) | 3.5 / 42 ms | app ~12% |
| 1 process, 10 POST/s × 1 pt (500 req/s) | 500 req/s | 0 / 0 | 7.5 / 65 ms | app 45–63%, db 24–36% |
| 2 replicas + nginx, 500 req/s | 500 req/s | 0 / 0 | client p99 56 ms | app 31–39% each, **db 52–63%** |
| 1 process, 4× batched (200 runs) | 1969/2000 pts/s | 0 / 0 (200/200) | 4.1 / 82 ms | app ~21% |

Correctness held everywhere. Client-measured latency (p50 ~60 ms) is mostly the load generator: one
generator couldn't reach 500 req/s, two could. **Change first:** deploy the tested 2-replica setup
(`docker-compose.replicas.yml`): one process has <2× headroom unbatched and is a single point of
failure (survivable, as tested, but every stream reconnects at once). Then make clients batch:
with replicas, **Postgres becomes the bottleneck** at 500 commits/s, and 10 points per commit cuts it 10×.
Then per-project rate limits and RLS.

## 5. What surprised me, what still concerns me

The biggest surprise was how often *the test* was wrong rather than the system, while still looking
green. The first E2E ran all four "mid-stream" disruptions after the last batch had been sent; now it asserts
their timing. Playwright's `set_offline(true)` doesn't touch an open EventSource, so that check would
have passed vacuously forever. My TCP proxy's `close()` on a socket another thread was blocked on in
`recv()` sent nothing. That accident exposed a real product bug: a silently dead connection kept the
page "live" forever, because SSE comments are invisible to JS. What still concerns me: Postgres takes
a global lock for NOTIFY at commit, a write ceiling I haven't measured; the per-run lock serializes
big concurrent batches to one run; and I can only show the absence of failures I thought of.
Production needs a server-side check (compare `last_seq` and `count(*)` per run) plus client-reported
gaps in monitoring to *know* an event was lost.

## AI tools

The code, tests and this note were produced in a Claude Code session (Claude Opus) that I
directed. Its mistakes were caught by running things, not by reading: the vacuous E2E schedule, the
`set_offline` assumption, the proxy RST bug, a wrong `accepted == 900` assertion (882: retried
timeouts come back as duplicates), a race-test writer that ignored HTTP status (500s would have
passed), a missing nginx upstream keepalive, and a shell word-splitting bug in the load runner.
*Candidate: add here what you personally re-checked, changed or redid.*
