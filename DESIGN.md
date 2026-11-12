# Design (written before code)

Stack: FastAPI + asyncpg + PostgreSQL 16, SSE for live updates, plain HTML/JS UI.
Postgres is the single source of truth; nothing the user sees is derived from
anything that is not in Postgres.

## Data model

```
projects(id uuid pk, name)
api_keys(id uuid pk, project_id fk, key_hash bytea unique, prefix text,
         created_at, revoked_at null)
runs(id uuid pk, project_id fk, name, created_at, last_seq bigint default 0)
metrics(run_id fk, seq bigint, key text, step bigint, value double, ts timestamptz,
        received_at, PRIMARY KEY (run_id, seq), UNIQUE (run_id, key, step))
```

IDs are random UUIDv4 (122 bits): not enumerable, not sequential.

## Event format and ordering

Client sends a batch:

```json
POST /runs/{run_id}/metrics      Authorization: Bearer <api key>
{"points": [{"key": "loss", "step": 17, "value": 0.42, "ts": "2026-11-12T10:00:00Z"}]}
```

* **Identity of a point = (run_id, key, step).** The client owns `step`; this is the
  idempotency key. No separate batch id is needed: a batch is just a set of points.
* **Ordering = server-assigned `seq`, per run, gap-free, 1..N.** `seq` is the order in
  which points were *committed*, not the order of `step` or `ts`. Out-of-order and late
  points are fine: they simply get a later `seq`. The UI orders points by `step` for
  display, but consumes them by `seq`.
* Why per-run and gap-free: a global `BIGSERIAL` is assigned at INSERT time, not at
  COMMIT time. Txn A gets id 10, txn B gets id 11, B commits first, a reader sees 11 and
  moves its cursor to 11, then A commits 10 → the reader never sees 10. That is a silent
  loss. To avoid it, every ingest transaction first takes a row lock on the run
  (`SELECT last_seq FROM runs WHERE id=$1 FOR UPDATE`), assigns `last_seq+1..last_seq+n`
  to the new points, and updates `last_seq`. Writes to one run are serialized, so commit
  order == seq order, and a rollback rolls back the seq too (no gaps). Cost: one run's
  writes are serial (fine for 10 ev/s; different runs are fully parallel).
* SSE event: `id: <seq>`, `event: metric`, `data: {"seq","key","step","value","ts"}`.

## Same batch sent again

Inside the run lock: dedupe the batch by (key, step) (first occurrence wins), drop points
that already exist, insert the rest with new seqs. Re-sending a batch (any number of
times, concurrently or not) inserts nothing and returns `200 {"accepted": 0,
"duplicates": n, "conflicts": k, "last_seq": ...}`. A "conflict" is a re-sent (key, step)
with a *different* value: first write wins, the conflict is counted in the response and
logged, never silently overwritten. The response is the same shape for first send and
retry, so a client that timed out and retried cannot tell (and need not care).

The whole batch is one transaction: validation happens before any write (422 rejects the
whole batch), and a failure during the write rolls everything back (no partial batch).
Batch size is capped (1000 points → 413).

## Catching up after a disconnect

`GET /runs/{id}/stream` (SSE). The server reads from **the database**, not from an
in-memory queue: each subscriber has a cursor `after`; it loops
`SELECT ... WHERE run_id=$1 AND seq > $cursor ORDER BY seq LIMIT 500`, emits rows,
advances the cursor, then waits for a wake-up.

* Start position: `Last-Event-ID` header (sent automatically by `EventSource` on
  reconnect) or `?after=N`; default 0 = full history. The page loads all state through
  the same stream, so there is exactly one code path for "initial load" and "catch-up".
* Wake-ups come from Postgres `LISTEN/NOTIFY` (ingest does `NOTIFY run_events, run_id`
  in the same transaction, so it fires only on commit). NOTIFY is only a hint: if it is
  lost (listener connection dropped), every subscriber also polls every 2 s. A lost
  notification costs latency, never data.
* Because seq is gap-free, the client checks `seq == last_seq + 1`; on a gap it drops the
  connection and reconnects from `last_seq` (and counts the gap, visible on the page).
* Slow clients: there is no per-client queue to overflow. A slow socket just means its
  cursor lags; memory per subscriber is one page of rows. A client 30 s behind catches up
  from the DB on its own pace.

## Project isolation

* Every request is authenticated by an API key (header) or by an HttpOnly, SameSite=Strict
  cookie set from that key by `POST /session` (EventSource can't send headers; keys never
  go into URLs, so they don't land in access logs).
* Keys are stored as SHA-256 hashes; only an 8-char prefix is ever logged. Key lookup hits
  the DB on every request (no auth cache), so revocation/rotation is immediate for REST.
  Open SSE streams re-check their key on every poll tick (≤ 2 s) and close when revoked.
* Every query that touches a run is scoped: `WHERE id = $run AND project_id = $project`.
  Foreign and non-existent runs return the same `404 {"detail":"run not found"}` from the
  same query, so neither the status, the body nor the query plan reveals existence.
* The only cache (run-id → exists-for-project, used on the ingest hot path) is keyed by
  `(project_id, run_id)`, so a hit for project A can never answer a request from B.
* Responses carry `Cache-Control: no-store` and `Vary: Authorization, Cookie` so no shared
  HTTP cache can serve A's data to B.
* WebSocket is not implemented (SSE only), so there is no second live channel to secure.
