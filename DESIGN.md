# Design (written before code)

Stack: FastAPI, asyncpg, PostgreSQL 16, SSE, and plain HTML/JS. PostgreSQL is the
only source of truth; the live stream is a resumable cursor over stored rows.

## Model and ordering

`projects` own API keys and runs. A metric is identified by
`(run_id, key, step)` and also has a server-assigned, per-run `seq`. IDs are
random UUIDv4.

Each ingest transaction locks its run row (`SELECT ... FOR UPDATE`), removes
duplicate `(key, step)` values, reads existing points, assigns
`last_seq + 1..N`, inserts, and updates `last_seq`. The transaction commits the
whole batch or none of it; validation happens first and batches are capped at
1000 points. Re-sends are idempotent. If the same identity arrives with a new
value, the first value wins and the conflict is reported and logged.

The row lock is intentional. A global sequence is allocated before commit, so
transaction B can commit sequence 11 before transaction A commits 10. A reader
that advances to 11 would then miss 10 forever. Per-run, gap-free sequences make
commit order equal sequence order; rollback also rolls back the sequence. This
serializes writes to one run, acceptable at 10 events/s, while different runs
remain parallel.

## Live delivery and recovery

`GET /runs/{id}/stream` emits SSE events whose `id` is `seq`. A subscriber reads
`WHERE run_id = ? AND seq > cursor ORDER BY seq LIMIT 500`, advances its cursor,
then waits. `LISTEN/NOTIFY` wakes readers after commits, but is only a hint: a
2-second poll covers a lost notification or listener reconnect.

The initial page and reconnect use the same path. The cursor comes from
`Last-Event-ID` or `?after=N`; default zero rebuilds full history. The client
requires `seq == last_seq + 1` and reconnects from its last valid cursor on a
gap. Real ping events every 5 seconds drive a 12-second browser watchdog, so a
silently dead TCP connection is detected. Slow clients have no in-memory queue
to overflow: each reads the database at its own pace.

## Isolation and security

Requests use an API key header or an HttpOnly, SameSite=Strict cookie created by
`POST /session` (EventSource cannot set headers). Keys are stored as SHA-256
hashes; raw keys never enter URLs or logs. Authentication hits PostgreSQL on
every request, so REST revocation is immediate; open streams re-check within
two seconds.

Every run query includes both `id` and `project_id`. Foreign and missing IDs
therefore return the same 404 from the same query. The hot-path cache is keyed
by `(project_id, run_id)`. Responses use `Cache-Control: no-store` and
`Vary: Authorization, Cookie`. PostgreSQL RLS and per-project rate limits are
recommended defense-in-depth, but are outside this implementation.
