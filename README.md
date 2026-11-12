# soup tracker: a mini experiment tracker

FastAPI + asyncpg + PostgreSQL 16. Live run page over SSE. See [DESIGN.md](DESIGN.md) (written
before the code) and [NOTE.md](NOTE.md) (results, verdict, concerns).

## Run

```bash
docker compose up --build
```

Open http://localhost:8000. Create a project (prints an API key):

```bash
curl -s -XPOST localhost:8000/admin/projects -H 'x-admin-token: dev-admin-token' \
  -H 'content-type: application/json' -d '{"name":"demo"}'
```

```bash
K=soup_...   # api_key from above
RUN=$(curl -s -XPOST localhost:8000/runs -H "authorization: Bearer $K" \
  -H 'content-type: application/json' -d '{"name":"exp-1"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["id"])')
curl -s -XPOST localhost:8000/runs/$RUN/metrics -H "authorization: Bearer $K" \
  -d '{"points":[{"key":"loss","step":1,"value":0.9},{"key":"loss","step":2,"value":0.7}]}'
```

Paste the key into the page and open the run. Points sent from now on appear live.

## API

| method | path | auth |
|---|---|---|
| POST | `/admin/projects` | `X-Admin-Token` |
| POST | `/keys/rotate` `{"grace_seconds":0}` | header |
| POST / DELETE | `/session` `{"api_key":...}` (sets HttpOnly cookie for the browser) | — |
| POST / GET | `/runs` | header (POST), header or cookie (GET) |
| GET | `/runs/{id}`, `/runs/{id}/metrics` | header or cookie |
| POST | `/runs/{id}/metrics` `{"points":[{key,step,value,ts?}]}` ≤1000 points | header |
| GET | `/runs/{id}/stream` SSE, resumes from `Last-Event-ID` or `?after=` | header or cookie |

## Tests

```bash
docker compose run --rm tests
```

Runs all 22 tests (isolation, silent failures, Playwright E2E); raw output goes to
`results/pytest_output.txt`, the E2E report to `results/e2e_report.json`, and the final page screenshot to `results/e2e_final_page.png`.

```bash
./scripts/check_logs.sh
```

```bash
./scripts/mutation_check.sh
```

```bash
docker compose run --rm tests python load_test.py --runs 50 --rate 10 --batch 10 --duration 60
```

`check_logs.sh` greps the app logs for every raw key the tests issued.
`mutation_check.sh` injects 6 known bugs and confirms the tests catch each one (it rebuilds the app).
`ui_scale_probe.py N` measures the page with N points.

### Two replicas behind nginx, and the crash test

```bash
docker compose -f docker-compose.yml -f docker-compose.replicas.yml up -d --build --wait
```

```bash
docker compose -f docker-compose.yml -f docker-compose.replicas.yml run --rm tests
```

```bash
uv run --python 3.12 --with httpx --with 'psycopg[binary]' scripts/crash_test.py
```

`crash_test.py` (run from the host, replicas setup) does `kill -9` on one replica and then on
Postgres under 8 writers plus a live reader, and checks: no partial batch, no acknowledged batch
missing, gap-free seq, reader exact, and DB outages answered with 503 (never 500).

`SOUP_TEST_HOOKS=1` (set in compose) enables `/_test/drop-streams` and the `X-Test-Fault`
header used by the tests. Never set it in production.

## Layout

```
app/soup/       main.py (routes), ingest.py, stream.py, auth.py, schema.sql, static/
tests/          conftest.py, test_isolation.py, test_silent_failures.py,
                test_e2e_consistency.py, load_test.py, ui_scale_probe.py
scripts/        check_logs.sh, mutation_check.sh, crash_test.py
lb/             nginx.conf for the replicas setup
results/        raw outputs of everything above
```
