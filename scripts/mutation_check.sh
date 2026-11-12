#!/usr/bin/env bash
# Negative controls: inject a known bug into the app, rebuild, run the test that is
# supposed to catch it. A test that still passes against a broken app proves nothing.
set -u
cd "$(dirname "$0")/.."
backup=$(mktemp -d); cp -r app/soup "$backup/"
restore() { rm -rf app/soup; cp -r "$backup/soup" app/soup; }
trap 'restore; docker compose up -d --build --wait app >/dev/null 2>&1; echo restored' EXIT
out=results/mutation_output.txt; : > "$out"

mutate() {  # file old new  (old must occur exactly once)
  python3 - "$@" <<'PY'
import sys
p, o, n = sys.argv[1:]
s = open(p).read()
assert s.count(o) == 1, f"pattern not unique in {p}: {o!r}"
open(p, "w").write(s.replace(o, n))
PY
}

check() {  # name test
  docker compose up -d --build --wait app >/dev/null 2>&1
  if docker compose run --rm tests pytest -q -x -p no:cacheprovider "$2" > "results/mut_$1.log" 2>&1; then
    echo "SURVIVED  $1  ($2 still passes)" | tee -a "$out"
  else
    echo "caught    $1  <- $(grep -m1 -E '^E ' "results/mut_$1.log" | cut -c1-110)" | tee -a "$out"
  fi
  restore
}

# 1. run cache keyed by run id only: A's cached hit answers B
mutate app/soup/main.py "k = (p.project_id, run_id)" "k = run_id"
check cache_key_without_project test_isolation.py::test_cache_does_not_leak_between_projects

# 2. run list not scoped to project
mutate app/soup/main.py "FROM runs WHERE project_id = \$1 ORDER BY" "FROM runs WHERE \$1::uuid IS NOT NULL ORDER BY"
check run_list_unscoped test_isolation.py::test_run_list_shows_only_own_runs

# 3. stream endpoint forgets the ownership check
mutate app/soup/main.py "    await get_run(request, p, rid)  # 404 before any stream bytes" "    pass"
check stream_unscoped test_isolation.py::test_cannot_subscribe_to_foreign_stream_header_or_cookie

# 4. open streams never re-check the key
mutate app/soup/stream.py "if not await auth.key_still_valid(pool, principal.key_id):" "if False:"
check stream_ignores_rotation test_silent_failures.py::test_rotated_key_stops_working_everywhere

# 5. the classic: global sequence assigned at INSERT time, no per-run lock
#    (commit order != seq order -> a reader can advance past an uncommitted seq)
echo "CREATE SEQUENCE IF NOT EXISTS gseq;" >> app/soup/schema.sql
mutate app/soup/ingest.py "WHERE id = \$1 AND project_id = \$2 FOR UPDATE" "WHERE id = \$1 AND project_id = \$2"
mutate app/soup/ingest.py "seqs = list(range(last_seq + 1, last_seq + 1 + len(new)))" \
  "seqs = [await conn.fetchval(\"SELECT nextval('gseq')\") for _ in new]; import asyncio, random; await asyncio.sleep(random.random() * 0.02)"
mutate app/soup/ingest.py "                   SELECT \$1, * FROM unnest(\$2::bigint[], \$3::text[], \$4::bigint[],
                                            \$5::float8[], \$6::timestamptz[])\"\"\"," \
  "                   SELECT \$1, * FROM unnest(\$2::bigint[], \$3::text[], \$4::bigint[],
                                            \$5::float8[], \$6::timestamptz[]) ON CONFLICT DO NOTHING\"\"\","
mutate app/soup/ingest.py "UPDATE runs SET last_seq = \$2 WHERE id = \$1\", run_id, last_seq)" \
  "UPDATE runs SET last_seq = GREATEST(last_seq, \$2) WHERE id = \$1\", run_id, last_seq)"
check global_seq_commit_order_race test_silent_failures.py::test_no_event_lost_with_concurrent_writers_and_reconnecting_reader

# 6. partial write: commit each point separately instead of one transaction
mutate app/soup/ingest.py "            if fault == \"after_insert\":" "            await conn.execute('COMMIT'); await conn.execute('BEGIN')
            if fault == \"after_insert\":"
check partial_write test_silent_failures.py::test_failure_mid_write_rolls_back_everything_and_leaves_no_seq_gap
