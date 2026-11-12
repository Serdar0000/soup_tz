#!/usr/bin/env bash
# Every raw API key the tests ever saw is in results/keys_issued.txt.
# None of them may appear anywhere in the app's logs (only 9-char prefixes may).
set -eu
cd "$(dirname "$0")/.."
logs=$(mktemp)
docker compose logs app --no-color > "$logs"
n=$(wc -l < results/keys_issued.txt | tr -d ' ')
hits=$(grep -c -F -f results/keys_issued.txt "$logs" || true)
lines=$(wc -l < "$logs" | tr -d ' ')
echo "log lines scanned: $lines, raw keys checked: $n, lines containing a raw key: $hits"
# what else is in the logs that a reader of logs from project A could learn about B?
echo "distinct key prefixes in logs: $(grep -oE 'key=soup_[A-Za-z0-9_-]{4}' "$logs" | sort -u | wc -l | tr -d ' ')"
echo "log lines with a run id in the path: $(grep -cE '/runs/[0-9a-f-]{36}' "$logs" || true)"
[ "$hits" = 0 ]
