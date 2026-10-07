#!/usr/bin/env bash
# Reliability and recovery experiments (D17). Needs the dev stack (fault injection on) and a
# built bin/bench (go build -o bin/bench ./cmd/bench). Every run resets all data.
#
#   1. reliability          100,000 events with duplicates, relay SIGKILLs and provider errors
#   2. reliability -control 10,000 events with the idempotency protections off (must show duplicates)
#   3. recovery relay       relay stopped for 60 s under load, 3 repetitions
#   4. recovery nats        NATS stopped for 60 s under load, 3 repetitions
#
# RATE is the arrival rate for the outage tests (about 70% of the measured capacity of the
# 4 relay, 4 consumer configuration).
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH=${BENCH:-bin/bench}
OUT=${OUT:-benchmark-results}
RATE=${RATE:-250}
mkdir -p "$OUT"

run() { echo; echo "##### $*"; "$BENCH" "$@" -confirm -out "$OUT" 2>&1 | tee -a "$OUT/reliability.log"; }

run reliability -events 100000 -rate 150
run reliability -control -events 10000 -rate 150
run recovery -service relay -rate "$RATE" -repeats 3
run recovery -service nats -rate "$RATE" -repeats 3
echo "done"
