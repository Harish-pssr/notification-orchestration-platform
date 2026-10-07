#!/usr/bin/env bash
# Capacity matrix (D13). For each configuration `bench capacity` runs three stages:
#   1. a geometric ramp (30 s steps) to find the neighbourhood,
#   2. confirmation with repeated 60 s steps and a majority vote,
#   3. three 60 s repetitions at 70% of the confirmed capacity, for latency.
#
#   consumer sweep:  4 relays, 1/2/4/8 consumers per channel
#   relay sweep:     8 consumers per channel, 1/2/8 relays (4 relays is in the consumer sweep)
#   baseline:        1 relay, 1 consumer per channel, measured first and last as a drift control
#
# Every step resets all data, so this destroys the dev data. Needs the dev stack and a built
# bin/bench (go build -o bin/bench ./cmd/bench). It refuses to run on battery power.
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH=${BENCH:-bin/bench}
OUT=${OUT:-benchmark-results}
ONLY=${ONLY:-}   # optional extended regex: run only the configurations whose label matches

# label relays consumers start-rate
CONFIGS=(
  "cap-r1c1-first 1 1 50"
  "cap-r4c1 4 1 50"
  "cap-r4c2 4 2 100"
  "cap-r4c4 4 4 150"
  "cap-r4c8 4 8 200"
  "cap-r1c8 1 8 100"
  "cap-r2c8 2 8 150"
  "cap-r8c8 8 8 200"
  "cap-r1c1-last 1 1 50"
)

mkdir -p "$OUT"
for cfg in "${CONFIGS[@]}"; do
  read -r label relays consumers start <<< "$cfg"
  if [[ -n "$ONLY" && ! "$label" =~ $ONLY ]]; then continue; fi
  "$BENCH" capacity -confirm -label "$label" -relays "$relays" -consumers "$consumers" \
    -start "$start" -factor 1.25 -out "$OUT" 2>&1 | tee -a "$OUT/capacity.log"
done
echo "done"
