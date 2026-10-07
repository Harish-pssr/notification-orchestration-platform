#!/usr/bin/env bash
# Capacity matrix (D13): for each configuration, ramp the open-loop arrival rate until the
# system stops sustaining it, and record the highest sustained rate.
#
#   consumer sweep:  4 relays, 1/2/4/8 consumers per channel
#   relay sweep:     8 consumers per channel, 1/2/8 relays (4 relays is in the consumer sweep)
#   baseline:        1 relay, 1 consumer per channel
#
# Each ramp step resets all data, so this destroys the dev data. Requires the dev stack
# (scripts/up) and a built bin/bench (go build -o bin/bench ./cmd/bench).
set -euo pipefail
cd "$(dirname "$0")/.."

BENCH=${BENCH:-bin/bench}
OUT=${OUT:-benchmark-results}
STEP=${STEP:-30s}

# label relays consumers start-rate
CONFIGS=(
  "cap-r4c1 4 1 50"
  "cap-r4c2 4 2 100"
  "cap-r4c4 4 4 200"
  "cap-r4c8 4 8 400"
  "cap-r1c8 1 8 100"
  "cap-r2c8 2 8 200"
  "cap-r8c8 8 8 400"
  "cap-r1c1 1 1 50"
)

mkdir -p "$OUT"
for cfg in "${CONFIGS[@]}"; do
  read -r label relays consumers start <<< "$cfg"
  echo "=== $label: $relays relay(s), $consumers consumer(s) per channel ==="
  "$BENCH" ramp -confirm -label "$label" -relays "$relays" -consumers "$consumers" \
    -start "$start" -factor 1.25 -step "$STEP" -warmup 5s -window 20s -out "$OUT" 2>&1 | tee -a "$OUT/capacity.log"
done
echo "done"
