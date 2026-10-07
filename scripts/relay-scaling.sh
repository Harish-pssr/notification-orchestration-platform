#!/usr/bin/env bash
# Relay-scaling experiment: how fast do R relays publish N pre-seeded events?
#
# Method, per run:
#   1. reset all data and purge the stream
#   2. start R relays (fresh containers) and wait until every one is ready
#   3. seed N events that only become due after SEED_DELAY, so all relays are warm
#      and idle and start claiming at the same moment
#   4. wait until every event is PUBLISHED
# Consumers are stopped, so only the relay stage is measured. All timings are read
# from the database (first to last published_at), never from this script's clock.
#
#   N=20000 RUNS=3 COUNTS="1 2 4 8" scripts/relay-scaling.sh
set -euo pipefail
cd "$(dirname "$0")/.."

N=${N:-20000}
RUNS=${RUNS:-3}
COUNTS=${COUNTS:-"1 2 4 8"}
SEED_DELAY=${SEED_DELAY:-6}
OUT=benchmark-results
CSV=$OUT/relay-scaling.csv
COMPOSE="docker compose -f docker-compose.yml -f docker-compose.dev.yml"

mkdir -p "$OUT" bin
go build -o bin/loadgen ./cmd/loadgen
# APPEND=1 resumes an interrupted experiment (use with COUNTS and RUN_START) instead of starting a new CSV.
RUN_START=${RUN_START:-1}
if [ "${APPEND:-0}" != "1" ]; then
  echo "relays,run,events,published,window_s,events_per_s,max_attempts,dlq,delivered,relay_cpu_pct,postgres_cpu_pct,nats_cpu_pct" > "$CSV"
fi

$COMPOSE stop consumer-email consumer-push consumer-webhook > /dev/null

for R in $COUNTS; do
  for run in $(seq "$RUN_START" "$RUNS"); do
    $COMPOSE stop relay > /dev/null
    bin/loadgen reset -confirm > /dev/null

    # Fresh containers so their logs only contain this run, then wait until all R are ready.
    $COMPOSE up -d --no-deps --force-recreate --scale relay="$R" relay > /dev/null 2>&1
    for _ in $(seq 60); do
      ready=$($COMPOSE logs relay 2>/dev/null | grep -c '"relay started"' || true)
      [ "$ready" -ge "$R" ] && break
      sleep 1
    done
    [ "$ready" -ge "$R" ] || { echo "only $ready of $R relays became ready" >&2; exit 1; }

    bin/loadgen seed -n "$N" -delay "${SEED_DELAY}s" > /dev/null

    # One CPU snapshot shortly after the events become due.
    ( sleep $((SEED_DELAY + 1)); docker stats --no-stream --format '{{.Name}} {{.CPUPerc}}' > "$OUT/stats.tmp" ) &
    bin/loadgen wait -n "$N" -timeout 10m
    wait

    result=$(bin/loadgen report -csv)
    # CPU% is relative to one core, so values above 100 mean more than one core.
    relay_cpu=$(grep -E 'relay-[0-9]+ ' "$OUT/stats.tmp" | awk '{gsub("%","",$2); s+=$2} END {printf "%.0f", s+0}')
    pg_cpu=$(grep 'postgres-platform' "$OUT/stats.tmp" | awk '{gsub("%","",$2); printf "%.0f", $2+0}')
    nats_cpu=$(grep -E 'nats-1 ' "$OUT/stats.tmp" | awk '{gsub("%","",$2); printf "%.0f", $2+0}')

    echo "$R,$run,$N,$result,$relay_cpu,$pg_cpu,$nats_cpu" | tee -a "$CSV"
  done
done

# Leave the stack as we found it: no leftover backlog, normal services running.
$COMPOSE stop relay > /dev/null
bin/loadgen reset -confirm > /dev/null
$COMPOSE up -d --scale relay=1 --remove-orphans > /dev/null 2>&1
echo "results: $CSV"
