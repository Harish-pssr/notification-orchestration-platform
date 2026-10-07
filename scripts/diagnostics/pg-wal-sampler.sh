#!/usr/bin/env bash
# Sample Postgres' own WAL timing about once a second: the cumulative number of WAL fsyncs and
# the total milliseconds spent in them, plus autovacuum workers and sessions waiting on locks.
# Deltas between rows give the average fsync time per interval. Needs, once:
#   ALTER SYSTEM SET track_wal_io_timing = on;  SELECT pg_reload_conf();
# (reset it afterwards with ALTER SYSTEM RESET track_wal_io_timing: it adds overhead).
# usage: pg-wal-sampler.sh out.csv seconds
OUT=$1
SECS=$2
DC="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
echo "epoch,wal_sync,wal_sync_ms,wal_write,wal_write_ms,wal_records,autovacuum_workers,lock_waiters" > "$OUT"
for _ in $(seq "$SECS"); do
  $DC exec -T postgres-platform psql -U notif -d platform -tA -F, -c "
    SELECT round(extract(epoch FROM clock_timestamp())::numeric, 2), wal_sync, round(wal_sync_time::numeric, 1), wal_write, round(wal_write_time::numeric, 1), wal_records,
           (SELECT count(*) FROM pg_stat_activity WHERE backend_type = 'autovacuum worker'),
           (SELECT count(*) FROM pg_stat_activity WHERE wait_event_type = 'Lock')
    FROM pg_stat_wal" >> "$OUT" 2>/dev/null
  sleep 0.2
done
