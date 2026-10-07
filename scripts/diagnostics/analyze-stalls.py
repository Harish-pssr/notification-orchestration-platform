#!/usr/bin/env python3
"""Find throughput dips in a benchmark's monitor CSV and compare them with Postgres' own WAL
fsync timing (from pg-wal-sampler.sh).

usage: analyze-stalls.py monitor.csv walsample.csv

A dip is a second in which deliveries completed at under 60% of the run's median rate.
Dips less than 3 s apart form one episode. For every episode the script prints the number of
WAL fsyncs and their average duration inside it, next to the run's overall figures.
"""
import csv
import statistics
import sys

monitor, walsample = sys.argv[1], sys.argv[2]

rows = list(csv.DictReader(open(monitor, encoding="utf-8")))
t = [float(r["db_epoch_s"]) for r in rows]
delivered = [int(r["deliveries_delivered"]) for r in rows]
rate = [(t[i], (delivered[i] - delivered[i - 1]) / (t[i] - t[i - 1])) for i in range(1, len(t))]
median = statistics.median([r for _, r in rate if r > 0])
print(f"run {t[-1] - t[0]:.0f} s, median delivery rate {median:.0f}/s")

dips = [ts for ts, r in rate if r < median * 0.6 and t[0] + 3 < ts < t[-1] - 3]
episodes = []
for ts in dips:
    if episodes and ts - episodes[-1][1] <= 3:
        episodes[-1][1] = ts
    else:
        episodes.append([ts, ts])
print("dip episodes (start-end, seconds into the run):", [(round(a - t[0]), round(b - t[0])) for a, b in episodes])

wal = list(csv.DictReader(open(walsample, encoding="utf-8")))
wt = [float(r["epoch"]) for r in wal]
syncs = [int(r["wal_sync"]) for r in wal]
sync_ms = [float(r["wal_sync_ms"]) for r in wal]
intervals = []
for i in range(1, len(wal)):
    n, ms = syncs[i] - syncs[i - 1], sync_ms[i] - sync_ms[i - 1]
    intervals.append((wt[i], n, ms, ms / n if n > 0 else 0.0))
averages = sorted(x[3] for x in intervals if x[1] > 0)
print(f"WAL fsyncs: typical interval average {statistics.median(averages):.2f} ms, "
      f"99th percentile {averages[int(len(averages) * 0.99)]:.1f} ms, worst {averages[-1]:.1f} ms")
total_n, duration = syncs[-1] - syncs[0], wt[-1] - wt[0]
print(f"overall: {total_n / duration:.0f} fsyncs/s, average {(sync_ms[-1] - sync_ms[0]) / total_n:.2f} ms")
for a, b in episodes:
    inside = [x for x in intervals if a - 1 <= x[0] <= b + 1]
    if not inside:
        continue
    n, ms = sum(x[1] for x in inside), sum(x[2] for x in inside)
    print(f"  episode {a - t[0]:5.0f}-{b - t[0]:.0f} s: {n / len(inside):.0f} fsyncs per sample, "
          f"average {ms / max(1, n):.2f} ms, worst sample average {max(x[3] for x in inside):.1f} ms")
