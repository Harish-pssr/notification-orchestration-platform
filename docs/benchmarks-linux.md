# Benchmarks on a Linux VM

This is the second set of measurements, taken after the laptop set in [benchmarks.md](benchmarks.md) (B1 to B7). The aim was to repeat the same experiments on a native Linux machine with more CPU, to run harder failure injection than the laptop allowed, and to check the explanation of the laptop's latency stalls (B6). It also found and fixed one real defect (section L5).

> **Scope: what is real and what is mocked.** As in [benchmarks.md](benchmarks.md): the pipeline, the load, the injected failures (real `SIGKILL`s, real container stops, a purge of the broker's stream) and the counting are real. **The provider is a mock** that honors idempotency keys, so the claim is **"0 duplicate side effects at a provider that honors idempotency keys"**, not a measurement of real notification delivery. Real email sending was off for every run.
> **Not a speed-up claim.** The VM and the laptop are different machines (16 vCPUs on a cloud VM against a laptop running Docker Desktop). A higher number here says what this VM can do, not what Linux does compared with Windows.

Raw data is in [`docs/data/2026-11-07-linux-vm/`](data/2026-11-07-linux-vm) (the time-series files of the capacity runs are not committed).

## Environment

| | |
|---|---|
| Machine | Google Compute Engine `n2-standard-16`, us-central1-a, standard (not Spot) instance |
| CPU | 16 vCPUs = 8 cores with 2 threads each, Intel Xeon (Cascade Lake) 2.80 GHz, KVM guest; CPU steal 0% |
| Memory | 62 GiB |
| Disk | 50 GB SSD persistent disk (resized to 500 GB for one experiment, L4) |
| OS / runtime | Ubuntu 24.04, Docker Engine, Go 1.27 |
| Layout | everything on the one VM: both databases, the broker, relays, consumers, the mock provider and the load generator |
| Code | commit `6f5e6bf` for the first runs; the consumer fix (L5) is `4021b43`, and L2 (B4) and L3 chaos were repeated on it |

The VM's background services (guest monitoring agent, automatic updates) were stopped before measuring. The same methodology as the laptop runs applies: open-loop load through the HTTP API, exact timestamps from the database, a geometric ramp (about 25% steps) with a majority-vote confirmation, latency measured at 70% of each configuration's capacity, and a baseline configuration run first and last to measure drift.

## L1. Relay scaling (B1 repeated)

20,000 events, relay stage only, three runs per size (events/s):

| Relays | Runs | Median | Speed-up | Laptop median (speed-up) |
|---|---|---|---|---|
| 1 | 554, 847, 854 | 847 | 1.0x | 524 (1.0x) |
| 2 | 1,675, 1,667, 1,668 | 1,668 | 2.0x | 777 (1.5x) |
| 4 | 2,761, 2,751, 2,728 | 2,751 | 3.2x | 1,426 (2.7x) |
| 8 | 4,983, 4,978, 4,996 | 4,983 | 5.9x | 2,148 (4.1x) |

Relay throughput scales close to linearly up to 8 relays on this VM. The first single-relay run (554) is far from the other two; it looks like a cold start, which is an inference, not something I isolated.

## L2. Capacity and latency end to end (B2/B3 repeated)

Confirmed capacity is the highest rate at which a stage was sustained in at least 2 of 3 attempts. The grid step is about 25%, and two runs of the same configuration differed by up to about 20% (see the drift row), so differences smaller than that are not findings.

| Relays / consumers per channel | Confirmed capacity (events/s) | Median latency at 70% (ms) | p99 at 70% (ms) | CPU busy at the ceiling (of 1,600%) |
|---|---|---|---|---|
| 1 / 1 (first run) | 582 | 9 | 170-207 | 847% |
| 4 / 1 | 728 | 14 | 38-151 | 1,156% |
| 4 / 2 | 745 | 11-12 | 21-163 | 1,048% |
| **4 / 4** | **894** (confirmed twice, in the main matrix and in the final-code check) | 19-22 | 125-196 | 1,297% |
| 4 / 8 | 763 | 14 | 51-121 | 1,261% |
| 1 / 8 | 477 | 11-18 | 198-214 | 765% |
| 2 / 8 | 715 | 12 | 91-93 | 1,077% |
| 8 / 8 | 763 | 13 | 87-141 | 1,161% |
| 1 / 1 (last run, about 2 h later) | 466 | 16-43 | 204-211 | 731% |

- **Against the laptop on the same code:** 1/1 was 153 events/s, 4/1 298, 4/4 366, 4/8 313. The VM figures are 2.4 to 3.8 times higher. This is two different machines; it does not isolate the operating system.
- **Drift:** the baseline configuration measured 582 at the start and 466 at the end of the session, 20% lower and one grid step down. Treat every capacity figure here as plus or minus about 20%.
- **Latency at 70% of capacity:** the median stayed between 9 and 43 ms. In 2 of 30 repetitions the load was not sustained (the delivery rate fell behind and a backlog grew steadily), so those repetitions' latencies are unbounded and are reported as failures, not averaged in: configuration 2/8 repetition 2, and the 4/4 final-code check repetition 2 (p50/p95/p99 of 8,103 / 18,831 / 19,391 ms, backlog 11,868). Their sibling repetitions of the same configuration were fine (4/4 final-code: 30/74/100 ms and 14/23/34 ms), which is the same run-to-run variability.
- **Where the ceiling is:** at the 4/4 ceiling the VM was about 1,290% busy out of 1,600% (about 81%), and the platform Postgres alone used 3.6 to 4.5 cores. The ceiling therefore looks CPU-bound on this shared machine. That is a hypothesis consistent with the CPU figures and with L4, not a proof: I did not remove the CPU limit separately.

## L3. Reliability under injected failure (B4 repeated)

100,000 events through the API at 150/s, about 5% of publishes duplicated, 2% of provider calls failing with 503, and 3 relay `SIGKILL`s at random times, on the final code (`4021b43`):

| | Protections on | Negative control (off) |
|---|---|---|
| Events stored | 99,999 | 9,999 |
| Deliveries | 299,997 | 29,997 |
| Delivered / dead-lettered / unfinished | 299,997 / 0 / 0 | 29,997 / 0 / 0 |
| Provider requests (injected errors) | 320,858 (6,395) | n/a |
| **Duplicate side effects** | **0** | **1,530** |
| Time to finish after the load ended | 0.3 s | 2.1 s |

The first run on the earlier code gave the same shape (299,997 delivered, 0 duplicate side effects; control 1,548). The laptop run had 0 and 1,425. The control shows the counters can see duplicates, so the zero is a measurement.

**B5 outages (60 s of relay or of NATS at 250 events/s, 3 repetitions each):** all six passed (45,000 events and 135,000 deliveries delivered each time, 0 dead letters, 0 duplicate side effects). The backlog peaked at about 15,200 events; **recovery took about 20.4 s after the relay returned and 21.4 s after NATS returned**, with a peak delivery rate of about 3,300 to 3,570 per second. On the laptop the same recovery took about 100 s at about 1,300 per second.

### Chaos run: kills of every kind

The laptop run killed only relays. Here consumers and the platform's Postgres were killed too, at random times in the middle 80% of the load (Postgres down for 5 s each time unless stated).

| | First run (earlier code, 150/s) | Final code, 500/s | Final code, long outage |
|---|---|---|---|
| Events planned | 150,000 | 300,000 | 60,000 at 150/s |
| Kills | 15 relay, 6 consumer, 2 Postgres | 15 relay, 6 consumer, 2 Postgres | 2 Postgres, each down 45 s |
| Accepted / rejected by the API | 147,396 / 2,604 | 289,894 / 10,106 | 45,674 / 14,319 (+7 transport errors) |
| Events stored | 147,396 | 289,896 | 45,674 |
| Deliveries delivered / dead-lettered | 442,188 / 0 | 869,688 / 0 | 137,022 / 0 |
| Provider requests | 472,848 | 929,843 | n/a |
| Duplicate side effects | 0 | 0 | 0 |
| Time to finish after the load ended | **485 s** | 0.7 s | 2.3 s |

Requests rejected while Postgres was down are not lost events (the client got an error and would retry). A request that fails with a transport error has an **unknown** outcome, because the commit may have happened before the connection died. The judge therefore accepts a stored count between "accepted" and "accepted + unknown", and nothing else: in the 500/s run, 289,896 were stored against 289,894 accepted, so 2 of the 10,106 unknown requests had committed. Every other check is exact.

## L4. Disk experiment

To test whether storage limited the ceiling, the boot disk was resized from 50 GB to 500 GB (a larger persistent disk is faster) and three configurations were repeated.

| | 50 GB | 500 GB |
|---|---|---|
| fio, 1 writer, flushed writes/s | 850 | 930 |
| fio, 16 writers, flushed writes/s | 6,715 | 9,131 |
| fio, raw 4 KiB random write, IOPS | 7,615 | 23,400 |
| Capacity 1/1 | 582 | 582 |
| Capacity 4/4 | 894 | 894 |
| Capacity 4/8 | 763 | 954 (one grid step, within the plus or minus 20%) |

A three-times faster raw disk did not move the ceiling in two configurations and moved it by one grid step in the third. Together with the CPU figures in L2, this points at CPU rather than the disk, but it is not proof.

## L5. A defect found by the Postgres-kill chaos run, and its fix (decision D23)

The first chaos run passed every correctness check but took **485 s** to finish after the load ended, where the others took under a second. That equals the reconciler's 10-minute wait after the second Postgres kill, so something had been lost that only the reconciler recovered.

1. **Diagnosis.** A smaller instrumented run (40,000 events, 2 Postgres kills of 5 s) left 11 deliveries stuck in `RETRYING`, attempts 1, until the reconciler repaired 5 and 6 of them exactly 10 minutes after each kill. The consumer log of a stuck delivery showed attempt 1 with an injected 503, then attempts 2 to 8 each failing with "load delivery failed" about a second apart, then nothing. While the database was down, the consumer gave each message back to the broker after every failed query with a fixed 1 s delay; every redelivery counts toward the broker's delivery limit (`MaxDeliver` = `MaxAttempts` + 3 = 8), so 8 seconds of outage used all eight and the broker dropped the message without telling anyone. This is the same family as D18 (a dependency outage consuming attempts).
2. **First fix, which failed.** Exponential back-off after a database error made the drain instant, but a re-test dead-lettered 1 healthy delivery as "retries exhausted: provider returned 503" with only 2 real provider attempts: database errors had still been counted as attempts.
3. **Final fix.** The consumer now holds the message while the database is down, retrying the query in place with growing pauses and telling the broker "still working" (`InProgress`), for up to `DB_WAIT` (default 2 minutes). No redelivery happens, so no attempt is spent. If the outage lasts longer, the message is given back with a growing delay as before.
4. **Verified.** The same 40,000-event run: 38,097 stored = accepted, 114,291 delivered, 0 dead letters, 0 duplicates, **0 reconciler repairs**, drain 0.1 s. Then on the final code, the 500/s chaos run and two 45-second outages (L3) passed with no repairs and no dead letters. Five new tests cover it, including one that cuts a real database connection through a proxy for about a second and asserts the delivery completes as attempt 1 and is never redelivered; the unit tests fail without the fix. The capacity spot check after the fix gave the same 894 events/s.

Limit: this is verified for outages up to 45 s, and held for up to `DB_WAIT` (2 minutes) by design. Longer outages fall back to the old give-back path, and the reconciler (D22) is the safety net behind it.

## L6. The reconciler under load (B7 repeated)

Consumers paused for 8 s under 150 events/s, the stream purged (simulated broker loss), reconciler wait shortened to 30 s, three repetitions, all passed: 3,263 / 3,231 / 3,293 deliveries repaired (each at most once), 22,500 events and 67,500 deliveries delivered each time, 0 dead letters, 0 duplicate side effects, recovery about 35 s. The laptop gave 3,216 to 3,243 repaired.

## L7. The latency stalls (B6 repeated)

Two 10-minute runs at 150 events/s with WAL timing sampled. **No stall episodes occurred in either run.** The WAL flush averaged 0.49 to 0.50 ms (laptop: 2.2 ms with dips of 7 to 10 ms), about 425 flushes per second, and the worst sample averaged 1.5 to 1.7 ms. End-to-end p50/p95/p99 was 102 / 191 / 208 ms in both runs (laptop p99: 2.2 to 2.3 s); the 102 ms median is the relay's poll interval at light load, not a slow path.

This is consistent with the B6 explanation that the stalls came from the laptop's Docker Desktop storage path. It is still an inference from a different machine not showing them; it does not prove the cause.

## L8. Production setup check

The production overlay (Caddy with HTTPS, the admin token required for writes) was brought up on the VM with `DOMAIN=localhost` and Caddy's local certificate authority. It was **not** exposed publicly (D20). Result: all services healthy; the smoke test passed (health 200, event creation without the token 401, idempotent replay, event published and fully delivered, fault toggle without the token 403, replay without the token 401); only ports 80 and 443 were published, and the database, broker and API ports were not reachable from the host; plain HTTP redirected to HTTPS (308). One caveat: `scripts/deploy.sh` waits for readiness with `curl` without `-k`, so it would not pass against a local certificate authority; the compose and smoke steps were run by hand.

## What the VM data does and does not show

- **It shows:** near-linear relay scaling to 8 relays; an end-to-end ceiling of roughly 470 to 900 events/s for this code on this VM; zero duplicate side effects and zero lost or dead-lettered deliveries through 23 `SIGKILL`s at 500 events/s and through two 45 s database outages; recovery from a 60 s relay or broker outage in about 20 s at 28% of capacity.
- **It does not show:** that Linux is faster than Windows, that the ceiling is CPU rather than disk beyond the evidence above, or anything about real email delivery.
- **Variability:** plus or minus about 20% run to run; 2 of 30 latency repetitions at 70% load were not sustained.
- **Single machine, single session:** the load generator and the provider share the VM's CPU with the platform, so the platform's own capacity is understated.
- **Cost and cleanup:** about 8 hours on one VM, paid from free credit; the VM and its disk were deleted afterwards.
