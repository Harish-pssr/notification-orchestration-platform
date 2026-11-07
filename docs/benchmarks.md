# Benchmarks

> **Scope: what is real and what is mocked.**
> **Real:** the pipeline (two PostgreSQL databases, NATS JetStream, relays, consumers), the load (sent through the HTTP API), the injected failures (real `SIGKILL`s, real container stops, a real purge of the broker's stream) and the counting (exact, from database timestamps and the provider's tables).
> **Mocked:** the provider. It stands in for the email, push and webhook services; it has its own database, and an "effect" is a row it records, not a delivered email, push or webhook call. It **honors idempotency keys** (it writes the key and the effect in one transaction).
> So the headline claim is **"0 duplicate side effects at a provider that honors idempotency keys"**. It is not a measurement of real notification delivery, and it is not "0 duplicate notifications". For a provider that does not honor keys (for example a real SMTP server, the optional real email mode, D21) the guarantee is at-least-once with a rare duplicate. No benchmark here was run with real sending, and the tools refuse to run while it is on.
> **Environment:** every result on this page is from one Windows laptop under Docker Desktop; each section records its own environment. The same experiments on a Linux VM, with harder failure injection, are in [benchmarks-linux.md](benchmarks-linux.md). Broker loss is simulated by purging the stream, not by a disk failure.

Every number here was measured. Raw data for each experiment is kept in [`docs/data/`](data) (the scripts write to `benchmark-results/`, which is not committed). Nothing is estimated. Each section states its environment, dataset, method and limitations.

## B1. Relay scaling (relay stage alone)

**Question:** how fast do N relay workers publish a backlog of outbox events to the broker, with nothing else competing?

This section was **re-measured** on mains power with the current tooling, which records the environment. The first measurement is kept at the end of this section as superseded, with the reasons.

### Environment

| | |
|---|---|
| Machine | Intel Core i5-12500H, 16 logical CPUs, laptop |
| Power | **AC power**, plan "Balanced", clock 2500 MHz (recorded in `environment.json`) |
| Container runtime | Docker Desktop on Windows; Linux VM with 16 CPUs and 7.6 GB |
| Software | Go 1.27.0; PostgreSQL 17 (alpine, `max_connections=300`); NATS 2 with JetStream (file storage) |
| Relay settings | batch 10 events per claim, lease 5 s (dev), idle poll 200 ms, publish timeout 5 s |
| Platform code | the current code, with the relay's metrics and the broker-outage handling (D18) |
| Load tool | `cmd/loadgen` on the Windows host, talking to the published database ports (the relay itself talks to Postgres and NATS inside the Compose network) |
| Captured | 2026-10-07 |

This is Docker Desktop on Windows (WSL2), not a native Linux server. The numbers describe this environment only.

### Dataset and method

- 20,000 `PaymentSucceeded` events, each routed to 3 channels, so **60,000 broker messages per run**. Events are inserted directly into `outbox_events` and `channel_deliveries`, bypassing the HTTP API, so the relay stage is measured on its own.
- Consumers are stopped, so nothing competes for CPU and the stream simply accumulates messages.
- Per run: reset all data and purge the stream; start R fresh relay containers and wait until all are ready; seed the events with a due time 6 s in the future (so all relays are idle and warm and start together); wait until every event is `PUBLISHED`.
- **Throughput = published events divided by the window from the first to the last `published_at`**, both read from the database. No clock from the load tool is involved.
- 3 runs for each of 1, 2, 4 and 8 relays. Script: `scripts/relay-scaling.sh` (`N=20000 RUNS=3 COUNTS="1 2 4 8"`). Raw data: `docs/data/2026-10-07-relay-scaling/`.

### Results

Events published per second (each event is 3 broker messages):

| Relays | Run 1 | Run 2 | Run 3 | Median | Speed-up vs 1 relay | Per-relay median |
|---|---|---|---|---|---|---|
| 1 | 438 | 524 | 542 | **524** | 1.0x | 524 |
| 2 | 793 | 733 | 777 | **777** | 1.5x | 389 |
| 4 | 1,409 | 1,438 | 1,426 | **1,426** | 2.7x | 357 |
| 8 | 2,438 | 2,148 | 2,128 | **2,148** | 4.1x | 269 |

At the median, 8 relays published about 2,148 events (about 6,444 messages) per second. In every run all 20,000 events were published, none was dead-lettered, and every event needed exactly one attempt.

CPU snapshot taken about one second after the backlog became due (100 means one full core; a single instantaneous sample per run, so only indicative):

| Relays | Relay processes (total) | Postgres | NATS |
|---|---|---|---|
| 1 | 15 to 20% | 19 to 36% | 12 to 14% |
| 2 | 27 to 35% | 23 to 30% | 17 to 23% |
| 4 | 67 to 71% | 50 to 64% | 33 to 38% |
| 8 | 145 to 167% | 130 to 169% | 58 to 63% |

### What the data shows

- **Scaling is real but sub-linear:** 1.5x with 2 relays, 2.7x with 4, 4.1x with 8, so each added relay contributes less than the one before. Per-relay throughput falls from 524 to 269 events/s.
- **A single relay is fast on its own:** 524 events/s, claiming 10 at a time, with the relay process using under a fifth of one core. It spends its time waiting on round trips to Postgres (the claim, and one `UPDATE` per event to finish it) and to NATS.
- **No component was CPU-saturated.** At 8 relays the relay processes use about 1.5 cores and Postgres about 1.3 to 1.7 cores out of 16. The shared database and the commit path, not raw CPU, are the likely limit as relays are added; this was not isolated by an experiment here.
- **Run-to-run spread is small except at 8 relays** (2,128 to 2,438), consistent with the machine being near its shared limits.

### Limitations

- One machine; the relay, Postgres, NATS and the Docker VM share the same CPU. Results will differ on a native Linux server.
- The relay stage only. End-to-end throughput and latency are in B2 to B3.
- Events were seeded in bulk, not created through the API.
- The CPU figures are single snapshots, not time series.
- 3 runs per configuration; medians are reported with all runs shown.

### The earlier measurement of B1 (superseded)

The first measurement of this experiment (2026-10-06, `docs/data/relay-scaling-2026-10-06.csv`) gave medians of 272, 670, 1,225 and 2,092 events/s for 1, 2, 4 and 8 relays (speed-ups 1.0x, 2.5x, 4.5x, 7.7x). It is **superseded** and should not be used: the machine's power state was not recorded then, and later work found that running on battery produced unstable results (see B2). The single-relay figure in particular (272/s) is about half of the 524/s measured here on mains power, while the 8-relay figure is about the same, which is what produced the apparent super-linear scaling. The explanation offered at the time for the higher per-relay rate with more relays (Postgres grouping concurrent commits) was a hypothesis that this re-measurement does not support. The re-measurement also uses the current, instrumented code.

### Notes on how the data was produced

- An earlier design of this experiment started the relays and seeded events at the same moment, which produced unreliable results (relay start-up skew dominated short runs). That design was discarded; the delayed-due-time method above removes the skew.
- The tool now records the environment and refuses to run on battery power (`bench env`, called by the script).

## B2. End-to-end capacity and latency through the HTTP API (baseline)

**Question:** how many events per second can the whole pipeline sustain (API, outbox, relay, broker, consumers, provider) for different numbers of relays and consumers, and what latency do events see at a comfortable load? This is the baseline that the optimization exercise is compared against.

### Environment

| | |
|---|---|
| Machine | 12th Gen Intel(R) Core(TM) i5-12500H, 16 logical CPUs (hybrid: 4 performance + 8 efficiency cores), laptop |
| Power | **AC power**, plan "Balanced" (full speed on mains power), clock 2500 MHz |
| Containers | Docker Desktop on Windows, Linux VM with 16 CPUs and 7.6 GB |
| Software | Go 1.27.0; PostgreSQL 17 (alpine; defaults except `max_connections=300`); NATS 2 with JetStream; Prometheus scraping every 5 s (Grafana off) |
| Platform code | commit `b90baa3` (the platform's behaviour is unchanged since the metrics slice) |
| Captured | 2026-10-07T07:33:35.5000298Z |

### Method

- **Open-loop load through the real HTTP API**, generated by a container on the Compose network (so Docker Desktop's host port forwarding is not measured). Requests are sent at a fixed rate whatever the system's answers; API latency is measured from each request's **intended** send time and the generator reports its own scheduling lag (p99 under 2 ms in the runs here, so it was never the bottleneck). Every request carries a unique `Idempotency-Key`.
- **One event type** (PaymentSucceeded: one event fans out to 3 channel deliveries, so 313 events/s means about 940 deliveries per second).
- **A rate is "sustained"** only if all of these hold: the generator kept up; the API accepted every request; API p99 latency (from intended send time) is under 1 s; the backlog at the end of the load is at most two seconds of arrivals; and at least 99% of the events in the measurement window were fully delivered.
- **Three stages per configuration:** (1) a ramp, geometric steps of x1.25 from a start rate, 30 s each, to find the neighbourhood; (2) **confirmation** with repeated 60 s steps and a majority vote (a rate must be sustained in at least 2 of up to 3 repetitions), walking up from the ramp's answer while rates hold and down if it does not; (3) **three 60 s repetitions at 70% of the confirmed capacity** for latency.
- **Every figure comes from database timestamps** (database clock), not the generator's clock. End-to-end latency is event created to the **last** of its deliveries. Percentiles are exact over every event created in the measurement window (10 s warm-up discarded, then 40 s), nothing sampled or bucketed.
- The 1-relay, 1-consumer configuration is measured **first and last** as a drift control.
- **118 measured steps** in one session of about two hours. Raw data: `docs/data/2026-10-07-capacity/`.

### Capacity: the highest rate sustained (events/s)

| Relays | Consumers per channel | Ramp alone suggested | **Confirmed capacity** | Confirmation (rate: sustained runs / runs) |
|---|---|---|---|---|
| 1 | 1 (measured first) | 98 | **98** | 98: 2/3, 122: 0/2 |
| 4 | 1 | 98 | **98** | 98: 2/2, 122: 0/2 |
| 4 | 2 | 156 | **156** | 156: 2/2, 195: 0/2 |
| 4 | 4 | 234 | **234** | 234: 2/2, 293: 0/2 |
| 4 | 8 | 391 | **313** | 391: 0/2, 313: 2/2 |
| 1 | 8 | 195 | **156** | 195: 1/3, 156: 2/2 |
| 2 | 8 | 293 | **293** | 293: 2/2, 366: 0/2 |
| 8 | 8 | 250 | **313** | 250: 2/2, 313: 2/2, 391: 0/2 |
| 1 | 1 (measured last) | 98 | **98** | 98: 2/3, 122: 0/2 |

Two things the confirmation stage caught that a single ramp would have reported wrongly: for 4 relays and 8 consumers the ramp suggested 391/s, but it was sustained in 0 of 2 runs, and the confirmed capacity is 313; for 1 relay and 8 consumers the ramp suggested 195/s, sustained in only 1 of 3 runs.

### Latency at 70% of each configuration's capacity (milliseconds, median of 3 repetitions, range in brackets)

| Relays | Consumers | Arrival rate | End-to-end p50 | End-to-end p95 | End-to-end p99 | Created to published p50 / p99 | API p99 |
|---|---|---|---|---|---|---|---|
| 1 | 1 (measured first) | 69/s | 159 (158 to 171) | 261 (260 to 1570) | 290 (286 to 2169) | 98 / 225 | 8 |
| 4 | 1 | 69/s | 140 (137 to 146) | 269 (267 to 309) | 302 (302 to 809) | 89 / 211 | 44 |
| 4 | 2 | 109/s | 82 (77 to 83) | 200 (190 to 206) | 256 (246 to 263) | 22 / 183 | 10 |
| 4 | 4 | 164/s | 31 (30 to 32) | 48 (47 to 73) | 65 (62 to 3493) | 20 / 47 | 12 |
| 4 | 8 | 219/s | 44 (44 to 47) | 87 (86 to 343) | 112 (111 to 611) | 30 / 94 | 17 |
| 1 | 8 | 109/s | 93 (84 to 100) | 212 (210 to 214) | 236 (235 to 240) | 83 / 227 | 10 |
| 2 | 8 | 205/s | 50 (49 to 59) | 98 (95 to 3044) | 129 (126 to 3578) | 37 / 104 | 15 |
| 8 | 8 | 219/s | 39 (39 to 45) | 78 (72 to 2333) | 119 (96 to 2815) | 25 / 82 | 18 |
| 1 | 1 (measured last) | 69/s | 165 (164 to 167) | 265 (263 to 269) | 297 (292 to 301) | 99 / 228 | 9 |

### Where the CPU goes (mean over the confirmation runs at the highest confirmed rate; 100 = one core, the VM has 16 = 1,600)

| Configuration | Total | Platform Postgres | Mock provider + its Postgres | Consumers (all) | API | NATS | Relay (all) | Load generator |
|---|---|---|---|---|---|---|---|---|
| r1c1 at 98/s | 154 | 33 | 53 | 27 | 14 | 13 | 6 | 9 |
| r1c8 at 156/s | 460 | 100 | 144 | 99 | 41 | 35 | 20 | 21 |
| r4c4 at 234/s | 721 | 165 | 236 | 142 | 66 | 48 | 36 | 28 |
| r2c8 at 293/s | 920 | 217 | 286 | 200 | 78 | 65 | 40 | 32 |
| r4c8 at 313/s | 941 | 222 | 291 | 204 | 78 | 66 | 45 | 33 |
| r8c8 at 313/s | 989 | 238 | 302 | 213 | 80 | 70 | 52 | 33 |

### What the data shows

1. **The baseline (1 relay, 1 consumer per channel) sustains 98 events/s**, and the drift control agrees: **98 at the start and 98 at the end** of the session, with matching latency (p50 159 vs 165 ms).
2. **A single consumer is the first bottleneck.** At the baseline load the stage means (from the metrics) are: consumer-to-provider call 6.2 ms (the provider's own work 3.3 ms), the consumer's own database read and status update roughly 3 ms more, so one consumer handles one message in about 9 to 10 ms, which is about 100 messages per second. That matches the measured 98 events/s (each event puts one message on each channel's consumer). The relay is not the constraint here: its finish update is 2.9 ms per event, publish 0.45 ms, claim 3.3 ms per batch of up to 10.
3. **Consumer scaling:** with 4 relays, capacity grows 98, 156, 234, 313 events/s at 1, 2, 4, 8 consumers per channel: close to linear up to 4, then flattening.
4. **Relay scaling:** with 8 consumers per channel, 1, 2, 4 and 8 relays sustain 156, 293, 313 and 313 events/s. One relay limits the system (it shares the database with everything else, so it manages about 150 events/s here, well below the 524 events/s it reached alone in B1 (relay stage only, nothing else competing)); from 2 relays on, adding relays changes little.
5. **The ceiling is about 313 events/s (about 940 deliveries/s) and it is a limit of the shared resources, not of one component.** At the ceiling the whole VM is about 60% busy (941 to 989 of 1,600), the platform's Postgres is the hottest component (222 to 238), the mock provider plus its own Postgres take about 290 (roughly 30% of everything), the consumers about 205, and the relay only 45 to 52. The ceiling is the same with 4 or 8 relays and with 4 or 8 consumers. On this hybrid CPU, hyperthread siblings and efficiency cores share compute, so the machine saturates well below 100% of its logical capacity.
6. **Latency at 70% load is low:** end-to-end p50 between 31 and 165 ms and p99 between 65 and 302 ms in the typical (median) repetition. It is best at 4 consumers per channel (p50 31 ms, p99 65 ms at 164 events/s). With one consumer, about 100 ms of the roughly 160 ms is the time from creation to publication by the relay (p50 98 ms), which is consistent with the relay's 200 ms idle polling interval (an event waits half a poll interval on average at low load). That explanation is a hypothesis to be tested in the optimization exercise, not yet verified.
7. **Occasional multi-second stalls remain.** In 4 of 27 latency repetitions the p99 exceeded 2 s (up to 3.6 s), although the medians are tight. The same kind of intermittent stall was seen once in an earlier, battery-powered run. Its cause was investigated afterwards (B6): periods of about 4 s, roughly every 2 minutes, in which Postgres' WAL fsync is 3 to 5 times slower (checkpoints, autovacuum and lock contention were ruled out); why the flush slows is not established, and the likely place is this environment's storage path.

### Limitations

- One machine, one session, one run of the matrix (with repetitions inside each stage). Results describe this laptop in this configuration and will differ on a native Linux server.
- The mock provider and its database consume about 30% of the CPU at the ceiling. A real external provider would not run on this machine, so the platform's own capacity here is understated. The numbers include that cost.
- A hybrid, hyperthreaded CPU on a laptop: its effective capacity at high load is not the sum of its logical CPUs.
- Only PaymentSucceeded events (3 channels). A mixed workload would change the fan-out.
- `created_at` is the start of the creating transaction, slightly earlier than the commit; for a transaction of about 5 ms this bias is small.
- Prometheus (scraping every 5 s) and the Go processes share the CPU with the system under test.
- The unexplained stalls above add run-to-run variation, shown in the ranges.

### How this data was produced (a correction)

A first attempt at this matrix was **discarded**. Its results were inconsistent (a configuration that passed one hour failed the next, and one step stalled for about 20 s without reproducing), and the cause turned out to be the machine: the laptop was **running on battery** with a power-limited CPU. The benchmark tool now records the machine's power source, power plan and clock with the results and **refuses to run on battery**. The figures in this section were all measured on mains power after that fix. The earlier relay-only measurement (B1) predates the environment capture, so its power state was unknown; it has since been re-measured with the current tooling and the old table is marked superseded.

## B3. Optimization: concurrent message processing in each consumer (before and after)

**Question:** the baseline (B2) showed one consumer per channel is the first bottleneck (about 100 messages/s, because each message is handled serially: database read, provider call, database write). Does processing several messages at the same time inside one consumer raise capacity, and by how much?

**Change:** each consumer instance now handles up to 8 messages at once (`CONSUMER_CONCURRENCY`, default 8) using a bounded worker pool. A slot is claimed before each fetch and the fetch asks for no more messages than there are free slots, so a slow message holds nothing else back. Nothing else changed: acknowledgement still follows the database write, and duplicates are still suppressed by the provider's idempotency key. A test checks that a stuck message does not delay the others. Code: commit `bfe6ac3`.

**Method:** identical to B2 (same machine, AC power, "Balanced" plan, same protocol, same ladder factor of 1.25, same confirmation and latency stages). Only four of B2's nine configurations were re-run. Raw data: `docs/data/2026-10-07-d15-concurrent-consumer/`.

### Confirmed capacity (events/s)

| Relays | Consumers per channel | Before (B2) | After | Change | After: confirmation (rate: sustained runs / runs) |
|---|---|---|---|---|---|
| 1 | 1 | 98 | **153** | +56% | 191: 1/3, 153: 2/2 |
| 4 | 1 | 98 | **298** | about 3x | 373: 0/2, 298: 2/2 |
| 4 | 4 | 234 | **366** | +56% | 366: 2/3, 458: 0/2 |
| 4 | 8 | 313 | **313** | none | 313: 2/3, 391: 0/2 |

### Latency at 70% of each configuration's own capacity (milliseconds, median of 3 repetitions)

The arrival rates differ between before and after because capacity differs, so these rows are **not** a like-for-like latency comparison.

| Relays | Consumers | After: rate | After p50 | After p95 | After p99 | Before: rate | Before p50 / p95 / p99 |
|---|---|---|---|---|---|---|---|
| 1 | 1 | 107/s | 106 | 217 | 240 | 69/s | 159 / 261 / 290 |
| 4 | 1 | 209/s | 39 | 79 | 108 | 69/s | 140 / 269 / 302 |
| 4 | 4 | 256/s | 47 | 118 | 211 | 164/s | 31 / 48 / 65 |
| 4 | 8 | 219/s | 41 | 79 | 103 | 219/s | 44 / 87 / 112 |

### What the data shows

1. **Concurrency inside a consumer gives roughly what extra consumer processes gave.** 4 relays with 1 consumer per channel (now 8 messages at a time) sustains 298 events/s; before the change it took 8 separate consumers per channel to reach 313. One process per channel replaces eight.
2. **It does not remove the machine-wide ceiling.** 4 relays with 8 consumers (64 messages in flight per channel) sustains the same 313 events/s as before, with the same latency at the same load (p50 41 vs 44 ms, p99 103 vs 112 ms) and the same platform Postgres CPU (221 vs 222 of 1,600). Beyond a point, more concurrency adds nothing.
3. **The 4 relay, 4 consumer configuration reached 366 events/s (before: 234).** That is partly the expected scaling of the same effect (4 consumers now means 32 messages at a time). The step between 313 and 366 is **smaller than the 25% ladder step** and the two sweeps used different starting rates, so the data does not show that the ceiling itself moved; it shows 313 and 366 were each the highest rate in their own ladder that passed. Treat the ceiling as roughly 310 to 370 events/s.
4. **1 relay, 1 consumer is now limited by the relay, not the consumer.** It reaches 153 events/s, essentially the 156 events/s that B2 measured for one relay with 8 consumers. The consumer is no longer the first bottleneck.
5. **Latency:** low-load behaviour is unchanged where it was relay-bound (the roughly 100 ms created-to-published floor from the 200 ms poll, decision D14). Higher capacity does not by itself change latency at equal load; the like-for-like row (4 relays, 8 consumers, 219/s) is within a few milliseconds of before.
6. **Intermittent stalls remain.** In 2 of 12 latency repetitions the p99 was 3.5 to 3.7 s while the median was normal (before: 4 of 27). The cause was investigated afterwards (B6: slow WAL fsync episodes, likely this environment's storage) and this change did not address it.

### Limitations

- One run per configuration with the repeated-measurement protocol inside it; the confirmed values are one rate step wide (25%).
- Only four of the nine B2 configurations were re-run (the relay-scaling rows with 8 consumers per channel and the 2 and 8 relay rows were not).
- The before and after sweeps started their ladders at different rates, which affects where in a 25% band the confirmed value lands.
- The mock provider and its database still take a large share of the CPU, so this machine's platform capacity is understated.
- Not tested: the latency effect of a slow provider, where concurrency should matter most because slow calls no longer queue behind each other.

## B4. Reliability under injected failure (100,000 events) and negative control

**Question:** with duplicate messages, crashing relays and failing provider calls happening during load, does every accepted event get delivered exactly once in effect: nothing lost, nothing applied twice?

### Environment and method

Same machine and conditions as B2 (AC power, Docker Desktop, 16 CPUs). Code: commit `6f0cc3f` (the platform's behaviour is unchanged since the broker-outage fix, decision D18). Run with `scripts/bench-reliability.sh`; raw reports are in `docs/data/2026-10-07-reliability/`.

- **Load:** 100,000 PaymentSucceeded events through the HTTP API at 150 events/s, open loop (about 11 minutes), with 4 relays and 4 consumers per channel (each consumer handles 8 messages at once). 150/s is below the measured capacity of this configuration (366/s, B3) on purpose: the run tests correctness under failure, not peak throughput.
- **Duplicates:** the relay publishes a second copy, with fresh message ids that the broker's own de-duplication cannot absorb, for about 5% of the events it handles.
- **Relay crashes:** 3 real `docker kill` (SIGKILL) of a random relay container at random times inside the load; the relay was restored after each. A kill can strike at any instruction, including between publishing and recording success.
- **Provider errors:** about 2% of provider requests answered with HTTP 503 before any effect ran (retried with backoff).
- **Counting:** exact, from both databases after the system drained. "Duplicate side effects" is the number of provider effect rows minus the number of distinct idempotency keys among them. Requests answered with an injected error are subtracted from the duplicate counts (they are retries, not duplicates).
- **Pass criteria:** every accepted event is stored; every delivery is DELIVERED; zero dead letters; exactly one provider effect per delivery; zero duplicate side effects.
- **Negative control:** 10,000 events with the same duplicate and error injection, but with the provider's idempotency protection off and the consumers' skip-if-already-delivered shortcut off. It must show duplicate side effects, otherwise the counters have not been shown to detect them.

### Results

| | Main run (protections on) | Negative control (protections off) |
|---|---|---|
| Events accepted by the API | 99,999 (0 rejected, 0 errors) | 9,999 |
| Events stored | 99,999 | 9,999 |
| Deliveries (3 per event) | 299,997 | 29,997 |
| Delivered / dead-lettered / unfinished | 299,997 / 0 / 0 | 29,997 / 0 / 0 |
| Deliveries that needed a retry | 5,764 | 539 |
| Relay crashes (SIGKILL) | 3 | 0 |
| Provider requests | 321,078 | 32,028 |
| Provider requests answered with an injected error | 6,480 | 606 |
| Duplicate copies absorbed | 15,192 (591 stopped by consumers, 14,601 suppressed by the provider's idempotency key) | 0 |
| Provider side effects executed | 299,997 (299,997 distinct keys) | 31,422 (29,997 distinct keys) |
| **Duplicate side effects** | **0** | **1,425** |
| Time to finish after the load ended | 0.3 s | 0.1 s |
| Verdict | **PASS** | **PASS** (duplicates were detected, as required) |

(The load was planned as 100,000 events; the generator's rounding created 99,999, and 9,999 in the control.)

### What the data shows

1. **Nothing was lost.** Every one of the 99,999 accepted events was stored and all 299,997 deliveries reached DELIVERED with no dead letters, through 3 relay SIGKILLs and about 6,500 injected provider errors.
2. **Nothing was applied twice.** 15,192 duplicate copies were absorbed (matching the expected 5% of 99,999 events times 3 channels, about 15,000) and the provider executed exactly 299,997 effects, one per delivery. Two layers did the absorbing: consumers recognised 591 duplicates as already delivered, and the provider's idempotency key suppressed the other 14,601. With concurrent consumers (D15) most duplicate copies are in progress at the same moment as the original, so the provider's key carries most of the load. That is the intended defence in depth: the consumer check is only an optimisation.
3. **The counters can see duplicates.** With both protections off, the same duplicate injection produced 1,425 duplicate side effects (4.75% of the 29,997 deliveries, matching the injected rate), so the zero in the main run is a measurement, not an absence of measurement.
4. **Failures cost latency, not correctness:** the system finished delivering within 0.3 s of the load ending, so the crashes did not leave a lasting backlog at this rate.

### Limitations

- One run of each; the random crash times and injected errors differ between runs (the report records them).
- Provider errors are injected **before** the effect runs. The case where the effect committed but the response was lost is covered by the relay-crash and consumer-redelivery paths and by automated tests, but not by this run's error injection.
- Only relays were killed during the load. Consumer crashes are covered by automated tests, not by this run; broker and relay outages are covered separately in B5.
- Duplicates come from the relay's injection point and from crash recovery. Other duplicate sources (for example a consumer crashing after the provider call) are covered by tests (see `docs/failure-scenarios.md`), not counted separately here.
- A rate below capacity was chosen deliberately; behaviour at saturation with failures injected is not measured.
- Everything ran on one laptop under Docker Desktop.

---

## B5. Recovery from a 60-second outage of the relay or the broker

**Question:** when the relay, or NATS, is stopped for 60 seconds under steady load, how large does the backlog get, how long does the system take to recover, and is anything lost?

### Method

Same environment and configuration as B4 (4 relays, 4 consumers per channel). For each repetition the system is reset, then runs steady load at 250 events/s (68% of the measured capacity of 366/s, B3) through the HTTP API: 30 s of steady load, then a **real** `docker compose stop` of the relay (all instances) or of NATS for 60 s, then 90 s more load. A one-second sampler reads exact counts from the database. 3 repetitions each.

- **Peak backlog:** the largest number of events not yet published (outbox) and of deliveries not yet delivered after the stop.
- **Recovery time:** from the restart until the number of undelivered deliveries is back within two seconds of arrivals (1,500 deliveries, the same bound as the capacity test's bounded-backlog rule).
- **Peak delivery rate:** the best five-second average of deliveries completed per second after the restart.
- **Pass criteria:** every accepted event stored, all deliveries DELIVERED, zero dead letters, zero duplicate side effects.

### Results

| Service stopped | Rep | Outage | Peak events waiting in the outbox | Peak deliveries undelivered | Recovery time | Peak delivery rate | Accepted / delivered (deliveries) | Dead letters | Duplicate side effects |
|---|---|---|---|---|---|---|---|---|---|
| Relay | 1 | 61.0 s | 15,510 | 46,545 | 99.0 s | 1,313/s | 45,000 / 135,000 | 0 | 0 |
| Relay | 2 | 61.1 s | 15,555 | 46,671 | 99.9 s | 1,315/s | 45,000 / 135,000 | 0 | 0 |
| Relay | 3 | 61.1 s | 15,583 | 46,749 | 97.9 s | 1,299/s | 45,000 / 135,000 | 0 | 0 |
| NATS | 1 | 61.9 s | 16,017 | 48,057 | 98.1 s | 1,311/s | 45,000 / 135,000 | 0 | 0 |
| NATS | 2 | 61.0 s | 15,479 | 46,449 | 97.0 s | 1,325/s | 45,000 / 135,000 | 0 | 0 |
| NATS | 3 | 61.0 s | 15,592 | 46,783 | 101.0 s | 1,289/s | 45,000 / 135,000 | 0 | 0 |

The undelivered-delivery count before the outage was 31 to 43 (steady state).

### What the data shows

1. **No loss in any of the six outages.** All 45,000 accepted events and all 135,000 deliveries completed each time, with zero dead letters and zero duplicate side effects.
2. **The backlog is as large as the arrivals during the outage** (250 events/s for about 61 s is about 15,300 events), i.e. nothing was being processed while the service was down, and the database absorbed all of it.
3. **Recovery takes about 100 s after a 61 s outage, in every repetition (97 to 101 s)**, for both services. At the measured peak delivery rate (about 1,300 deliveries, about 435 events, per second) minus the 250 events/s still arriving, the net drain is about 185 events/s, and 15,500 events divided by 185 gives about 84 s plus a ramp-up: consistent with the measured time (this is an arithmetic cross-check, not a separate measurement).
4. **Recovery is bounded by the system's own drain rate, not by the failure.** Relay and NATS outages looked the same from outside because in both cases the events wait in the outbox (the relay down: not claimed; NATS down: claimed, release without attempting, back off; D18) and the same pipeline drains them afterwards.

### A defect this experiment exposed (decision D18)

An earlier version of the relay dead-lettered events during a broker outage: every failed publish used one of an event's five attempts. At about 100 events/s the effect was hidden (each failing publish blocks a relay for its 5 s timeout, so no single event was retried five times within a 30 to 60 s outage), but with three events in flight a 75 s NATS outage dead-lettered all three. The relay now gives the attempt back when the broker is at fault, releases the rest of the batch untried and backs off, and the client no longer buffers while disconnected. After the fix the same low-traffic outage ends with all three events published and delivered, and a test covers it (`TestBrokerOutageNeverDeadLettersAnEvent`). B5's runs above are on the fixed code.

### Limitations

- NATS was stopped and started with its data intact. Loss of the broker's storage is a different failure and is not tested.
- One arrival rate (250/s) and one outage length (60 s); a longer outage means a proportionally larger backlog and longer recovery.
- Recovery time is bounded by this machine's drain rate (about 435 events/s here, with the mock provider and its database sharing the CPU), so the absolute figures will differ elsewhere; the no-loss result does not depend on them.
- The outage tests do not combine with injected duplicates and provider errors (B4 does that without outages).

## B6. Investigation: the recurring multi-second latency stalls

**Question:** B2 and B3 saw intermittent repetitions whose p99 end-to-end latency was 3 to 4 seconds while the median stayed normal (about 15% of repetitions). What causes them?

### Method

A 10-minute steady run at 150 events/s (4 relays, 4 consumers per channel, the same configuration and conditions as B4), repeated twice, with a one-second monitor of exact database counts (as in every other benchmark) plus, alongside it:

- **Run 1:** a once-a-second sample of what Postgres was doing: running autovacuum workers, sessions waiting on locks, the wait events of active backends. `log_checkpoints` was switched on to record every checkpoint.
- **Run 2:** Postgres' own WAL timing, sampled about every 1.3 s (`pg_stat_wal` with `track_wal_io_timing` on): the number of WAL fsyncs and the total time spent in them. `scripts/diagnostics/pg-wal-sampler.sh` collects it and `scripts/diagnostics/analyze-stalls.py` compares it with the throughput dips. The setting was reset afterwards because it adds overhead.

A dip is a second in which deliveries completed at under 60% of the run's median rate. Raw data: `docs/data/2026-10-07-stall-investigation/`.

### Results

Both clean runs reproduced the tail: median end-to-end latency 32 to 34 ms, p95 107 to 631 ms, **p99 2.2 to 2.3 s**, with the API still answering (p99 51 to 89 ms). In the monitor data the cause is visible as **short dips: throughput falls to about a fifth for 3 to 4 seconds, then recovers on its own**, while arrivals continue and a backlog of up to about 1,300 deliveries builds and drains.

| | Dips (seconds into the run) | Spacing |
|---|---|---|
| Run 1 | 20, 144, 266, 398, 542 | 122 to 144 s |
| Run 2 | 163, 292, 413 | 121 to 129 s |

What was ruled out:

| Suspect | Evidence |
|---|---|
| Checkpoints | `log_checkpoints` showed two checkpoints in run 1, each spread over about 270 s with a total sync time of 0.12 to 0.14 s and a longest single sync of 0.02 s; the dips do not line up with their starts or ends |
| Autovacuum | Autovacuum workers were running in 14 of 500 samples and none during any dip (autovacuum does run about every 60 s on each table, so it is not the periodic cause) |
| Lock contention | Zero sessions waiting on a lock in all 500 samples |
| The platform's own code | During the dips the active backends' wait events were almost all `WALWrite` and `WalSync`: every commit waiting for the WAL flush, with nothing else going on |

What was found, from Postgres' own timing in run 2:

| | WAL fsync count per sample | Average fsync time |
|---|---|---|
| Whole run | about 500 per sample (400 per second) | **2.2 ms** (99th percentile of sample averages 21 ms) |
| Dip at 163 to 167 s | 101 | **7.1 ms** (worst sample average 28.7 ms) |
| Dip at 292 to 295 s | 78 | **10.2 ms** (worst 21.2 ms) |
| Dip at 413 to 417 s | 104 | **6.8 ms** (worst 23.4 ms) |

During each dip the WAL flush was **3 to 5 times slower** and the number of flushes completed per sample fell by about the same factor. Every event needs several synchronous commits (the create transaction, the relay's claim and finish, the consumer's update, the provider's own commit), so the whole pipeline is commit-bound, and commits that take 3 to 5 times longer produce exactly this throughput dip. Latency then grows for the events in flight during the dip, which is the p99.

### Conclusion, and how far it goes

**Established:** the stalls are periods of about 4 seconds, roughly every 2 minutes, in which the WAL fsync is several times slower than usual, and that explains the throughput dips and the latency tail. They are not caused by checkpoints, autovacuum or lock contention, and not by anything the platform's code does while they last (the processes are simply waiting on commits).

**Not established:** *why* the flush is slow. The slowness is below Postgres, so the likely place is the storage path of Docker Desktop's Linux VM on Windows (the virtual disk and the host's flushing), whose roughly 2-minute rhythm I could not tie to anything I measured. That is an inference from elimination, not a measurement of the disk layer. A first attempt to time raw synchronous writes inside the container in an idle system showed none under 100 ms in 4,059 writes, but its resolution (whole milliseconds, one block rewritten) was too coarse to be evidence either way, so it is not used.

**Consequences:** (1) the 3 to 4 s p99 tails in B2 and B3 are a property of this test environment's storage, not of the platform; medians are unaffected. (2) They explain why some repetitions look unstable while others (the same configuration) are clean. (3) A repeat on a native Linux machine would be the natural test (if the same pattern is absent there, the environment is confirmed as the cause). That comparison was made later on a Linux VM ([benchmarks-linux.md](benchmarks-linux.md), L7): no stalls and a WAL flush averaging 0.5 ms against 2.2 ms here. It is consistent with this explanation but remains an inference, and every figure in this document describes one Windows laptop running Docker Desktop.

### Notes on how the data was produced

A first attempt at this investigation was **discarded**: two benchmark processes were accidentally running at the same time (the command I used to launch the first one was chained in a way that started it unexpectedly), so the arrival rate was about 300 events/s (near this configuration's capacity), not 150. The two clean runs above were made after confirming that exactly one benchmark process was running. Switching on `log_checkpoints` is kept in the dev Compose file (`docker-compose.dev.yml`); the other instrumentation was temporary.

## B7. The broker loses every message it holds, under load (the reconciler, D22)

**Question:** if the broker loses the messages it is holding while events are flowing, does the system notice, repair every lost delivery, and avoid a duplicate side effect?

### What is real and what is simulated

The pipeline, the load and the counting are real. The **loss is simulated**: the stream's messages are deleted with the broker's own purge call, as if its storage had been lost. A disk failure, or a clustered broker losing a replica, is not tested. The reconciler's wait is shortened for this run (below), so the recovery time is not a prediction for the default setting.

### Method

Same environment as B4 and B5 (4 relays, 4 consumers per channel, mock provider, Docker Desktop on Windows, AC power, plan "Balanced"; `environment.json` in the data folder). For each of 3 repetitions the system is reset, then steady load of 150 events/s (roughly 40 to 50% of the measured capacity of 310 to 370 events/s) runs through the HTTP API for 150 s in total. At 30 s all consumers are **paused for 8 s**, so that unconsumed messages pile up in the stream (at this rate the consumers otherwise keep the stream almost empty and a purge would find nothing to lose, see the notes below). Then every message in the stream is purged and the consumers resume. The load keeps running until the 150 s are over. A one-second sampler reads exact counts from the database.

The relays ran with `RECONCILE_EVERY=5s` and `RECONCILE_AFTER=30s` (defaults: 30 s and 10 minutes). The wait has to be shorter than the tool's 5-minute drain limit to demonstrate a repair; it must still be longer than a healthy delivery ever takes at this load.

- **Pass criteria:** every accepted event stored, all deliveries `DELIVERED`, zero dead letters, zero duplicate side effects.
- **Repaired deliveries:** deliveries whose `reconcile_count` is above zero at the end of the run (the reconciler's own record in the database).
- **Recovery time:** from the purge until the number of undelivered deliveries is back within two seconds of arrivals (900 deliveries).

Raw data: `docs/data/2026-11-05-reconciler-purge/`.

### Results

| Rep | Peak events waiting in the outbox | Peak deliveries undelivered | Deliveries repaired by the reconciler | Most repairs for one delivery | Recovery time | Accepted events / delivered (deliveries) | Dead letters | Duplicate side effects |
|---|---|---|---|---|---|---|---|---|
| 1 | 28 | 3,314 | 3,230 | 1 | 34.9 s | 22,500 / 67,500 | 0 | 0 |
| 2 | 346 | 4,257 | 3,216 | 1 | 34.0 s | 22,500 / 67,500 | 0 | 0 |
| 3 | 239 | 3,332 | 3,243 | 1 | 34.0 s | 22,500 / 67,500 | 0 | 0 |

In all three repetitions the provider received exactly one request and recorded exactly one effect for each of the 67,500 keys (67,500 distinct request keys, 67,500 distinct effect keys), consumers ignored no duplicates and the provider suppressed none.

### What the data shows

1. **The reconciler found and repaired every lost delivery.** About 3,200 deliveries were lost in each repetition (the messages for the 8 seconds of arrivals while the consumers were paused: 8 s x 150 events/s x 3 channels is 3,600; this is an arithmetic cross-check, not a separate measurement). Each was repaired once, and all 67,500 deliveries ended `DELIVERED`.
2. **No duplicate side effects**, with 0 dead letters. The lost messages had never reached the provider, so the repair was the first and only send; the key and the unique message id (`:recon-1`) did their job without being exercised against a duplicate here. Repairing a message that was only *slow* (which would produce a duplicate that the consumer or the provider absorbs) is covered by tests, not by this run.
3. **Recovery took about 34 s.** That is consistent with the 30 s wait (counted from each lost delivery's last publish, which was up to 8 s after the pause began), the 5 s check interval and the time to republish. It is dominated by the configured wait: with the default 10 minutes it would be about ten minutes.

### Notes on how the data was produced

Three earlier attempts are not used, and the reasons matter:

- **A first run passed but proved nothing about the reconciler.** Without pausing the consumers the stream was nearly empty at the moment of the purge. All three repetitions passed with **zero** repairs (the reconciler's counter showed it had done nothing), so that result is discarded. The 8-second pause was added to the tool for this reason.
- **A second run failed, for a configuration reason.** The tool recreated the relay containers when it scaled them, and the recreated relays did not have the shortened wait, so they used the 10-minute default. After the tool's 5-minute drain limit 64,320 of 67,500 deliveries were delivered and 3,180 were still waiting, with zero repairs. That was a mistake in how I ran it, not a defect, but it also shows the purge really loses deliveries when nothing repairs them in time. It is not a clean control (the reconciler was running with a long wait), so I do not count it as one.
- **Two further starts aborted** with a database deadlock during the tool's reset (`TRUNCATE`). I had force-killed the second run, and its load generator, running in its own container, kept posting events and collided with the reset. Stopping the leftover container fixed it. This is a hazard of killing the benchmark tool, not of the platform.

### Limitations

- The loss is a purge through the broker's API with the consumers paused. It does not include messages that were delivered but not yet acknowledged, partly processed messages, or a real storage failure.
- The reconciler's wait was 30 s instead of the default 10 minutes, so the recovery time above does not describe the default.
- One arrival rate (150 events/s) and one 8-second backlog; three repetitions.
- The "Balanced" power plan and Docker Desktop on Windows, like the rest of this document. The correctness result does not depend on speed; the recovery time does.
- A consumer that is down longer than the whole repair schedule would have its deliveries dead-lettered by the reconciler (see `limitations.md`); this run does not test that.

