# Benchmarks

Every number here was measured. Raw data for each experiment is kept in [`docs/data/`](data) (the scripts write to `benchmark-results/`, which is not committed). Nothing is estimated. Each section states its environment, dataset, method and limitations.

## B1. Relay scaling (baseline, unoptimized)

**Question:** how fast do N relay workers publish a backlog of outbox events to the broker?


> **Note (added later):** B1 predates the environment capture added to the benchmark tooling, so the machine's power state during B1 is unknown, and it predates the relay's metrics instrumentation. It will be re-measured with the current tooling; treat its figures as provisional until then.

### Environment

| | |
|---|---|
| Machine | Laptop, Intel Core i5-12500H (12 cores, 16 logical), 15.6 GB RAM |
| OS | Windows 11 Home (10.0.26300) |
| Container runtime | Docker Desktop 29.8.2 (Compose 5.5.1), Linux VM given 16 CPUs and about 8 GB RAM |
| Software | Go 1.27.0; PostgreSQL 17 (alpine, default settings); NATS 2 (alpine) with JetStream, file storage |
| Relay settings | batch 10 events per claim, lease 30 s, idle poll 200 ms, publish timeout 5 s (all defaults) |
| Load tool | `cmd/loadgen` on the Windows host, talking to the published ports |

This is Docker Desktop on Windows (WSL2), not a native Linux server. The numbers describe this environment only.

### Dataset and method

- 20,000 `PaymentSucceeded` events, each routed to 3 channels, so **60,000 broker messages per run**. Events are inserted directly into `outbox_events` and `channel_deliveries`, bypassing the HTTP API, so the relay stage is measured on its own.
- Consumers were stopped, so nothing competes for CPU and the stream simply accumulates messages.
- Per run: reset all data and purge the stream; start R fresh relay containers and wait until all are ready; seed the events with a due time 6 s in the future (so all relays are idle and warm and start together); wait until every event is `PUBLISHED`.
- **Throughput = published events divided by the window from the first to the last `published_at`**, both read from the database. No clock from the load tool is involved.
- 3 runs for each of 1, 2, 4 and 8 relays. Script: `scripts/relay-scaling.sh` (`N=20000 RUNS=3 COUNTS="1 2 4 8"`).

### Results

Events published per second (each event is 3 broker messages):

| Relays | Run 1 | Run 2 | Run 3 | Median | Speed-up vs 1 relay | Per-relay median |
|---|---|---|---|---|---|---|
| 1 | 261 | 282 | 272 | **272** | 1.0x | 272 |
| 2 | 651 | 672 | 670 | **670** | 2.5x | 335 |
| 4 | 1,207 | 1,225 | 1,248 | **1,225** | 4.5x | 306 |
| 8 | 2,108 | 1,743 | 2,092 | **2,092** | 7.7x | 262 |

At the median, 8 relays published about 2,092 events (about 6,276 messages) per second. In every run all 20,000 events were published, none went to the dead-letter state, and every event needed exactly one attempt (no lease expiries or retries happened).

CPU snapshot taken about one second after the backlog became due (CPU% is relative to one core, so 100 means one full core). It is a single instantaneous sample per run, so it is only indicative:

| Relays | Relay processes (total) | Postgres | NATS |
|---|---|---|---|
| 1 | 24 to 28% | 27 to 34% | 16 to 25% |
| 2 | 39 to 41% | 33 to 47% | 24 to 26% |
| 4 | 70 to 81% | 80 to 85% | 37 to 45% |
| 8 | 162 to 181% | 134 to 174% | 67 to 70% |

### What the data shows

- **Throughput scaled well but not perfectly evenly.** Doubling relays roughly doubled throughput up to 4 relays, and 8 relays gave 7.7x. The 8-relay runs varied more (1,743 to 2,108).
- **No component was CPU-saturated.** With 1 relay, the relay process used about a quarter of one core. It is waiting on round trips (database and broker), not computing. Postgres was the most heavily used component at 8 relays (about 1.3 to 1.7 cores out of 16 available), but not saturated.
- **Per-relay throughput is not constant** (272, 335, 306 and 262 events/s per relay). One hypothesis for the higher per-relay rate at 2 and 4 relays is that Postgres groups concurrent commits into fewer disk flushes, which helps more as concurrency rises. **This has not been verified**; it is a hypothesis to test in the optimization slice, not a finding.

### Limitations

- One machine; the relay, Postgres, NATS and the Docker VM share the same CPU. Results will differ on a native Linux server.
- The relay stage only. End-to-end throughput and latency (committed to delivered) are measured separately in a later benchmark with consumers running.
- Events were seeded in bulk, not created through the API.
- The CPU figures are single snapshots, not time series.
- These figures were measured **before** the Prometheus instrumentation was added to the relay (Slice 6). The instrumented relay adds a small amount of work per event, so the baseline will be measured again with metrics enabled before any optimization is compared against it.
- This is a baseline of the simple implementation: one claim per 10 events, one `UPDATE` per event to finish it. No optimization has been applied. The optimization exercise will be driven by measurements and compared against these numbers.

### Notes on how the data was produced

- A first design of this experiment started the relays and seeded events at the same moment, which produced unreliable results (relay start-up skew dominated short runs: 82/s for 1 relay in one run against about 290/s in repeats). That design was discarded. The delayed-due-time method above removed the skew.
- The laptop went to sleep during the full experiment after run 1 of the 8-relay configuration. Runs 2 and 3 of that configuration were re-run immediately afterwards under the same conditions with the same script (`APPEND=1 RUN_START=2 COUNTS=8`). The 8-relay run 2 (1,743/s) is the lowest of that group and is kept in the table.

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
4. **Relay scaling:** with 8 consumers per channel, 1, 2, 4 and 8 relays sustain 156, 293, 313 and 313 events/s. One relay limits the system (it shares the database with everything else, so it manages about 150 events/s here, well below the 270/s it reached alone in B1); from 2 relays on, adding relays changes little.
5. **The ceiling is about 313 events/s (about 940 deliveries/s) and it is a limit of the shared resources, not of one component.** At the ceiling the whole VM is about 60% busy (941 to 989 of 1,600), the platform's Postgres is the hottest component (222 to 238), the mock provider plus its own Postgres take about 290 (roughly 30% of everything), the consumers about 205, and the relay only 45 to 52. The ceiling is the same with 4 or 8 relays and with 4 or 8 consumers. On this hybrid CPU, hyperthread siblings and efficiency cores share compute, so the machine saturates well below 100% of its logical capacity.
6. **Latency at 70% load is low:** end-to-end p50 between 31 and 165 ms and p99 between 65 and 302 ms in the typical (median) repetition. It is best at 4 consumers per channel (p50 31 ms, p99 65 ms at 164 events/s). With one consumer, about 100 ms of the roughly 160 ms is the time from creation to publication by the relay (p50 98 ms), which is consistent with the relay's 200 ms idle polling interval (an event waits half a poll interval on average at low load). That explanation is a hypothesis to be tested in the optimization exercise, not yet verified.
7. **Occasional multi-second stalls remain.** In 4 of 27 latency repetitions the p99 exceeded 2 s (up to 3.6 s), although the medians are tight. The same kind of intermittent stall was seen once in an earlier, battery-powered run. Its cause is **not yet identified**; autovacuum and CPU saturation were checked in that earlier investigation and did not explain it.

### Limitations

- One machine, one session, one run of the matrix (with repetitions inside each stage). Results describe this laptop in this configuration and will differ on a native Linux server.
- The mock provider and its database consume about 30% of the CPU at the ceiling. A real external provider would not run on this machine, so the platform's own capacity here is understated. The numbers include that cost.
- A hybrid, hyperthreaded CPU on a laptop: its effective capacity at high load is not the sum of its logical CPUs.
- Only PaymentSucceeded events (3 channels). A mixed workload would change the fan-out.
- `created_at` is the start of the creating transaction, slightly earlier than the commit; for a transaction of about 5 ms this bias is small.
- Prometheus (scraping every 5 s) and the Go processes share the CPU with the system under test.
- The unexplained stalls above add run-to-run variation, shown in the ranges.

### How this data was produced (a correction)

A first attempt at this matrix was **discarded**. Its results were inconsistent (a configuration that passed one hour failed the next, and one step stalled for about 20 s without reproducing), and the cause turned out to be the machine: the laptop was **running on battery** with a power-limited CPU. The benchmark tool now records the machine's power source, power plan and clock with the results and **refuses to run on battery**. The figures in this section were all measured on mains power after that fix. The earlier relay-only measurement (B1) predates the environment capture, so its power state is unknown; it will be re-measured with the current tooling.

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
6. **Intermittent stalls remain.** In 2 of 12 latency repetitions the p99 was 3.5 to 3.7 s while the median was normal (before: 4 of 27). The cause is still unidentified and this change did not address it.

### Limitations

- One run per configuration with the repeated-measurement protocol inside it; the confirmed values are one rate step wide (25%).
- Only four of the nine B2 configurations were re-run (the relay-scaling rows with 8 consumers per channel and the 2 and 8 relay rows were not).
- The before and after sweeps started their ladders at different rates, which affects where in a 25% band the confirmed value lands.
- The mock provider and its database still take a large share of the CPU, so this machine's platform capacity is understated.
- Not tested: the latency effect of a slow provider, where concurrency should matter most because slow calls no longer queue behind each other.
