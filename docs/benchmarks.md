# Benchmarks

Every number here was measured. Raw data for each experiment is kept in [`docs/data/`](data) (the scripts write to `benchmark-results/`, which is not committed). Nothing is estimated. Each section states its environment, dataset, method and limitations.

## B1. Relay scaling (baseline, unoptimized)

**Question:** how fast do N relay workers publish a backlog of outbox events to the broker?

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
