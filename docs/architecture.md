# Architecture

Status: describes what is built (outbox, concurrent relays, concurrent idempotent consumers, retries, dead letters, replay, fault injection, metrics, a dashboard, benchmarks and failure experiments). What is deliberately not handled is in [limitations.md](limitations.md); what happens at each crash point is in [crash-analysis.md](crash-analysis.md). References like "D4" or "D7" in code comments and below are numbered design decisions; [design-decisions.md](design-decisions.md) explains each.

## Goal

A business operation commits to the database, and notifications for it must eventually reach downstream channels even if the application, relay, broker or consumers crash. The system targets **at-least-once delivery** (never exactly-once) and makes duplicates harmless with **idempotent side effects**.

## Components

```text
                       client
                         |  POST /api/events/<type>   (Idempotency-Key required)
                         v
                  +--------------+
                  |     API      |  serves the dashboard too
                  +------+-------+
                         |  ONE transaction
                         v
        +------------------------------------------+
        | PostgreSQL (platform)                    |
        |   business row  (payments, bookings...)  |
        |   outbox_events         (event state)    |
        |   channel_deliveries    (per channel)    |
        |   api_idempotency       (request replay) |
        +--------------------+---------------------+
                             |  claim (SKIP LOCKED + lease)
                             v
                  +----------------------+
                  |  Relay workers (N)   |  publish one thin message per channel
                  +----------+-----------+
                             |  notify.email / notify.push / notify.webhook
                             v
                  +----------------------+
                  |  NATS JetStream      |  stream NOTIFY, WorkQueue retention
                  +----+-------+-----+---+
                       |       |     |       durable pull consumer per channel
                       v       v     v
                  email     push   webhook    consumers (N each)
                       \       |     /
                        v      v    v
                     +----------------------+
                     |  Mock provider       |  Idempotency-Key + effect in one tx
                     |  PostgreSQL (own DB) |  requests / effects / keys
                     +----------------------+
```

| Component | Binary | Responsibility |
|---|---|---|
| API | `cmd/api` | Validates requests; writes business row, outbox row, delivery rows and the idempotency record atomically; serves read endpoints and the dashboard |
| Relay | `cmd/relay` | Claims due outbox rows, publishes one message per pending channel, finishes the row with a fenced update |
| Consumer | `cmd/consumer` (`CHANNEL=email\|push\|webhook`) | Pulls messages for one channel and processes up to `CONSUMER_CONCURRENCY` (default 8) at once: loads the delivery and payload, calls the provider with an idempotency key, marks the delivery (D15) |
| Provider | `cmd/provider` | Stands in for the external email/push/webhook systems; deduplicates by `Idempotency-Key`; counts requests and effects separately; can optionally send the email channel for real to one fixed recipient (D21) |
| Migrate | `cmd/migrate` | Applies embedded SQL migrations to the platform or provider database |
| Loadgen, Bench | `cmd/loadgen`, `cmd/bench` | Open-loop load generation and the benchmark, reliability and recovery experiments (see [benchmarks.md](benchmarks.md)) |

## Data flow for one event

1. **Create.** One transaction inserts the idempotency record, the business row, the `outbox_events` row (`PENDING`) and one `channel_deliveries` row per routed channel (`PENDING`). Either all commit or none do. Routing is fixed at this moment (D3, D6).
2. **Claim.** A relay runs one short statement that moves due rows to `PROCESSING`, sets `locked_by`, `locked_until`, increments `attempts`, and sets `next_attempt_at` to the lease expiry. `FOR UPDATE SKIP LOCKED` lets concurrent relays take disjoint rows without blocking (D4).
3. **Publish.** For each delivery still `PENDING`, the relay publishes a thin message (`event_id`, `channel`, `event_type`) with NATS message ID `<event_id>:<channel>` and waits for the broker acknowledgements (D6, D11).
4. **Finish.** A fenced update sets the row `PUBLISHED`; it only succeeds if `(locked_by, attempts)` still match the claim (D4).
5. **Consume.** The channel's consumer fetches a message, reads the delivery state and payload in one query, skips if already `DELIVERED`, otherwise calls the provider with `Idempotency-Key: <event_id>:<channel>` (D5).
6. **Effect.** The provider inserts the key and the effect in one transaction under a unique constraint; a repeated key returns the stored result without a second effect.
7. **Complete.** The consumer marks the delivery `DELIVERED` and acks the message.

## State models

Outbox event (relay to broker):

```text
PENDING --claim--> PROCESSING --publish ok--> PUBLISHED
                       |  ^
          publish fail |  | lease expired / backoff elapsed (re-claim)
                       v  |
                    RETRYING --max attempts--> DLQ
```

Channel delivery (broker to side effect):

```text
PENDING --> DELIVERED
   |  \
   |   +--> RETRYING --> DELIVERED
   |            |
   +------------+--> DLQ        (retries exhausted, permanent error, or poison guard; replay returns it to PENDING)
```

## Reliability guarantees and how they are achieved

| Guarantee | Mechanism |
|---|---|
| A committed business change is never lost to the notification system | Business row and outbox row are written in the same transaction (D3) |
| A client retry does not create a second event | `Idempotency-Key` stored in the same transaction; replay returns the stored response (D12) |
| Multiple relays never publish a row concurrently | `FOR UPDATE SKIP LOCKED` inside the claim statement (D4) |
| A dead relay does not strand work | Lease expiry: `next_attempt_at` is the lease end, so another relay claims the row (D4) |
| A slow relay cannot corrupt a row it lost | Fenced finish on `(locked_by, attempts)` (D4) |
| Duplicate deliveries are possible | Accepted by design: relay crash after publish, lease overlap, broker redelivery (D4, D11) |
| Duplicate side effects do not occur | Receiver-side idempotency key with the effect in one transaction (D5) |
| Messages are not lost when the broker is full | `discard new` makes the relay keep the event in the outbox and retry (D11) |
| A broker outage never dead-letters an event | A failure while talking to the broker gives the attempt back and the event is retried; the rest of the batch is released and the relay backs off (D18) |
| A slow message does not hold up others | Consumers process messages concurrently with a bounded worker pool (D15) |
| Writes need a credential, reads do not | One admin token guards every write, including event creation when `CREATE_REQUIRES_TOKEN=true` (D8, D19) |

What is **not** guaranteed: ordering across events or per aggregate; exactly-once delivery; broker durability across a host crash with the default fsync behaviour (the outbox is the safety net).
## Retry, dead letters and replay (D7)

```text
consumer receives message (attempt = broker delivery count)
   |
   +-- delivery already DELIVERED ........ ack, count a duplicate (D5)
   +-- delivery already DLQ .............. terminate (stale message)
   +-- attempt > MAX_ATTEMPTS ............ DLQ without calling the provider (poison guard)
   +-- call provider with Idempotency-Key
         |
         +-- 2xx .......................... DELIVERED, ack
         +-- 4xx (not 408/429) ............ DLQ "permanent error", terminate
         +-- attempt >= MAX_ATTEMPTS ...... DLQ "retries exhausted", terminate
         +-- otherwise (5xx, 408, 429, timeout, connection error)
               delay = equal-jitter backoff (or Retry-After if longer, capped)
               row -> RETRYING with last_error and next_attempt_at, NakWithDelay(delay)
```

NATS owns the retry timing; the delivery row only mirrors the state for the dashboard. The dead-letter state is a status on `channel_deliveries` (not a separate stream). Replay is one transaction: dead deliveries return to `PENDING` with attempts reset and `replay_count + 1`, and their events return to the relay, which publishes only `PENDING` deliveries with a replay-specific message ID. It is protected by the admin token.

Provider failure injection (status, delay, count per channel) is controlled through the provider's own admin endpoints.

## Reconciler (D22)

The outbox makes the step from the database to the broker reliable. Nothing in the flow above covers a broker that accepts a message and then loses it (its storage is lost, or a consumer exhausts the broker's redelivery limit): the event says `PUBLISHED` and its delivery stays `PENDING` or `RETRYING` forever. The reconciler, a loop inside every relay, closes that gap:

```text
every RECONCILE_EVERY (30 s):
  find open deliveries (PENDING / RETRYING) whose event is PUBLISHED and that have shown no sign of life
  for RECONCILE_AFTER (10 min) x 2^repairs so far          -- sign of life = last publish or planned retry time
  lock their events (SKIP LOCKED), then in one transaction:
    repairs so far >= RECONCILE_MAX_REPAIRS (3)  -> delivery DLQ, reason names the loss (replayable)
    otherwise -> delivery PENDING, attempts 0, reconcile_count + 1; event PENDING, due now
  the relay then claims the event as usual and publishes the PENDING deliveries with message ID <event>:<channel>:recon-N
```

It publishes nothing itself, so every republish keeps the lease and the fence. A repair of a delivery that was merely slow produces a second copy, which the consumer's check and the provider's idempotency key absorb. Repairs are exported as `notif_reconciler_repaired_total` (zero in normal operation); `RECONCILE_EVERY=0` turns the loop off. See [limitations.md](limitations.md) for what is and is not tested.

## Health and pause (D9, D8)

Workers are ephemeral, so health is defined per role rather than per instance.

- Every relay and consumer instance upserts a heartbeat row (`workers`: name, role, state, last seen) every few seconds. Age is computed with the database clock, so it does not depend on clocks agreeing.
- A role is **UP** when at least one instance has a heartbeat within the stale threshold, **DOWN** when none has, and **PAUSED** when every live instance reports it was paused by an operator. Every known role always appears, so a role with no instances shows as DOWN.
- Pausing a role (fault points `relay.pause`, `consumer.<channel>.pause`) makes it stop taking new work. Nothing is lost: work waits in the outbox (relay) or in the broker (consumers) until the role is resumed. A worker reports PAUSED only after its own loop has stopped, not merely when the pause was requested.
- `GET /api/health` combines role status, the outbox and per-channel delivery backlog from the database (counted through the partial indexes), and the broker's own backlog per channel (messages waiting, in flight, redelivered). Each part degrades independently if its source is unreachable.

## Observability (D9)

**Structured logs.** Every process writes one JSON line per action to stdout with `event_id`, `event_type`, `consumer` or `worker`, `attempt` and `status` where they apply. The event id is the correlation id: `grep` it across the API, relay and consumer logs to follow one event.

**Metrics.** Every process exposes Prometheus metrics at `/metrics` (API and provider on their HTTP ports; relay and consumers on port 9100). Prometheus discovers every replica through Compose DNS. Where each kind of metric is recorded:

| Kind | Recorded by | Examples |
|---|---|---|
| Counters and histograms of work done | the process doing the work (Prometheus sums replicas) | events created, relay published, claimed and fenced-out, consumer outcomes (delivered, retried, dead_lettered, duplicate_ignored), provider requests (executed, replayed, injected_failure) |
| Durations | same | create transaction, relay claim / publish / finish, provider call, delivery latency (created to delivered) |
| **Backlog and health gauges** | **the API, computed at scrape time** from Postgres and the broker | outbox waiting, deliveries waiting and dead per channel, broker pending and in flight per channel, role up / paused / live instances |
| Runtime | every process | CPU, memory, goroutines (to check that a benchmark measures the infrastructure, not the Go processes) |

Design rules: labels are low-cardinality only (`channel`, `event_type`, `outcome`, `result`, `reason`, `role`; never an event id, enforced by a test); gauges are computed in one place so several relays never double-count; if the API cannot read Postgres it emits **no** backlog gauges plus an error counter, because zeros would look like "all clear" exactly when we are blind; live histograms are bucketed and approximate, and benchmark percentiles come from exact database timestamps instead.

**Delivery latency** is computed by the database in the same `UPDATE` that marks a delivery `DELIVERED` (`delivered_at - created_at`, database clock), and only on the first transition, so a repeated delivery cannot double-count.

**Grafana** (optional profile) is provisioned with a dashboard covering throughput, backlog, dead letters, role health, consumer outcomes, duplicates handled, relay stage latencies, delivery latency, provider traffic, and CPU and memory per process.

## Not built

Deliberate gaps (single points of failure, unbounded table growth, no ordering, one shared token, and others) are listed with their production remedies in [limitations.md](limitations.md). The production Compose setup in [deployment.md](deployment.md) was verified locally and has not been deployed to a public server.
