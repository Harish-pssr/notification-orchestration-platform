# Architecture

Status: describes what is built so far (outbox, concurrent relays, idempotent consumers, retries, dead letters, replay, fault injection). Sections marked *planned* describe work that is designed but not built yet. References like "D4" or "D7" in code comments and below are numbered design decisions kept in the maintainer's private notes.

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
| Consumer | `cmd/consumer` (`CHANNEL=email\|push\|webhook`) | Pulls messages for one channel, loads the delivery and payload, calls the provider with an idempotency key, marks the delivery |
| Provider | `cmd/provider` | Stands in for the external email/push/webhook systems; deduplicates by `Idempotency-Key`; counts requests and effects separately |
| Migrate | `cmd/migrate` | Applies embedded SQL migrations to the platform or provider database |
| Loadgen | `cmd/loadgen` | Resets, seeds and reports benchmark runs |

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
   +------------+--> DLQ        (retry and DLQ behaviour arrive in Slice 4)
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

## Planned (decided, not yet built)

- Worker heartbeats, health panel, pause hooks (D8, D9, Slice 5)
- Prometheus metrics and structured-log fields throughout (D9, Slice 6)
- Benchmarks and the optimization exercise (Slice 7), deployment on one VM (D10, Slice 8)
