# Design decisions

Code comments and documents refer to numbered decisions such as "(D4)" or "(D18)". This page is the index: each entry gives **what was decided**, **why**, and **the trade-off that was accepted**. Where a later decision changed an earlier one, the entry says so. The reasoning behind each is summarised here, not reproduced in full.

| # | Decision | Where it shows up |
|---|---|---|
| D0 | PostgreSQL as the database | `migrations/`, `internal/outbox` |
| D1 | NATS JetStream as the broker | `internal/broker` |
| D2 | Go; one module, many binaries (D2b) | `cmd/`, `internal/` |
| D3 | Two-table outbox schema, written atomically | `internal/outbox/store.go` |
| D4 | Relay claiming by a short lease with fencing | `Claim`, `MarkPublished` |
| D5 | Idempotency enforced by the receiver | `internal/provider` |
| D6 | Routing fixed at creation; thin messages | `internal/events`, relay, consumer |
| D7 | Retries, backoff, dead letters, replay | `internal/backoff`, consumer, `replay.go` |
| D8 | Failure injection | `internal/faults`, `internal/admin` |
| D9 | Metrics, logs and health | `internal/metrics`, `internal/health` |
| D10 | Deployment on one VM (partly superseded by D20) | `docker-compose*.yml` |
| D11 | Stream and consumer configuration | `internal/broker/broker.go` |
| D12 | API-level idempotency | `CreateEvent`, `api_idempotency` |
| D13 | Benchmark methodology | `internal/bench`, `cmd/bench` |
| D14 | Keep the relay's 200 ms poll | `internal/relay` |
| D15 | Concurrent message processing in consumers | `internal/consumer` |
| D16 | Dashboard context while experiments run | `experiment_markers`, Experiment page |
| D17 | Reliability and recovery experiments | `cmd/bench`, `scripts/bench-reliability.sh` |
| D18 | A broker outage never dead-letters an event | `internal/relay` |
| D19 | One credential, the admin token, for every write | `internal/admin`, `internal/httpapi` |
| D20 | No public hosting and no cloud benchmark | `docs/deployment.md` |
| D21 | Optional real email sending | `internal/provider/email.go`, `migrations/provider/0004_real_email.sql` |

## D0. PostgreSQL

**Decided:** PostgreSQL for all state. **Why:** the outbox pattern rests on one ACID transaction across several rows; `FOR UPDATE SKIP LOCKED` lets concurrent workers claim disjoint rows without blocking; partial indexes keep the relay's hot query small however large the table grows; JSONB, native UUIDs and strong tooling (`EXPLAIN ANALYZE`, `pg_stat_*`) help the benchmarks. **Accepted:** polling creates steady query load, and status updates create dead tuples and vacuum work.

## D1. NATS JetStream

**Decided:** NATS JetStream with one durable consumer per channel; Postgres stays the source of truth. **Why:** explicit acknowledgements and `AckWait` redelivery reproduce the "consumer crashes before ack" case exactly; publish acknowledgements give a precise "published but the database not yet updated" window; it is a single small binary. Kafka was considered and judged overkill for a personal project. **Accepted:** less familiar than Kafka; JetStream durability depends on its storage configuration, which is acceptable because the outbox is the safety net. Its message-id de-duplication is time-bounded and is only an optimisation.

## D2. Go, and D2b. One module, many binaries

**Decided:** Go, with one `go.mod`, one binary per role under `cmd/`, shared code under `internal/`, and one Dockerfile parameterised by binary name. **Why:** a light runtime, goroutines for concurrency, quick start-up, and the benchmarks measure the infrastructure, not the language. `internal/events` is the only package everyone imports, and a test fails the build if the consumer and relay packages import each other. **Accepted:** all binaries share dependency versions; coupling is controlled by convention and a test, not by module boundaries.

## D3. Outbox schema

**Decided:** two tables: `outbox_events` (one row per event: relay to broker) and `channel_deliveries` (one row per event and channel: broker to side effect), created with the business row in one transaction, with UUIDv7 ids generated in the application. A delivery's key `(event_id, channel)` doubles as the idempotency key (D5). **Why:** separating "was the event handed to the broker" from "was each channel delivered" keeps each state machine simple and makes per-channel failure visible. Partial indexes cover only rows that still need work. **Accepted:** more writes per event, routing frozen at write time, unbounded table growth (see [limitations.md](limitations.md)).

## D4. Relay claiming

**Decided:** a relay claims a batch with one statement (`UPDATE ... WHERE id IN (SELECT ... FOR UPDATE SKIP LOCKED LIMIT n) RETURNING ...`) that sets the row `PROCESSING`, records `locked_by`, increments `attempts` and sets `next_attempt_at` to the lease end. The lease is data, not a held lock; publishing happens **outside** any transaction; the finishing update is **fenced** on `(locked_by, attempts)`. **Why:** no lock is held across network calls, a dead relay's work becomes claimable again by itself, and a slow relay that lost its claim cannot overwrite the new owner's state. **Accepted:** duplicate publishes after a crash or a lease overlap (by design: delivery is at-least-once), lease tuning, no lease heartbeats, no ordering.

## D5. Idempotency enforced by the receiver

**Decided:** the provider receives an idempotency key with every request and writes the key and the effect in **one transaction** under a unique constraint; a repeated key returns the stored result without a second effect. Requests and effects are counted in separate tables so duplicates are measurable, and a negative control switches the protections off to prove the counters can see duplicates. The consumer's "already delivered" shortcut is only an optimisation. **Why:** it is the only mechanism that closes "the effect ran but the record of it was lost". **Accepted:** it depends on the receiver honouring keys (see limitations).

## D6. Routing and messages

**Decided:** which channels an event goes to is fixed when the event is created (frozen into delivery rows). The broker message is **thin**: `{event_id, channel, event_type}`; consumers read the payload from Postgres. The message id is `<event>:<channel>`. **Why:** one source of truth for the payload, small messages, and routing that cannot change under a running event. **Accepted:** every delivery needs a database read.

## D7. Retries, dead letters and replay

**Decided:** capped exponential backoff with equal jitter; permanent errors (4xx except 408 and 429) go straight to the dead letters; `Retry-After` is honoured within the cap; the broker owns retry timing (delayed negative acknowledgement); a poison guard dead-letters a delivery whose attempts exceed the limit; the dead-letter state is a **status** on the delivery row, not a separate stream; replay is one transaction that returns dead deliveries to `PENDING` and hands their events back to the relay. **Why:** simple to inspect and query, and replay cannot lose work. **Accepted:** no automatic dead-letter retention or alerting.

## D8. Failure injection

**Decided:** code hooks at named points, configured through a `fault_config` table that every process caches and re-reads about once a second; a crash is a real `os.Exit(137)`; provider failures (status, delay, count, probability) live in the provider's memory; everything is off unless `FAULTS_ENABLED=true` and needs the shared admin token. **Why:** hooks reproduce the dangerous windows on demand and a real exit is a real crash. **Accepted:** the hooks are production code paths guarded by a switch.

## D9. Metrics, logs and health

**Decided:** structured JSON logs with the event id as correlation id; Prometheus metrics in every process with low-cardinality labels only; backlog and health gauges computed by the API at scrape time so replicas never double-count; heartbeats in the database and health defined per role (UP, DOWN, PAUSED) because workers are ephemeral; Grafana optional. Benchmark percentiles come from exact database timestamps, never from the approximate live histograms. **Accepted:** the live histograms are bucketed and approximate.

## D10. Deployment (partly superseded by D20)

**Decided:** one VM running the same Compose stack, with Caddy in front for automatic HTTPS, secrets in an uncommitted `.env`, faults off by default. **Status:** the production overlay, Caddy configuration, deploy script and smoke test were built and verified locally, but the project was **not** deployed to a public server (D20).

## D11. JetStream configuration

**Decided:** one stream `NOTIFY` (subjects `notify.*`) with work-queue retention, file storage and `DiscardNew` when full (a full broker rejects publishes, so the relay keeps the event in the outbox: backpressure on Postgres, not data loss); a 2-minute de-duplication window; one durable pull consumer per channel with explicit acknowledgements, `AckWait` 30 s and `MaxDeliver` equal to the maximum attempts plus 3; the attempt number is the broker's delivery count. **Accepted:** the de-duplication window is an optimisation only.

## D12. API-level idempotency

**Decided:** every event-creating request needs an `Idempotency-Key`; the key, a hash of the request and the stored response are written in the same transaction as the business row. The same key with the same request replays the stored response; the same key with a different request is rejected (422). **Why:** a client that retries after "committed but the response was lost" must not create a second payment. **Accepted:** keys are kept forever (see limitations).

## D13. Benchmark methodology

**Decided:** open-loop load through the HTTP API (a fixed arrival rate regardless of the answers, so queueing is visible), latency measured from each request's intended send time, exact figures from database timestamps, a geometric capacity ramp followed by repeated confirmation with a majority vote, latency runs at 70% of the confirmed capacity, and the machine's environment recorded (the tool refuses to run on battery power). **Why:** a closed loop hides queueing, and an unrecorded environment made the first results meaningless. See [benchmarks.md](benchmarks.md).

## D14. Relay polling

**Decided:** keep a fixed 200 ms idle poll. **Considered:** a shorter interval, adaptive polling, and `LISTEN/NOTIFY`. **Why:** a roughly 100 ms average added wait at idle is invisible for notification delivery, and `LISTEN/NOTIFY` would add a dedicated connection per relay, reconnect logic and a writer-side commit cost for a latency nobody perceives. The alternatives are listed in the order they would be tried.

## D15. Concurrent message processing in consumers

**Decided:** each consumer instance processes up to 8 messages at once with a bounded worker pool (a slot is claimed before each fetch, and the fetch asks for no more messages than there are free slots). **Considered and rejected:** batching the consumer's database writes, because one slow message would delay the status writes of the others (head-of-line blocking). **Result:** one consumer per channel went from 98 to 153 events/s; the machine-wide ceiling did not move (B3).

## D16. Dashboard context while experiments run

**Decided:** every worker's heartbeat carries its settings; the benchmark tool writes run and phase markers straight to the database; an Experiment page, listed only while a run is active, shows what is being varied, the phase and live counters that should converge. **Why:** runs looked the same on screen whatever was varied.

## D17. Reliability and recovery experiments

**Decided:** a 100,000-event run through the API with about 5% duplicate publishes, three real SIGKILLs of relays at random times and about 2% provider errors, plus a negative control with the protections off; and 60-second outages of the relay and of NATS under load, three repetitions each. **Pass criteria:** every accepted event stored and delivered, no dead letters, no duplicate side effects. Results in [benchmarks.md](benchmarks.md) (B4, B5).

## D18. A broker outage never dead-letters an event

**Decided:** a failure while talking to the broker gives the attempt back and the event is retried; the rest of the batch is released untried and the relay backs off a second; the relay's client does not buffer while disconnected and every binary reconnects forever. **Why:** found by the recovery experiment: a 75 s outage at low traffic dead-lettered healthy events because each failed publish used one of their attempts. **Accepted:** an event that could never be published for a reason classed as the broker's would retry forever instead of reaching the dead letters.

## D19. One credential for every write

**Decided:** reading is open; every write (creating events, replay, fault injection) needs the same admin token. `CREATE_REQUIRES_TOKEN` makes event creation need it too, and the production overlay turns it on. **Considered:** a separate proxy-level password, rejected as two mechanisms where one suffices. **Accepted:** a shared token is not an identity system, and payloads are readable by anyone who can reach the dashboard.

## D20. No public hosting and no cloud benchmark

**Decided:** the project is not hosted publicly and no benchmark was run on a cloud VM. **Why:** for a personal project the live link adds little and the cost, upkeep and public attack surface are real. **Consequences stated plainly:** the production setup is verified locally only, there are no Linux numbers, and the explanation of the latency stalls (B6) remains an inference. See [deployment.md](deployment.md) and [limitations.md](limitations.md).

## D21. Optional real email sending

**Decided:** the provider can send the email channel for real, to **one fixed recipient**, through an SMTP account configured in the environment. It is off unless the SMTP settings are present **and** an operator switches it on in the dashboard (Duplicates, Email delivery); the switch always starts off after a restart; a hard cap (20 per hour by default) limits the sends; the benchmark tools refuse to run while it is on. Push and webhook stay mock. **Why:** the platform's claims rest on a mock that is atomic with its idempotency key, so a reader can ask whether a repeated message really produces one email; with a real inbox, a duplicate delivery visibly produces one email.

**The consequence that matters:** a real send cannot share a database transaction with the idempotency key, so the real path uses a state machine instead of one transaction: claim the key as `IN_PROGRESS` (a unique insert), send, then in one transaction record the effect and mark the key `DONE`. A crash between the send and `DONE` leaves the key `IN_PROGRESS` with the email already sent; after a stuck timeout (60 s) the next retry sends again. So the guarantee for **real** email is *at least once, with a rare duplicate in that window*; the mock stays at zero measured duplicate side effects. A repeat that arrives while a key is `IN_PROGRESS` gets 503 (retried), never a second send; a send that definitely failed releases the key; one whose outcome is unknown keeps it. Each send attempt gets its own Message-ID, because some mailbox providers hide a repeated Message-ID and would hide a real duplicate. A real send takes seconds (about 5 with Gmail), so the work after the claim is deliberately not tied to the request: if the consumer's call timeout fires, the send and its record still finish, and the consumer's retry is answered from the stored result. The consumers' `CALL_TIMEOUT` should be raised (30 s) when real sending is used; with the default 5 s a consumer times out and retries, and with the protections off (the negative control) every retry sends another real email. **Accepted:** "accepted by the mail server" is all that can be known (not inbox, spam or bounce); the recipient is chosen by whoever holds the admin token, so it must not be enabled on a public deployment; the cap lives in memory and resets on restart.

## D22. Reconciler for deliveries the broker lost

**Decided:** a periodic job in every relay (default every 30 s) finds deliveries still `PENDING` or `RETRYING` although their event is `PUBLISHED` and nothing has happened to them for the configured wait (default 10 minutes; "something happened" means the event's last publish or the consumer's planned retry time, whichever is later). It returns those deliveries to `PENDING` with their attempts reset and hands their events back to the relay, which republishes them through the normal lease-and-fence path. The republished message gets its own broker message ID (`...:recon-N`) so the dedup window cannot drop it. A per-delivery counter (`reconcile_count`) doubles the wait after each repair and stops at 3 repairs, after which the delivery is dead-lettered with a reason that names the loss; an operator replay resets the counter. Events are locked with `SKIP LOCKED`, so several relays can run it at once without repairing a delivery twice. **Why:** the outbox covers the database-to-broker gap and nothing covered the broker-to-consumer gap; reusing the relay path means no second publishing mechanism, and idempotent consumers and providers make a needless repair harmless. **Considered:** a separate reconciler process (one more thing to deploy for a query), republishing directly from the reconciler (it would bypass the lease and fence), and asking the broker about each message (it cannot tell "lost" from "not yet consumed"). **Accepted:** a consumer that is down longer than the whole repair schedule (about 2.5 hours with the defaults) has its deliveries dead-lettered although nothing was lost; they stay replayable. Tested by purging the stream (a simulated loss), not by failing a disk.
