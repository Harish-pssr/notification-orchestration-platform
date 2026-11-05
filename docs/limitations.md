# Known limitations

What this system does **not** handle, why that is acceptable here, and how each gap would be handled in production. Nothing below is hidden elsewhere: where a limitation affects a claim, the claim is worded to match. See [crash-analysis.md](crash-analysis.md) for what happens at each failure point that *is* handled.

Each entry has the same shape: **the gap**, **why it exists** (a deliberate scope choice, not an oversight), and **what production would do**.

## Availability and durability

**Single points of failure.** One Postgres (platform), one Postgres (provider), one NATS node, one host. A Postgres outage stops the API and the whole pipeline; the system recovers when it returns but is not available meanwhile. *Why:* the project is about the behaviour of the pipeline, not high availability of its infrastructure. *Production:* Postgres with streaming replication and automated failover (for example Patroni or a managed service), NATS JetStream with 3 replicas, services spread across hosts.

**No backups.** The databases live in Docker volumes; losing the volume loses the data. *Production:* continuous archiving with point-in-time recovery, restore drills.

**A broker that loses its messages is repaired by a reconciler, with limits (D22).** If NATS loses messages (its storage is lost, or a consumer exhausts the broker's redelivery limit), the outbox says `PUBLISHED` while the deliveries stay open. A periodic reconciler in every relay finds deliveries still `PENDING` or `RETRYING` long after their event was published (default: 10 minutes without any sign of life), returns them to `PENDING` and hands their events back to the relay, which republishes through the normal lease-and-fence path with a message ID that carries the repair count. Each repair doubles the wait before the next; after 3 repairs the delivery is dead-lettered with a reason that names the loss (and can be replayed). The number of repairs is exported as `notif_reconciler_repaired_total` and should be zero in normal operation. *What was tested:* purging the broker's stream while consumers were paused (a simulated loss of every stored message), then verifying that every delivery ends `DELIVERED` with exactly one provider effect; the repair limit, the doubling wait, concurrent reconcilers and the message ID have their own tests. *What was not tested:* a real disk failure, or a clustered broker losing a replica. *Limits:* a consumer that is down for longer than the whole repair schedule (about 2.5 hours with the defaults: 10, 20 and 40 minutes of waiting, then 80 more) has its deliveries dead-lettered by the reconciler although nothing was lost; they stay replayable. Repairing a message that was only slow sends the consumer a second copy, which the consumer's check and the provider's idempotency key absorb. The wait must stay longer than the longest legitimate retry window, or healthy deliveries are republished needlessly. *Production:* alert on the repair counter and tune the wait to the slowest legitimate consumer outage.

**The outbox, deliveries and idempotency tables grow without bound.** Nothing archives or deletes finished events or old API idempotency keys. At the measured rates a table reaches tens of millions of rows in days. *Production:* partition the tables by time and detach or drop old partitions, a retention policy for `api_idempotency` (keys only need to outlive client retries), and autovacuum tuning (the investigation in B6 showed autovacuum running about every minute on the hot tables).

## Ordering and semantics

**No ordering guarantee.** Events are not delivered in the order they were created, even for the same account or aggregate: several relays and several concurrent consumers process rows and messages in parallel. *Why:* ordering and parallelism pull in opposite directions, and notifications such as "payment succeeded" rarely need order. *Production:* if needed, a per-key sequence number and a per-key single writer (partition by aggregate id), or consumer-side buffering that applies events in sequence order.

**"Delivered" means "the provider accepted it", not "the person received it".** There are no delivery receipts, bounces, opens or webhooks back from the provider. *Production:* an asynchronous feedback path from the real provider (callbacks or polling a status API) that moves a delivery through further states (accepted, delivered, bounced) and handles bounces and suppression lists.

**A provider that ignores idempotency keys breaks the "no duplicate side effects" claim.** The guarantee holds because the (mock) provider writes the key and the effect in one transaction under a unique constraint. Real providers vary: some honour an idempotency header, some do not. *Production:* for providers without key support, accept at-most-once for that channel (and risk loss), or reconcile with the provider's status API before retrying, or use a provider that supports keys.

**Real email: "delivered" means "accepted by the mail server", and a crash can still duplicate.** The optional real email mode (D21) cannot make the send and the idempotency record one transaction. A crash after the mail server accepted the message and before the key is marked `DONE` leaves the key `IN_PROGRESS`; after a timeout the retry sends again, so a rare duplicate email is possible. The mock does not have this window (it measured zero duplicate side effects). The system also cannot know whether the message reached the inbox, went to spam or bounced later; it records the server's acceptance reply and nothing more. *Production:* send with a stable message id and look it up in the mail system's activity API before retrying, handle bounces and delivery callbacks, and decide per message type whether a duplicate or a miss is worse.

**Real email is slow.** A send takes seconds, so a consumer with the default 5 s call timeout gives up and retries; the provider still finishes the first send and answers the retry from the stored result, but with the protections off each retry sends another real email. Use `CALL_TIMEOUT=30s` (see the README).

**Real email is for a local demo only.** One recipient, chosen in the dashboard by anyone with the admin token, so it must not be enabled on a public deployment; the SMTP password is read from the environment and never shown; the hourly cap is kept in memory and resets when the provider restarts.

**Retention of the provider's keys.** The mock keeps keys forever; real providers expire them (often after 24 hours), which bounds how late a retry can still be recognised. *Production:* keep retry windows shorter than the provider's key lifetime.

## Failure handling

**No circuit breaker.** When a provider is down, every delivery is attempted and fails individually, with backoff, until it is dead-lettered. A breaker would stop calling a provider that is clearly down. The relay does back off when the broker is unreachable (D18), but a general breaker was deferred. *Production:* a breaker per provider and channel with half-open probing, and pausing the consumer rather than burning attempts.

**No lease heartbeats.** A relay holds its claim for a fixed lease (30 s in production, 5 s in the development stack). If a publish legitimately takes longer, another relay takes the row; the fenced finish keeps the data correct, but the event may be published twice. *Production:* extend the lease while work is in progress.

**Dead-letter handling is manual.** Dead letters are kept with their reason and replayed by an operator; there is no automatic retention, alerting or auto-replay. *Production:* alerts on dead-letter count and age, an expiry policy, and a runbook per failure reason.

**Retry policy is global.** One maximum attempt count and one backoff shape for all channels. *Production:* per-channel policies (an email can wait a day, an OTP cannot) and per-provider rate limits.

## Security and compliance

**One shared admin token, no identities.** Every write (creating events, replay, fault injection) uses the same bearer token (D19). There is no per-user authentication, no roles and no audit log of who did what. Reading is open, so event payloads are visible to anyone who can reach the dashboard. *Production:* OIDC login, role-based access, an audit trail, and authenticated reads.

**No transport security between internal services,** and secrets are plain environment variables. *Production:* mutual TLS or a service mesh, a secrets manager.

**No payload governance.** There is no schema versioning beyond strict decoding of the three event types, no encryption of payloads at rest, and no handling of personal data (retention, erasure). *Production:* a schema registry and versioned event types, field-level encryption for personal data, and deletion jobs that honour privacy requests.

**No rate limiting or abuse protection** on the API. *Production:* per-client limits at the edge.

## Observability

**No distributed tracing and no alerting rules.** The system has structured logs with a correlation id (the event id), Prometheus metrics and a Grafana dashboard, but no OpenTelemetry traces, no alert rules and no service-level objectives. Live latency histograms are bucketed and approximate. *Production:* traces across API, relay, broker, consumer and provider, SLOs on delivery latency and dead-letter rate, and alerts on backlog growth.

## Performance and scale

**The measured ceiling is modest and specific to one machine.** Roughly 310 to 370 events per second (about 1,000 deliveries per second) end to end on a Windows laptop under Docker Desktop (B2, B3). The pipeline is commit-bound: each event needs several synchronous commits. *Why not pushed further:* batching the consumer's database writes was considered and rejected because one slow message would delay the others' status writes (D15). *Production:* batch and group commits where head-of-line blocking is acceptable, partition the outbox across databases, shard by tenant, and move the hot path off a single Postgres.

**The relay polls.** At low load an event waits half a poll interval (about 100 ms on average at the 200 ms default) before the relay notices it (D14). *Production:* `LISTEN/NOTIFY` as a wake-up hint with the poll kept as the safety net, or a shorter interval.

**Schema migrations are forward-only** (applied once, under an advisory lock) with no rollback and no online-change strategy for large tables. *Production:* expand-and-contract migrations and a tested rollback path.

## What the evidence covers

- **One laptop, one environment.** All performance figures come from Docker Desktop on Windows. No benchmark was run on Linux, and the production setup in [deployment.md](deployment.md) was verified locally, not on a public server (decision D20).
- **The multi-second latency stalls are explained but not proven.** They are periods of slow WAL fsync (B6); the likely cause is this environment's storage path, an inference from elimination.
- **Failure injection is partial.** Provider errors are injected before the effect runs; consumer crash loops, API and Postgres crashes, and broker storage loss are not provoked (see the end of crash-analysis.md).
- **The provider is a mock** that shares the machine's CPU (about 30% of it at the ceiling), so the platform's own capacity is understated.
- **One event type dominates the benchmarks** (PaymentSucceeded, three channels); a mixed workload would change the fan-out.
