# Crash analysis: what happens when each step fails

This document follows one event through the system and asks, at every step: **what if the process dies (or the dependency fails) right here?** For each point it states what is left behind, how the system recovers, whether a duplicate can result, and how the claim is tested. It is deliberately honest about the points that are **not** tested.

The two ideas that make recovery work:

- **The database is the source of truth.** An event exists once its transaction commits. Everything after that is retryable, repeatable and allowed to crash.
- **At least once, with idempotent effects.** A message may be delivered more than once; the provider performs each notification once because the idempotency key and the effect are written in one transaction.

How to read the "Tested" column: **test** = an automated test in `test/` (named); **experiment** = a real-failure experiment in [benchmarks.md](benchmarks.md) (B4: SIGKILLs of relays under load; B5: 60-second stops of the relay and NATS); **simulated** = the condition is simulated, not a real crash; **not tested** = reasoned from the code but never provoked.

## The flow, step by step

```text
 client -> (1) API handler -> (2) commit -> (3) relay claim -> (4) publish -> (5) mark PUBLISHED
        -> (6) broker -> (7) consumer fetch -> (8) read delivery -> (9) provider call
        -> (10) provider commit -> (11) mark DELIVERED -> (12) ack
```

| # | Crash or failure at this point | What is left behind | Recovery | Duplicate? | Tested |
|---|---|---|---|---|---|
| 1a | Request never reaches the API (network, API down) | Nothing | The client retries with the same `Idempotency-Key` | No | not tested (nothing to recover) |
| 1b | API dies **before the transaction commits** | Nothing: Postgres rolls the transaction back, so no business row, no outbox row, no key | The client retries; the retry is a first request | No | test `TestEventCreationIsAtomic` (a failure inside the transaction leaves nothing); a real API kill is **not tested** |
| 2 | API dies **after commit, before the response** (the client never learns it worked) | The event is fully written, including the idempotency record | The client retries with the same key; the API finds the key and **replays the stored response** (header `Idempotent-Replayed: true`) instead of creating a second event | No | test `TestIdempotencyKeySemantics` (same key replays; different body or endpoint is 422; missing key is 400) |
| 2b | The client retries with a **different** key | A second, legitimate-looking event | Not preventable by the platform: a new key means a new operation. The client owns key generation | Yes (by the client's choice) | by definition |
| 3 | Relay dies **after claiming, before publishing** | Row `PROCESSING`, `locked_by` set, `next_attempt_at` = lease end | When the lease expires the claim query's `next_attempt_at <= now()` matches the row again; another relay claims it with `attempts + 1`. A row whose relays keep dying is dead-lettered by the **poison guard** once `attempts > MAX_ATTEMPTS` | No | test `TestLeaseExpiryLetsAnotherWorkerTakeOver`; poison guard for the relay: not tested separately |
| 4a | Relay dies **while publishing**, after some channel messages were accepted by the broker | Some messages in the broker, row `PROCESSING` | After the lease expires another relay publishes every delivery still `PENDING`. The broker drops an exact repeat inside its 2-minute de-duplication window (an optimisation), and any repeat that gets through is absorbed downstream | Possible, absorbed | test `TestPendingChannelsSkipsFinishedDeliveries` (only `PENDING` deliveries are republished); experiment B4 (real SIGKILLs) |
| 4b | Publish **fails** (broker unreachable, timeout) | Row `PROCESSING` | Broker failures do **not spend an attempt**: the row goes back to `RETRYING` with its attempt given back, the rest of the batch is released untried, and the relay backs off one second (D18). An outage of any length never dead-letters an event | No | test `TestBrokerOutageNeverDeadLettersAnEvent`, `TestBrokerFailureReleasesTheRestOfTheBatchWithoutTryingIt` (fake broker); experiment B5 (real NATS stop, 3 repetitions) |
| 5a | Relay dies **after the broker accepted the publish, before marking the row `PUBLISHED`** (the dangerous gap) | Messages in the broker; row still `PROCESSING` | Lease expires, another relay republishes. Consumers and the provider absorb the repeat. This is the worst case from the project brief | **Yes**, absorbed | tests `TestRelayCrashAfterPublishIsRecoveredWithoutDuplicateEffects`, `TestInjectedRelayCrashAfterPublishIsRecovered` (real process exit through a fault hook); experiment B4 (3 SIGKILLs at random times, about 15,000 duplicate copies absorbed, 0 duplicate side effects) |
| 5b | The `MarkPublished` update itself **fails** (database error) | Messages published; row `PROCESSING` | Same as 5a: the lease expires and the event is republished | Yes, absorbed | not tested separately (same path as 5a) |
| 5c | A relay is **frozen** past its lease, then wakes and finishes | Another relay already owns the row | The finish is **fenced** on `(locked_by, attempts)`: it matches 0 rows, so the stale relay changes nothing | Possibly one extra publish | tests `TestFenceRejectsAWorkersOwnStaleAttempt`, `TestConcurrentClaimsAreDisjoint` |
| 6a | The broker is **down** | Events wait in the outbox; the API keeps accepting | Relays retry every second per batch; when NATS returns the backlog drains (about 100 s after a 60 s outage at 250 events/s) | No | experiment B5 (3 repetitions: nothing lost, 0 dead letters, 0 duplicate side effects) |
| 6b | The broker **loses its stored messages** (disk lost) | Outbox says `PUBLISHED`, but the message is gone and the delivery stays `PENDING` | **Not handled.** See limitations: a reconciler is needed | n/a | not tested |
| 7 | Consumer dies **after fetching, before processing** | Message delivered but unacked | After `AckWait` (30 s) the broker redelivers it; `NumDelivered` (the attempt number) grows. If the consumer keeps dying, the **poison guard** dead-letters the delivery once `attempt > MAX_ATTEMPTS`, without calling the provider; the broker stops redelivering at `MAX_ATTEMPTS + 3` | No | poison guard: test `TestPoisonGuardDeadLettersWithoutCallingTheProvider` (**simulated** delivery count); a real crash loop is **not tested** |
| 8 | The delivery lookup **fails** (database error) | Message unacked | The consumer sends a delayed negative acknowledgement (`NakWithDelay`) and the message returns | No | not tested |
| 9a | The **provider call fails** (5xx, 408, 429, timeout, connection error) | Delivery row `RETRYING` with `last_error` | Retried by the broker with equal-jitter exponential backoff (or the provider's `Retry-After`, capped), until `MAX_ATTEMPTS`, then dead-lettered; a replay is then possible | No | tests `TestTransientProviderErrorsAreRetriedUntilSuccess`, `TestRetriesExhaustedThenReplayDelivers`, `TestRetryAfterHeaderLengthensTheDelay`; experiment B4 (about 6,500 injected 503s, all retried to success) |
| 9b | The provider answers a **permanent error** (4xx except 408/429) | Nothing retryable | Straight to the dead letters: retrying the identical request cannot help | No | tests `TestPermanentProviderErrorGoesStraightToDeadLetter`, `TestPermanentErrorIsDeadLetteredWithoutRetry` |
| 9c | The consumer dies **during** the provider call | The effect may or may not have happened | The message is redelivered and the call repeats with the **same idempotency key**; the provider returns the stored result if the effect already ran | Possibly, suppressed by the provider | test `TestProviderAloneSuppressesDuplicateSideEffects`; experiment B4 |
| 10a | The provider dies **before its transaction commits** | No effect, no key | The retry runs the effect for the first time | No | not tested (the provider is a mock) |
| 10b | The provider dies **after commit, before answering** | The effect and the key are committed | The consumer sees a connection error or timeout and retries; the key is found and the stored response returned, so no second effect | No second effect | covered by the same mechanism as 9c; not provoked directly |
| 11 | Consumer dies **after the provider succeeded, before recording `DELIVERED`** | Provider has the effect; delivery row not `DELIVERED` | Redelivery, the same key replays the stored result, then the row is updated | A repeat request, no second effect | test `TestProviderAloneSuppressesDuplicateSideEffects` |
| 12 | Consumer dies **after recording, before acking** (or the ack is lost) | Delivery `DELIVERED`, message unacked | Redelivery; the consumer sees `DELIVERED`, counts `duplicate_count`, and just acks | Yes, ignored by the consumer | test `TestDuplicateDeliveryIsAbsorbedWithoutASecondSideEffect` |
| 13 | **Two copies of one message are in progress at once** (concurrent consumers, D15) | Both pass the "already delivered?" check | Both call the provider; the unique key lets one run the effect and returns the stored result to the other | Yes, suppressed by the provider | same test; experiment B4 (14,601 suppressed by the provider, 591 ignored by consumers) |
| 14 | **Platform Postgres goes down** | Everything committed is durable; in-flight transactions roll back | The API answers 5xx (clients retry with the same key), relays log the error and poll again, consumers send delayed negative acknowledgements, `/healthz` reports unhealthy. Work resumes when Postgres returns | No | **not tested** (Postgres is a single point of failure; see limitations) |
| 15 | **Provider is down** for a while | Deliveries `RETRYING` | Retried with backoff, then dead-lettered; the operator replays them when it is back | No | tests above (injected errors); a real provider outage is **not tested** |
| 16 | A **replay** is interrupted | Nothing partial: replay is one transaction | Either all matching dead deliveries return to `PENDING` or none do. The relay (not the replay) republishes, so a crash right after the replay loses nothing | No | tests `TestReplayResetsDeadDeliveriesAndReturnsEventsToTheRelay`, `TestReplayCanBeNarrowedByChannelOrEvent`, `TestReplayEndpointRules` |

## Reading the table in one sentence each

- **Before the commit** the event does not exist, so the client simply retries.
- **After the commit** it exists forever, and every later failure is a retry of an idempotent step.
- **The only place a duplicate is created** is "work happened but the record of it was lost" (rows 4a, 5, 9c, 11, 12); the receiver's idempotency key is what makes those harmless, and the consumer's "already delivered" check is only an optimisation on top of it.
- **No step relies on a lock being held** across a network call: the claim is a lease stored as data (`next_attempt_at`), so a dead worker's work becomes claimable again by itself.

## What is tested for real, and what is not

Provoked with **real failures**: relays killed with SIGKILL at random times under load (B4, three kills in a 100,000-event run); the relay and NATS stopped for 60 seconds under load, three times each (B5); the relay crashing through a real process exit inside the publish-to-finish gap (the fault hook tests); duplicates injected into the broker and the provider with the protections on and off (B4, with its negative control).

**Simulated or not provoked**, stated plainly so nobody has to find it:
- a real consumer crash loop (the poison guard is verified with a simulated delivery count);
- a real API crash, a real provider crash (it is a mock), and a Postgres outage;
- a broker that loses its stored messages (row 6b);
- provider errors are injected **before** the effect runs, so the "effect committed, response lost" case relies on the crash and redelivery paths above rather than on error injection.

See [limitations.md](limitations.md) for what the system does not handle and how it could be handled in production.
