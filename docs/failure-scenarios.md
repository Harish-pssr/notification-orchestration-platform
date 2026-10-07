# Failure scenarios

Each scenario lists the failure, the expected behaviour, how it is tested, and what was observed. Only scenarios that have actually been run are marked as observed. A complete crash-at-every-step analysis and a known-gaps list are still to be written.

## Verified (automated tests)

All tests live in [`test/`](../test) and run with `scripts/test.sh` against the running stack.

### F1. Crash before the API transaction commits

- **Failure:** the transaction fails after the business row and outbox row were written (simulated by a routing table that returns an invalid channel, so the delivery insert fails).
- **Expected:** nothing is committed: no business row, no outbox row, no idempotency record.
- **Test:** `TestEventCreationIsAtomic`.
- **Observed:** passes; the payment row and idempotency key are absent after the failure.

### F2. Client retries after the commit but before the response

- **Failure:** the API committed, the client never saw the response and resends the same request.
- **Expected:** the same event is returned (flagged `Idempotent-Replayed`); exactly one event and one business row exist. Same key with a different body or endpoint returns `422`; a missing key returns `400`.
- **Test:** `TestIdempotencyKeySemantics`, `TestInvalidBodyIsRejectedWithoutSideEffects`.
- **Observed:** passes.

### F3. Two relays claim concurrently

- **Failure:** eight workers claim from 300 due events at once.
- **Expected:** every event is claimed exactly once; no event claimed by two workers.
- **Test:** `TestConcurrentClaimsAreDisjoint`.
- **Observed:** passes (300 distinct events across 8 workers, no duplicates).

### F4. A relay dies while holding a claim

- **Failure:** a relay claims a row and never finishes it.
- **Expected:** nobody else may take it during the lease; after expiry another relay claims it (attempt 2); the original relay's late finish is rejected by the fence.
- **Test:** `TestLeaseExpiryLetsAnotherWorkerTakeOver`, `TestFenceRejectsAWorkersOwnStaleAttempt` (same worker re-claiming its own expired lease).
- **Observed:** passes.

### F5. A relay publishes to the broker and dies before recording success (worst case)

- **Failure:** the broker accepted the messages, the relay crashed before the database update.
- **Expected:** after the lease expires the real relay container publishes again; the event completes with attempts of at least 2; the provider shows exactly one effect per channel even though the event was published twice.
- **Test:** `TestRelayCrashAfterPublishIsRecoveredWithoutDuplicateEffects` (a test-driven relay publishes then abandons the claim; the running relay recovers it).
- **Observed:** passes. Note: the second publish uses the same NATS message ID inside the 2-minute dedup window, so the broker may absorb it; correctness does not depend on that. Forcing the duplicate through to the consumers (dedup bypass) arrives with the fault hooks and the duplicate-delivery demo.

### F6. Failed publish backs off; poisoned row is dead-lettered

- **Expected:** a row marked failed is not claimable until its backoff elapses; a dead-lettered row is never claimed again; only `PENDING` deliveries are republished.
- **Test:** `TestFailedRowWaitsForItsBackoff`, `TestDeadLetteredRowIsNeverClaimedAgain`, `TestPendingChannelsSkipsFinishedDeliveries`.
- **Observed:** passes.

### F7. Duplicate delivery, stopped by the consumers (Demo 4)

- **Failure:** the relay publishes every message twice with fresh message IDs, so the broker's dedup window cannot absorb the copy (fault `relay.after_publish` = `duplicate`, once).
- **Expected:** each channel's consumer receives the second copy, finds the delivery already `DELIVERED`, ignores it and counts it; the provider sees 3 requests and executes 3 effects.
- **Test:** `TestDuplicateDeliveryIsIgnoredByConsumers`; also runnable from the dashboard (Idempotency, "Both on").
- **Observed:** 6 messages received by consumers, 3 ignored by consumers, 0 suppressed by the provider, 3 side effects, 0 duplicate side effects.

### F8. Duplicate delivery, stopped by the provider alone

- **Failure:** same duplicate, with the consumer's "skip if already delivered" shortcut switched off (fault `consumer.skip_delivered_check`).
- **Expected:** the duplicates reach the provider, which recognises the idempotency keys and suppresses them: 6 requests, 3 effects. This shows correctness does not depend on the consumer shortcut.
- **Test:** `TestProviderAloneSuppressesDuplicateSideEffects`; dashboard mode "Consumer shortcut off".
- **Observed:** 6 messages received, 0 ignored by consumers, 3 suppressed by the provider, 3 side effects, 0 duplicate side effects.

### F9. Negative control: duplicates with no protection

- **Failure:** the same duplicate with both the consumer shortcut and the provider's idempotency switched off.
- **Expected:** duplicate side effects appear (6 effects for 3 distinct keys). If they did not, the zeros in F7 and F8 would prove nothing, because the counters could not detect duplicates.
- **Test:** `TestNegativeControlDuplicateEffectsAreDetectedWithoutProtection`; dashboard mode "Negative control". The experiment's rows are deleted afterwards so the live counters stay honest.
- **Observed:** 6 messages received, 6 side effects, **3 duplicate side effects**, so the detector works.

### F10. Relay killed right after the broker accepted the publish (Demo 5, via fault injection)

- **Failure:** fault `relay.after_publish` = `crash`, once: the relay calls `os.Exit(137)` before recording success.
- **Expected:** Docker restarts the relay; after the lease expires (5s in dev) a relay claims the event again (attempts of at least 2) and publishes it again; the event completes with exactly one effect per channel.
- **Test:** `TestInjectedRelayCrashAfterPublishIsRecovered`. The relay's log shows `injected crash: exiting now` and Docker reports a restart count of 1.
- **Observed:** passes: attempts of at least 2, 3 effects for 3 distinct keys.

### F11. Fault controls are protected

- **Expected:** the admin endpoints exist only with `FAULTS_ENABLED=true` and always require the shared token; unknown injection points and unsupported actions are rejected; a limited fault fires exactly its count even when many processes race for it.
- **Test:** `TestAdminEndpointsRequireTheToken`, `TestLimitedFaultFiresExactlyItsCountAcrossConcurrentCallers` (20 concurrent callers, count 3, fired exactly 3 times), `TestDisabledRegistryNeverFiresAndCrashUsesTheInjectedExit`, `internal/admin` unit tests.
- **Observed:** passes.

### F12. Transient provider errors are retried until success

- **Failure:** the webhook provider returns 500 for the next 2 calls only.
- **Expected:** the consumer retries with capped, jittered backoff; the third attempt succeeds; the delivery ends `DELIVERED` with its error cleared; the provider ran exactly one effect.
- **Test:** `TestTransientProviderErrorsAreRetriedUntilSuccess` (end to end); `TestTransientErrorSchedulesARetryWithinTheJitterRange` (the retry delay for attempt 2 lies in the equal-jitter range [100 ms, 200 ms] for a 100 ms base).
- **Observed:** passes: 3 provider requests (2 failed, 1 ok), 1 effect, attempts = 3.

### F13. Retries exhausted, dead letter, restore, replay (Demo 3)

- **Failure:** the webhook provider returns 500 on every call.
- **Expected:** after 5 attempts the webhook delivery is dead-lettered with reason "retries exhausted: provider returned 500" while email and push are delivered normally; `/api/dead-letters` lists it. After the provider is restored, a replay of that one delivery delivers it, with exactly one webhook effect, and email and push are not sent again.
- **Test:** `TestRetriesExhaustedThenReplayDelivers`; also driven through the dashboard (Failure controls, Create events, Dead letters, Replay) in a headless browser.
- **Observed:** passes: 5 attempts, 5 failed requests and 0 effects before restore; after replay `replay_count = 1`, 1 webhook effect, 1 effect each for email and push. In the dev configuration the whole sequence takes about 4 seconds.

### F14. Permanent provider error is not retried

- **Failure:** the webhook provider returns 400 (the request itself is rejected).
- **Expected:** the delivery goes straight to the dead-letter state after one attempt with a "permanent error" reason; the provider sees exactly one request. Replay by channel delivers it after the provider is fixed.
- **Test:** `TestPermanentProviderErrorGoesStraightToDeadLetter`; classification table in `TestPermanentClassification` (every 4xx except 408 and 429 is permanent; 5xx, 408, 429 and transport errors are retried).
- **Observed:** passes.

### F15. Rate limiting honours `Retry-After`

- **Failure:** the provider answers 429 with `Retry-After: 3`.
- **Expected:** the next attempt is scheduled in exactly 3 s rather than our own much shorter backoff (capped by the backoff cap).
- **Test:** `TestRetryAfterHeaderLengthensTheDelay`.
- **Observed:** passes.

### F16. Crash loop on one message (poison guard)

- **Failure:** earlier deliveries of a message died before recording anything, so the broker's delivery count exceeds our maximum.
- **Expected:** the consumer dead-letters it without calling the provider again, with a "exceeded max deliveries" reason.
- **Test:** `TestPoisonGuardDeadLettersWithoutCallingTheProvider` (a message presented as its 6th delivery against a provider that would have succeeded; the provider receives zero calls).
- **Observed:** passes. Note: this verifies the guard's decision with a simulated delivery count, not a real crash loop.

### F17. Replay semantics and safety

- **Expected:** replay resets only dead deliveries of events that are already `PUBLISHED`, returns their events to the relay, increments `replay_count`, skips (and reports) deliveries of events still being published, replays relay-dead events when no channel filter applies, honours event and channel filters, and refuses an empty request. The endpoint requires the admin token.
- **Test:** `TestReplayResetsDeadDeliveriesAndReturnsEventsToTheRelay`, `TestReplayCanBeNarrowedByChannelOrEvent`, `TestReplayEndpointRules`, `TestPendingChannelsSkipsFinishedDeliveries`.
- **Observed:** passes.

## Planned

Scenarios arriving with later slices: broker down (real `docker compose stop nats`), consumer crash before ack (real crash rather than a simulated delivery count), consumer paused, outage and backlog recovery timing.
