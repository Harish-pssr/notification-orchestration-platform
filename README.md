# Reliable Notification Orchestration Platform

A fault-tolerant asynchronous notification platform built on the transactional
outbox pattern. When a business operation commits, its notifications (email, push,
webhook) must reach the downstream systems even if the API, a relay, the message
broker, a consumer or the external provider crashes, and a repeated message must not cause a second side effect.

<p align="center"><img src="docs/images/architecture.svg" alt="Architecture: a client sends an event to the API, which saves it in PostgreSQL in one transaction; a relay publishes it to NATS JetStream; one consumer per channel calls the provider, which records one effect per idempotency key. A reconciler inside the relay repairs deliveries the broker lost." width="100%"></p>

1. The client sends an event with an `Idempotency-Key`. 2. The API saves the business row, an outbox record and one delivery row per channel in **one transaction**. 3. A relay claims due outbox rows with a lease. 4. It publishes one small message per channel to NATS JetStream, waits for the broker's confirmation, then marks the event published. 5. Each channel's consumer pulls its messages. 6. The consumer calls the provider with an idempotency key; the provider records the key and the effect in one transaction. 7. The consumer records the delivery as done. 8. In the background, the **reconciler** (the dashed box, inside the relay) finds deliveries the broker lost after confirming them and sends them back through the relay to be published again.

> **Scope: what is real and what is mocked.** The pipeline (two PostgreSQL databases, NATS JetStream, relays, consumers), the load, the injected failures (real `SIGKILL`s, real container stops, a purge of the broker's stream) and the counting are real. **The provider is a mock**: it stands in for the email, push and webhook services, has its own database, and an "effect" is a row it records, not a delivered email or push. It honors idempotency keys, so the claim is **"0 duplicate side effects at a provider that honors idempotency keys"**; it is not a measurement of real notification delivery. For a provider that does not honor keys (for example real SMTP, an optional mode here) the guarantee is at-least-once with a rare duplicate. All results are from one Windows laptop under Docker Desktop; there are no Linux numbers yet.

## What it demonstrates

- **Transactional outbox** with concurrent relay workers: the business change and the
  event commit in one transaction, and relays claim work with `FOR UPDATE SKIP LOCKED`
  and a lease stored as data, with fenced updates so a slow worker cannot corrupt a row.
- **At-least-once delivery with idempotent effects:** duplicates are expected and made
  harmless by an idempotency key written with the effect in one transaction. It never
  claims exactly-once delivery.
- **Retries, dead letters and replay:** capped exponential backoff with jitter, permanent
  versus transient errors, a poison guard, and a safe operator replay.
- **A reconciler for lost broker messages:** deliveries still open long after their event
  was published are sent back through the relay, with a repair limit. Tested by purging
  the broker's stream mid-run (a simulated loss, not a disk failure).
- **Failure injection and experiments:** real crashes, duplicates, provider errors and
  outages, with exact counts and a negative control that proves the counters can see duplicates.
- **Measured, not claimed:** open-loop load through the HTTP API, exact latency from
  database timestamps, and every result recorded with its method and limitations.
- **A live dashboard** that explains itself: pipeline, events, health, dead letters,
  failure controls and the duplicate-protection experiment.

## Results (one Windows laptop, Docker Desktop)

| | Result |
|---|---|
| Reliability under failure | 99,999 events with 3 relay SIGKILLs, about 15,000 injected duplicate messages and about 2% failing provider calls: every delivery completed, 0 dead letters, **0 duplicate side effects at a provider that honors idempotency keys** (with the protections off, the same run produced 1,425) |
| Outages | the relay, and separately the broker, stopped for 60 s under load, 3 times each: nothing lost, backlog drained about 100 s after each restart |
| Lost broker messages | the broker's stream purged under load, 3 times: about 3,200 lost deliveries per run, all found and republished by the reconciler, 0 duplicate side effects (a simulated loss, not a disk failure) |
| Capacity | roughly 310 to 370 events per second end to end; running 8 messages at a time per consumer lifted one consumer's capacity from 98 to 153 events/s |

Full method, tables and limitations are in [docs/benchmarks.md](docs/benchmarks.md). These
figures describe one machine and are not a promise about any other.

## The dashboard

The dashboard is built into the API and explains itself. First, the overview with the system running normally (40 events, every delivery completed, nothing stuck):

<p align="center"><img src="docs/images/dashboard-overview.png" alt="The overview page: 40 events created, 94 of 94 deliveries completed, nothing needing attention, median latency 128 ms, and the pipeline with every channel delivered." width="90%"></p>

The **Duplicates** page runs the central experiment: one payment, every message published twice. On the left, both protections on: six messages arrive, three effects are recorded, **0 duplicate side effects**. On the right, the **negative control** with both protections off: the same duplicates get through (**3 duplicate side effects**), which shows the counters can see duplicates and the zero on the left means something. (The effects are rows at the mock provider.)

<table>
<tr>
<th width="50%">Both protections on</th>
<th width="50%">Negative control: both protections off</th>
</tr>
<tr>
<td><img src="docs/images/dashboard-duplicates-normal.png" alt="Result card, both protections on: 6 messages received by consumers, 3 stopped by the provider's idempotency key, 3 side effects executed, 0 duplicate side effects."></td>
<td><img src="docs/images/dashboard-duplicates-control.png" alt="Result card, negative control with both protections off: 6 side effects executed, 3 duplicate side effects got through."></td>
</tr>
</table>

## Documentation

| | |
|---|---|
| [docs/architecture.md](docs/architecture.md) | how it works: components, data flow, state models, guarantees |
| [docs/crash-analysis.md](docs/crash-analysis.md) | what a crash at each step of an event's flow leaves behind, how it recovers, and how that is tested |
| [docs/failure-scenarios.md](docs/failure-scenarios.md) | the failures it is tested against, with what was observed |
| [docs/benchmarks.md](docs/benchmarks.md) | measured results, with environment, method and limitations |
| [docs/limitations.md](docs/limitations.md) | what it does not handle, and how production would |
| [docs/deployment.md](docs/deployment.md) | the production setup (verified locally, not deployed to a public server) |

## Run locally

**You need:** Docker (Docker Desktop on Windows or macOS; Docker Engine with the Compose plugin on Linux).
Go 1.27 is needed only to run the tests or build the benchmark tools on your machine; the stack itself is
built inside Docker.

```bash
# from the root folder of this repository
scripts/up.sh            # Linux, macOS, or Git Bash on Windows
# Windows PowerShell:    powershell -ExecutionPolicy Bypass -File scripts\up.ps1
```

The first run builds the images, which takes a few minutes. It starts two Postgres databases (the platform's and
the mock provider's), NATS JetStream, the migrations, the provider, the API and dashboard, one relay, three
consumers (email, push, webhook), Prometheus and Grafana. Set `NO_GRAFANA=1` before `scripts/up.sh` (or
`$env:NO_GRAFANA = "1"` in PowerShell) to leave Grafana out.

Then:

- **Dashboard:** http://localhost:8080. A first visit opens "Getting started". The pages that change things
  (Failures, Duplicates experiment, Dead letters replay, Health pause) need the admin token **`dev-token`**, which
  you type into the box at the top right. It is a local development token; the production setup requires a real one.
- **Check it is alive:** `curl localhost:8080/healthz`
- **Send an event without the UI** (any unique string works as the key):

  ```bash
  curl -X POST localhost:8080/api/events/payment-succeeded \
    -H "Idempotency-Key: demo-1" -H "Content-Type: application/json" \
    -d '{"account_id":"acct-1","amount_cents":4999,"currency":"USD"}'
  ```

  Sending the same request again returns the same event instead of creating a second one.
- **Run the tests:** `scripts/test.sh` (vet plus every test; the integration tests need the stack to be up).
  Add `-short` to run only the tests that need no infrastructure.
- **Stop:** `scripts/down.sh` stops everything, Grafana included, and keeps the data; `scripts/down.sh -v` deletes
  it for a clean slate. (PowerShell: `scripts\down.ps1`.)

All published ports are bound to `127.0.0.1` (this machine only):

| Port | What |
|---|---|
| 8080 | API and dashboard |
| 15433 | platform Postgres (user `notif`, password `notif`, database `platform`) |
| 15434 | provider Postgres (same credentials, database `provider`) |
| 4222, 18222 | NATS client port and NATS monitoring (published as 18222; Windows reserves 8222 on some machines) |
| 9090 | mock provider |
| 9091 | Prometheus |
| 3000 | Grafana (started by `scripts/up.sh`; no login in development) |

Postgres uses 15433 and 15434 because Windows reserves the range 5317 to 5816.

### Real email (optional)

By default the email channel is a mock: nothing is ever sent. To see the "one email however many times the broker delivers it" guarantee in a real inbox, give the provider an SMTP account in an uncommitted `.env` (see `.env.example`). Gmail works with an **App password** (turn on 2-Step Verification, then Google Account, Security, App passwords):

```bash
EMAIL_SMTP_HOST=smtp.gmail.com
EMAIL_SMTP_PORT=587
EMAIL_SMTP_USER=you@gmail.com
EMAIL_SMTP_PASS=the-16-character-app-password
EMAIL_TO=you@gmail.com
CALL_TIMEOUT=30s          # a real send takes several seconds; the default 5 s would make consumers time out and retry
```

Restart the provider (`scripts/up.sh`), open **Duplicates**, use **Send test email** to check the settings, then switch on **Send live emails** and run the experiment: one email arrives even though the provider received each request twice. The sidebar shows "Live email sending: ON" or "OFF" on every page. The switch always starts off after a restart, only the email channel is real (to the one recipient, which can be changed on that page), at most 20 emails per hour are sent (`EMAIL_MAX_PER_HOUR`), and the benchmark tools refuse to run while it is on. Never enable it on a public deployment. A real send cannot share a transaction with the idempotency key, so for real email the guarantee is at least once with a rare duplicate if the provider crashes between the send and its record (D21, [limitations](docs/limitations.md)).

## Look inside (beyond the dashboard)

Everything the dashboard shows comes from these places, which you can query directly.

**Logs.** Every process writes one JSON line per action to its container's output. The fields are `event_id`,
`event_type`, `worker` or `consumer`, `attempt` and `status` (for example `published`, `delivered`, `retrying`,
`dlq`, `fence_lost`, `broker_unavailable`):

```bash
DC="docker compose -f docker-compose.yml -f docker-compose.dev.yml"
$DC logs -f relay consumer-email                                              # follow two services
$DC logs api relay consumer-email consumer-push consumer-webhook | grep <event_id>   # follow one event everywhere
```

**The platform database** (the source of truth):

```bash
$DC exec postgres-platform psql -U notif -d platform     # or: psql postgres://notif:notif@localhost:15433/platform
```

```sql
SELECT status, count(*) FROM outbox_events GROUP BY status;                    -- where events are
SELECT channel, status, count(*) FROM channel_deliveries GROUP BY 1, 2;        -- where each delivery is
SELECT id, status, attempts, last_error, created_at, published_at
  FROM outbox_events ORDER BY created_at DESC LIMIT 5;                         -- the newest events
SELECT * FROM channel_deliveries WHERE event_id = '<event id>';                -- one event's deliveries
SELECT name, role, state, config, last_seen FROM workers ORDER BY role, name;  -- who is running, with settings
SELECT * FROM fault_config;                                                    -- injected faults
```

**The provider database** (the "external" system, separate on purpose). It is where duplicate side effects would
show up:

```bash
$DC exec postgres-provider psql -U notif -d provider
```

```sql
SELECT count(*) AS requests, count(DISTINCT key) AS distinct_keys FROM requests;
SELECT count(*) AS effects, count(*) - count(DISTINCT key) AS duplicate_side_effects FROM effects;   -- must be 0
```

The same counts are available as `curl localhost:9090/stats`.

**The broker:** `curl "localhost:18222/jsz?streams=true&consumers=true"` shows the `NOTIFY` stream and each
channel's consumer (messages pending, awaiting acknowledgement, redelivered).

**Metrics.** `curl localhost:8080/metrics` serves the API's counters and the backlog gauges. Prometheus at
http://localhost:9091 scrapes every process (Status, Targets lists them); try `notif_outbox_waiting_events` or
`sum(rate(notif_events_created_total[1m]))`. The relay and consumers serve metrics on port 9100 inside the Docker
network. **Grafana** (http://localhost:3000, no login in development) starts with `scripts/up.sh` and comes with a
provisioned dashboard called "Notification platform" (throughput, backlog, dead letters, role health, latencies,
CPU and memory per process).

Live histograms are approximate by design; the benchmark figures in [docs/benchmarks.md](docs/benchmarks.md) are
computed from exact database timestamps.

**If something does not work:**

- *`Permission denied` running a script:* `chmod +x scripts/*.sh`.
- *The integration tests fail immediately:* the stack is not running; start it with `scripts/up.sh`.
- *Docker errors about the engine or a pipe:* Docker Desktop is not running.
- *A port is already in use:* stop whatever uses it, or change the published port in `docker-compose.dev.yml`.
- *Start from scratch:* `scripts/down.sh -v && scripts/up.sh`.

## Deployment

A production setup (the same Compose stack behind Caddy with automatic HTTPS) is included and was smoke-tested locally; it has not been deployed to a public server. See [docs/deployment.md](docs/deployment.md).
`scripts/deploy.sh` checks the configuration, starts everything and runs `scripts/smoke.sh`; fault injection is
off by default and no database or broker port is ever published beyond the machine.

## Benchmarks and failure experiments

These reset all data, so they need the dev stack (`scripts/up.sh`) and refuse to run on battery power.
Results, method and limitations are in [docs/benchmarks.md](docs/benchmarks.md).

```bash
go build -o bin/bench ./cmd/bench
scripts/bench-capacity.sh      # capacity and latency by relay and consumer count (open-loop load through the API)
scripts/bench-reliability.sh   # 100,000 events with duplicates, relay crashes and provider errors; negative control;
                               # 60 s relay and broker outages
```

While an experiment runs, the dashboard shows an **Experiment** page: what is being varied, the current
phase, and live counters that should converge.

## Layout

```text
cmd/        one folder per binary (api, relay, consumer, provider, migrate, loadgen, bench)
internal/   shared code
migrations/ SQL for the platform and provider databases (embedded in the binaries)
web/        the dashboard (embedded in the API binary)
deploy/     Dockerfile, Prometheus and Grafana configuration
scripts/    bash and PowerShell helpers
test/       integration and failure-scenario tests
docs/       architecture, crash analysis, failure scenarios, benchmarks, limitations, deployment
```

## License

MIT. See [LICENSE](LICENSE).
