# Reliable Notification Orchestration Platform

A fault-tolerant asynchronous notification platform built on the transactional
outbox pattern. When a business operation commits, its notifications (email, push,
webhook) must reach the downstream systems even if the API, a relay, the message
broker, a consumer or the external provider crashes, and nobody may be notified twice.

## What it demonstrates

- **Transactional outbox** with concurrent relay workers: the business change and the
  event commit in one transaction, and relays claim work with `FOR UPDATE SKIP LOCKED`
  and a lease stored as data, with fenced updates so a slow worker cannot corrupt a row.
- **At-least-once delivery with idempotent effects:** duplicates are expected and made
  harmless by an idempotency key written with the effect in one transaction. It never
  claims exactly-once delivery.
- **Retries, dead letters and replay:** capped exponential backoff with jitter, permanent
  versus transient errors, a poison guard, and a safe operator replay.
- **Failure injection and experiments:** real crashes, duplicates, provider errors and
  outages, with exact counts and a negative control that proves the counters can see duplicates.
- **Measured, not claimed:** open-loop load through the HTTP API, exact latency from
  database timestamps, and every result recorded with its method and limitations.
- **A live dashboard** that explains itself: pipeline, events, health, dead letters,
  failure controls and the duplicate-protection experiment.

## Results (one Windows laptop, Docker Desktop)

| | Result |
|---|---|
| Reliability under failure | 99,999 events with 3 relay SIGKILLs, about 15,000 injected duplicate messages and about 2% failing provider calls: every delivery completed, 0 dead letters, **0 duplicate side effects** (with the protections off, the same run produced 1,425) |
| Outages | the relay, and separately the broker, stopped for 60 s under load, 3 times each: nothing lost, backlog drained about 100 s after each restart |
| Capacity | roughly 310 to 370 events per second end to end; running 8 messages at a time per consumer lifted one consumer's capacity from 98 to 153 events/s |

Full method, tables and limitations are in [docs/benchmarks.md](docs/benchmarks.md). These
figures describe one machine and are not a promise about any other.

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

Requires Go and Docker Desktop.

```bash
scripts/up.sh          # build and start Postgres x2, NATS, migrations, API
curl localhost:8080/healthz
scripts/test.sh        # vet + all tests (integration tests need the stack up)
scripts/down.sh -v     # stop and delete data
```

Open http://localhost:8080 for the dashboard (create events, watch them flow).
`scripts/up.sh` enables fault injection locally with the admin token `dev-token`
(enter it in the dashboard header). On any other deployment faults are off unless
you set `FAULTS_ENABLED=true` and `ADMIN_TOKEN` in `.env`.

Local-only ports: Postgres 15433 (platform) and 15434 (provider), NATS 4222/8222,
API and dashboard 8080, provider 9090, Prometheus 9091, Grafana 3000.
Postgres uses 15433/15434 because Windows reserves 5317-5816.

## Metrics

Every process exposes Prometheus metrics (`/metrics`): the API and provider on their
HTTP ports, the relay and each consumer on port 9100. Prometheus is part of the stack
and finds every replica automatically. Grafana with a provisioned dashboard is
optional:

```bash
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile grafana up -d
# Prometheus: http://localhost:9091    Grafana (dev, no login): http://localhost:3000
```

Live histograms are approximate by design; benchmark figures in
[docs/benchmarks.md](docs/benchmarks.md) are computed from exact database timestamps.

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
