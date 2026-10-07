# Reliable Notification Orchestration Platform

A fault-tolerant asynchronous notification platform built on the transactional
outbox pattern: at-least-once delivery, concurrent relay workers, idempotent
consumers, retries, a dead-letter state, replay and failure injection.

Status: under construction in vertical slices. See
[docs/architecture.md](docs/architecture.md) for how it works,
[docs/failure-scenarios.md](docs/failure-scenarios.md) for the failures it is
tested against, and [docs/benchmarks.md](docs/benchmarks.md) for measured results.

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
docs/       architecture, failure scenarios, benchmarks
```
