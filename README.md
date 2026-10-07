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
API and dashboard 8080, provider 9090. Postgres uses 15433/15434 because Windows reserves 5317-5816.

## Layout

```text
cmd/        one folder per binary (api, migrate; relay, consumer, provider, loadgen later)
internal/   shared code
migrations/ SQL for the platform and provider databases (embedded in the binaries)
deploy/     Dockerfile and deployment config
scripts/    bash helpers (the same scripts run locally and on the VM)
test/       integration and failure-scenario tests
docs/       architecture, decisions, failure scenarios, benchmarks
```
