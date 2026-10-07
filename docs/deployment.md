# Deployment

> **Status: verified locally, not deployed.** The production setup below (Compose overlay, Caddy with automatic HTTPS, `scripts/deploy.sh`, `scripts/smoke.sh`) was built and tested on a development machine, with Caddy using its own local certificate authority on test ports, and the smoke test passed. It has **not** been run on a public server with a real domain and a Let's Encrypt certificate, and no benchmarks were run on a cloud VM. Treat the steps for a real server as a runbook that has not been exercised end to end.

One Linux VM runs the same Docker Compose stack as local development, with Caddy in front for automatic HTTPS. The VM is the only thing obtained from outside: both databases, the broker, the mock provider and Prometheus run on it as containers.

## What runs where

| Container | Published on the host | Notes |
|---|---|---|
| caddy | 80, 443 (and 443/udp) | The only public entry: HTTPS certificates, reverse proxy to the API |
| api | nothing (127.0.0.1:8080 in the base file only; the production overlay removes it) | Serves the dashboard and the HTTP API |
| relay, consumer-email, consumer-push, consumer-webhook | nothing | Scale with `--scale relay=N` |
| provider (the mock external system) | nothing | Has its own Postgres, so the boundary is real |
| postgres-platform, postgres-provider, nats, prometheus | nothing | Data in named volumes |

Every port that Compose publishes in this repository is bound to `127.0.0.1`. That matters: Docker publishes ports by writing its own firewall rules, which bypass host firewalls such as `ufw`, so a port published on all interfaces would be reachable from the internet whatever `ufw` says.

## What you need

- A Linux VM, x86-64 or ARM64, **2 vCPU, 4 GB RAM and 40 GB disk or more** (a starting estimate; check with `docker stats`). Docker Engine with the Compose plugin (v2.24 or newer).
- A domain name with an **A record** (and AAAA, if you use IPv6) pointing at the VM, so Caddy can obtain a certificate. HTTPS is not optional: the admin token travels in a header.
- The VM's firewall (the cloud provider's security group) open for **80 and 443** to the world and **22 (SSH)** restricted to your own address.

## First deployment

```bash
git clone <this repository> notification-platform && cd notification-platform
cp .env.example .env
$EDITOR .env        # DOMAIN, ACME_EMAIL, POSTGRES_PASSWORD, ADMIN_TOKEN (openssl rand -hex 24)
scripts/deploy.sh
```

`scripts/deploy.sh` refuses to continue with a missing or placeholder secret, builds and starts the stack, waits for `https://$DOMAIN/healthz`, and runs `scripts/smoke.sh`, which checks health, creates one event and follows it to delivery, checks that a repeated idempotency key replays the stored response, and checks that the admin endpoints refuse requests without the token.

Secrets live only in `.env` (git-ignored). The Postgres password is applied when the data volume is first created, so changing it later needs the database's own `ALTER USER`, not just a new `.env`.

## Updating

```bash
git pull && scripts/deploy.sh
```

Compose recreates only what changed, and the volumes (databases, broker storage, certificates) are kept. Migrations run automatically through the `migrate-*` containers.

## Defaults that protect a public deployment

- **Fault injection is off** (`FAULTS_ENABLED=false`). The failure-injection endpoints answer 403, so nobody can crash a worker or break the provider through the dashboard. It is meant for the local and benchmark setups.
- **Replay needs the admin token**; it is an operator action, not fault injection, so it works with faults off.
- Postgres, NATS and the provider are never published.
- **One credential, the admin token, for every write.** Anyone can open the dashboard and read events; creating an event, replaying dead letters and (if enabled) fault injection all need the same `ADMIN_TOKEN`, entered in the box at the top right of the dashboard (the Create page's Send button stays disabled until it is). The production overlay sets `CREATE_REQUIRES_TOKEN=true` to make event creation require it; locally it is off, so the benchmarks and tests can create events freely. Give the token only to the people you want to let try it. There is no login system beyond that: payloads are readable by anyone, so a public deployment should carry only demo data.

## Operating notes

- **Logs:** each container logs JSON to stdout. Rotate Docker's logs so they cannot fill the disk: in `/etc/docker/daemon.json` set `{"log-driver": "json-file", "log-opts": {"max-size": "10m", "max-file": "3"}}` and restart Docker.
- **Restarts:** every service has `restart: unless-stopped`, so a crashed container comes back and a reboot brings the stack up by itself.
- **Backups:** none in this version. The databases are in the Docker volumes `pg-platform` and `pg-provider`; losing the VM loses them.
- **Single point of failure:** one VM. That is a deliberate trade-off for a project whose subject is the behaviour of the pipeline, not high availability of the host.
- **Teardown:** `docker compose -f docker-compose.yml -f docker-compose.prod.yml down` stops it; add `-v` to delete the data.

## Running the benchmarks on the VM

The benchmarks reset all data and need ports for the databases, which the production overlay does not publish. Stop the public stack first, run the benchmark on the loopback-only development ports, and bring the public stack back afterwards:

```bash
docker compose -f docker-compose.yml -f docker-compose.prod.yml down        # keeps volumes
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build # loopback-only ports, faults on with the dev token
go build -o bin/bench ./cmd/bench                                            # needs Go (the version in go.mod)
ONLY='cap-r1c1-first|cap-r4c4' OUT=benchmark-results-vm scripts/bench-capacity.sh
docker compose -f docker-compose.yml -f docker-compose.dev.yml down
scripts/deploy.sh
```

While the dev overlay is up the dashboard is reachable only from the VM itself (every published port is on `127.0.0.1`), and Caddy is not running, so nothing public is exposed even though fault injection is on. The benchmark tool records the VM's CPU model, hypervisor product name and Docker's CPU and memory in `environment.json` next to the results. Results from the VM and from the laptop are labelled by environment in `docs/benchmarks.md` and are not interchangeable.
