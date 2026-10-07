#!/usr/bin/env bash
# Start or update the production stack on the VM (D10): checks the configuration, builds and
# starts everything behind Caddy, waits for HTTPS to come up and runs the smoke test.
#
#   git pull && scripts/deploy.sh
#
# Needs a filled-in .env (copy .env.example). Safe to run again: Compose only recreates what
# changed, and the data volumes are kept.
set -euo pipefail
cd "$(dirname "$0")/.."

[ -f .env ] || { echo ".env is missing: copy .env.example to .env and fill it in" >&2; exit 1; }
set -a
# shellcheck disable=SC1091
. ./.env
set +a

for name in DOMAIN ACME_EMAIL POSTGRES_PASSWORD ADMIN_TOKEN; do
  value=${!name:-}
  if [ -z "$value" ] || [ "$value" = change-me ] || [[ "$value" == *example.com ]]; then
    echo "$name is not set to a real value in .env" >&2
    exit 1
  fi
done
if [ "${FAULTS_ENABLED:-false}" != false ]; then
  echo "WARNING: FAULTS_ENABLED=${FAULTS_ENABLED} on a deployment that faces the internet (D8)" >&2
fi

docker compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build --remove-orphans

# The first start also obtains the certificate, which takes a little while.
echo "waiting for https://$DOMAIN/healthz ..."
for _ in $(seq 60); do
  if curl -fsS --max-time 5 -o /dev/null "https://$DOMAIN/healthz" 2>/dev/null; then
    scripts/smoke.sh "https://$DOMAIN"
    exit 0
  fi
  sleep 3
done
echo "https://$DOMAIN did not become healthy in 3 minutes. Check: DNS points at this machine, ports 80 and 443 are open, and 'docker compose logs caddy api'" >&2
exit 1
