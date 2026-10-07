#!/usr/bin/env bash
# Stop the whole stack, Grafana included. The data is kept; pass -v to also delete the data volumes
# (a clean slate).
#
# Grafana sits in a Compose profile, and "docker compose down" only touches the profiles it is told
# about, so the profile is named here whether or not up.sh started it.
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile grafana down --remove-orphans "$@"
