#!/usr/bin/env bash
# Start the whole stack locally, including Grafana (with the infrastructure ports published for tests).
# Set NO_GRAFANA=1 to leave Grafana out (it adds one image download and a little memory).
set -euo pipefail
cd "$(dirname "$0")/.."

profiles=(--profile grafana)
if [ "${NO_GRAFANA:-0}" = 1 ]; then profiles=(); fi

docker compose -f docker-compose.yml -f docker-compose.dev.yml "${profiles[@]+"${profiles[@]}"}" up -d --build "$@"

echo "Dashboard:  http://localhost:8080   (admin token for the local failure controls: dev-token)"
echo "Prometheus: http://localhost:9091"
if [ "${NO_GRAFANA:-0}" != 1 ]; then echo "Grafana:    http://localhost:3000   (no login in development)"; fi
echo "Health:     curl localhost:8080/healthz"
