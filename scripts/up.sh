#!/usr/bin/env bash
# Start the full stack locally (with infrastructure ports published for tests).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build "$@"
echo "API: http://localhost:8080/healthz"
