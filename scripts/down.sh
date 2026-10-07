#!/usr/bin/env bash
# Stop the stack. Pass -v to also delete the data volumes (a clean slate).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose -f docker-compose.yml -f docker-compose.dev.yml down "$@"
