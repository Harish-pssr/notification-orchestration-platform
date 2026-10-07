# Stop and remove the containers, keeping the data (PowerShell version of scripts/down.sh).
# Add -v to also delete the data volumes (a clean slate).
#   powershell -ExecutionPolicy Bypass -File scripts\down.ps1
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose -f docker-compose.yml -f docker-compose.dev.yml down --remove-orphans @args
