# Stop and remove the whole stack, Grafana included, keeping the data (PowerShell version of scripts/down.sh).
# Add -v to also delete the data volumes (a clean slate).
#   powershell -ExecutionPolicy Bypass -File scripts\down.ps1
# Grafana sits in a Compose profile, and "docker compose down" only touches the profiles it is told about,
# so the profile is named here whether or not up.ps1 started it.
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose -f docker-compose.yml -f docker-compose.dev.yml --profile grafana down --remove-orphans @args
