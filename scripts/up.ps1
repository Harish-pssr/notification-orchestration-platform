# Start the whole stack locally, including Grafana (PowerShell version of scripts/up.sh).
# Run from any folder:  powershell -ExecutionPolicy Bypass -File scripts\up.ps1
# Set $env:NO_GRAFANA = "1" first to leave Grafana out.
Set-Location (Split-Path -Parent $PSScriptRoot)

$profiles = @("--profile", "grafana")
if ($env:NO_GRAFANA -eq "1") { $profiles = @() }

docker compose -f docker-compose.yml -f docker-compose.dev.yml @profiles up -d --build @args

Write-Host "Dashboard:  http://localhost:8080   (admin token for the local failure controls: dev-token)"
Write-Host "Prometheus: http://localhost:9091"
if ($env:NO_GRAFANA -ne "1") { Write-Host "Grafana:    http://localhost:3000   (no login in development)" }
Write-Host "Health:     curl localhost:8080/healthz"
