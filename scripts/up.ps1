# Start the full stack locally (PowerShell version of scripts/up.sh).
# Run from any folder:  powershell -ExecutionPolicy Bypass -File scripts\up.ps1
Set-Location (Split-Path -Parent $PSScriptRoot)
docker compose -f docker-compose.yml -f docker-compose.dev.yml up -d --build @args
Write-Host "Dashboard: http://localhost:8080   (admin token for local faults: dev-token)"
