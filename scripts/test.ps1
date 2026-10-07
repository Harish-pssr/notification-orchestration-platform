# Run vet and all tests (PowerShell version of scripts/test.sh). The integration
# tests need the stack from scripts\up.ps1; add -short to skip them.
Set-Location (Split-Path -Parent $PSScriptRoot)
go vet ./...
if ($LASTEXITCODE -eq 0) { go test -count=1 ./... @args }
