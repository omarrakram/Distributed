# Windows (Docker Desktop) equivalent of start.sh
$ErrorActionPreference = "Stop"
Set-Location (Join-Path $PSScriptRoot "..")
if (-not (Test-Path ".env")) { Copy-Item ".env.example" ".env"; Write-Host "Created .env from .env.example" }
docker compose up -d --build
Write-Host "Waiting for the API to report a healthy system..."
for ($i = 0; $i -lt 90; $i++) {
  try {
    $h = Invoke-RestMethod -Uri "http://localhost:8000/api/v1/system/health" -TimeoutSec 3
    if ($h.status -eq "healthy") { Write-Host "Healthy. Dashboard: http://localhost:8080  API docs: http://localhost:8000/docs"; exit 0 }
  } catch { }
  Start-Sleep -Seconds 2
}
Write-Error "The stack did not become healthy in time. Run: docker compose ps; docker compose logs --tail=50"
