# Windows equivalent of stop.sh (pass -v to delete the database volume)
param([switch]$v)
Set-Location (Join-Path $PSScriptRoot "..")
if ($v) { docker compose down -v } else { docker compose down }
$left = docker ps -aq --filter "label=dtq.managed=true"
if ($left) { docker rm -f $left | Out-Null; Write-Host "Removed leftover worker containers" }
