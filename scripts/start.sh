#!/usr/bin/env bash
# Build and start the complete stack, then wait until it is healthy.
set -euo pipefail
cd "$(dirname "$0")/.."
[ -f .env ] || { cp .env.example .env; echo "Created .env from .env.example (edit tokens for shared deployments)"; }
docker compose up -d --build
echo "Waiting for the API to report a healthy system..."
for _ in $(seq 1 90); do
  if curl -sf "http://localhost:${DTQ_API_PORT:-8000}/api/v1/system/health" | grep -q '"status":"healthy"'; then
    echo "Healthy. Dashboard: http://localhost:${DTQ_HTTP_PORT:-8080}   API docs: http://localhost:${DTQ_API_PORT:-8000}/docs"
    exit 0
  fi
  sleep 2
done
echo "The stack did not become healthy in time; inspect: docker compose ps && docker compose logs --tail=50" >&2
exit 1
