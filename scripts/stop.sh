#!/usr/bin/env bash
# Stop the stack. Worker containers are created by the agent, so remove any leftovers explicitly.
# Pass --volumes to also delete the PostgreSQL data volume (irreversible).
set -euo pipefail
cd "$(dirname "$0")/.."
docker compose down "$@"
leftovers=$(docker ps -aq --filter label=dtq.managed=true)
if [ -n "$leftovers" ]; then docker rm -f $leftovers >/dev/null && echo "Removed leftover worker containers"; fi
