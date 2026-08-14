#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

env_file="${ORABBIT_ENV_FILE:-.env.worker}"
scale="${WORKER_SCALE:-1}"
build_on_deploy="${ORABBIT_BUILD_ON_DEPLOY:-true}"
started_at=$(date +%s)

log() {
  printf '[orabbit-deploy] %s\n' "$*"
}

elapsed() {
  printf '%ss' "$(( $(date +%s) - $1 ))"
}

debug_info() {
  if [ "${ORABBIT_DEPLOY_DEBUG:-0}" != "1" ]; then
    return
  fi

  log "debug timestamp: $(date -u '+%Y-%m-%dT%H:%M:%SZ')"
  log "debug hostname: $(hostname)"
  log "debug architecture: $(uname -m)"
  docker version --format '[orabbit-deploy] debug docker client={{.Client.Version}} server={{.Server.Version}}' 2>/dev/null || log "debug docker version unavailable"
  docker compose version 2>/dev/null || log "debug docker compose version unavailable"
  docker buildx inspect 2>/dev/null || log "debug docker buildx inspect unavailable"
  docker system df 2>/dev/null || log "debug docker disk usage unavailable"
}

case "$build_on_deploy" in
  true|false) ;;
  *)
    echo "ORABBIT_BUILD_ON_DEPLOY must be true or false" >&2
    exit 2
    ;;
esac

debug_info

if [ ! -f "$env_file" ]; then
  echo "missing $env_file; copy .env.worker.example to $env_file and edit it first" >&2
  exit 2
fi

phase_started=$(date +%s)
log "worker network setup started"
if ! docker network inspect orabbit-control-plane >/dev/null 2>&1; then
  docker network create orabbit-control-plane >/dev/null
fi
log "worker network setup completed in $(elapsed "$phase_started")"

if [ "$build_on_deploy" = "true" ]; then
  phase_started=$(date +%s)
  log "worker build started"
  ORABBIT_ENV_FILE="$env_file" docker compose --env-file "$env_file" -f docker-compose.worker.yml build orabbit-worker
  log "worker build completed in $(elapsed "$phase_started")"
else
  log "worker build skipped (ORABBIT_BUILD_ON_DEPLOY=false)"
fi

phase_started=$(date +%s)
log "worker startup started"
ORABBIT_ENV_FILE="$env_file" docker compose --env-file "$env_file" -f docker-compose.worker.yml up -d --no-build --scale "orabbit-worker=$scale"
log "worker startup completed in $(elapsed "$phase_started")"

phase_started=$(date +%s)
log "worker container status started"
ORABBIT_ENV_FILE="$env_file" docker compose --env-file "$env_file" -f docker-compose.worker.yml ps
log "worker container status completed in $(elapsed "$phase_started")"
log "worker total deployment time: $(elapsed "$started_at")"
