#!/usr/bin/env sh
set -eu

cd "$(dirname "$0")"

env_file="${ORABBIT_ENV_FILE:-.env.minio}"
if [ ! -f "$env_file" ]; then
  echo "missing $env_file; copy .env.minio.example to $env_file and edit it first" >&2
  exit 2
fi

ORABBIT_ENV_FILE="$env_file" docker compose --env-file "$env_file" -f docker-compose.minio.yml up -d
ORABBIT_ENV_FILE="$env_file" docker compose --env-file "$env_file" -f docker-compose.minio.yml ps
