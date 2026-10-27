#!/usr/bin/env bash
# Runs the end-to-end test: builds the master and worker images from this
# checkout, starts the isolated stack in test/e2e/docker-compose.yml, runs
# go test ./test/e2e against it, and always removes the stack afterwards.
#
# On failure, container logs and the run state are saved to
# ${ORABBIT_E2E_ARTIFACTS:-e2e-artifacts}/ (CI uploads that directory).
#
# Set ORABBIT_E2E_SKIP_BUILD=1 when the orabbit-master:e2e and
# orabbit-worker:e2e images are already built (CI builds them with its cache).
# Set ORABBIT_E2E_DR=1 to add the Litestream backup (docker-compose.dr.yml)
# and run the disaster-recovery drill; its report is written to the
# artifacts directory on success too.
# Set ORABBIT_E2E_KEEP=1 to leave the stack running for debugging; remove it
# later with: docker compose -p orabbit-e2e down -v
set -euo pipefail

cd "$(dirname "$0")/.."

services=(postgres minio ice-rest-catalog master worker)
COMPOSE_FILE=test/e2e/docker-compose.yml
if [ "${ORABBIT_E2E_DR:-0}" = "1" ]; then
	COMPOSE_FILE="$COMPOSE_FILE:test/e2e/docker-compose.dr.yml"
	services+=(litestream)
fi
# Exported so the drill's restore and verify scripts use the same stack.
export COMPOSE_FILE
compose=(docker compose)
artifacts="${ORABBIT_E2E_ARTIFACTS:-e2e-artifacts}"
export ORABBIT_E2E_ARTIFACTS="$artifacts"

# Throwaway secrets for this stack only, never reused.
ORABBIT_HTTP_AUTH_TOKEN="$(openssl rand -hex 32)"
ORABBIT_WORKER_AUTH_TOKEN="$(openssl rand -hex 32)"
ORABBIT_MASTER_KEY="$(openssl rand -base64 32)"
export ORABBIT_HTTP_AUTH_TOKEN ORABBIT_WORKER_AUTH_TOKEN ORABBIT_MASTER_KEY

collect() {
	mkdir -p "$artifacts"
	"${compose[@]}" ps -a >"$artifacts/ps.txt" 2>&1 || true
	for service in $("${compose[@]}" config --services); do
		"${compose[@]}" logs --no-color --timestamps "$service" >"$artifacts/$service.log" 2>&1 || true
	done
	curl -fsS -H "Authorization: Bearer $ORABBIT_HTTP_AUTH_TOKEN" \
		"http://127.0.0.1:19100/api/v1/runs" >"$artifacts/runs.json" 2>&1 || true
	echo "saved stack logs to $artifacts/"
}

cleanup() {
	status=$?
	if [ "$status" -ne 0 ]; then
		collect
	fi
	if [ "${ORABBIT_E2E_KEEP:-0}" = "1" ]; then
		echo "ORABBIT_E2E_KEEP=1: leaving the stack running"
	else
		"${compose[@]}" down -v --remove-orphans >/dev/null 2>&1 || true
	fi
	exit "$status"
}
trap cleanup EXIT

if [ "${ORABBIT_E2E_SKIP_BUILD:-0}" != "1" ]; then
	"${compose[@]}" build
fi
"${compose[@]}" up -d --wait --wait-timeout 180 "${services[@]}"

ORABBIT_E2E=1 go test -count=1 -timeout 20m -v ./test/e2e/
