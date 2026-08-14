#!/usr/bin/env sh
set -u

MASTER_HTTP="${MASTER_HTTP:-http://master:9100}"
MASTER_GRPC="${MASTER_GRPC:-http://master:9102}"
MINIO_ENDPOINT="${MINIO_ENDPOINT:-http://minio:9000}"
POSTGRES_HOST="${POSTGRES_HOST:-postgres}"
POSTGRES_PORT="${POSTGRES_PORT:-5432}"
AUTH_TOKEN="${ORABBIT_HTTP_AUTH_TOKEN:-}"

failures=0

ok() {
  echo "ok: $*"
}

fail() {
  failures=$((failures + 1))
  echo "fail: $*" >&2
}

have() {
  command -v "$1" >/dev/null 2>&1
}

check_url() {
  label="$1"
  url="$2"
  if have curl; then
    if curl -fsS "$url" >/dev/null; then
      ok "$label $url"
    else
      fail "$label $url"
    fi
    return
  fi
  if have wget; then
    if wget -q -O /dev/null "$url"; then
      ok "$label $url"
    else
      fail "$label $url"
    fi
    return
  fi
  fail "$label $url (curl or wget required)"
}

check_authed_url() {
  label="$1"
  url="$2"
  if [ -z "$AUTH_TOKEN" ]; then
    echo "skip: $label requires ORABBIT_HTTP_AUTH_TOKEN"
    return
  fi
  if ! have curl; then
    echo "skip: $label requires curl"
    return
  fi
  if curl -fsS -H "Authorization: Bearer $AUTH_TOKEN" "$url" >/dev/null; then
    ok "$label $url"
  else
    fail "$label $url"
  fi
}

check_tcp() {
  label="$1"
  host="$2"
  port="$3"
  if have nc; then
    if nc -zvw3 "$host" "$port" >/dev/null 2>&1; then
      ok "$label $host:$port"
    else
      fail "$label $host:$port"
    fi
    return
  fi
  if have telnet; then
    if printf 'quit\n' | telnet "$host" "$port" >/dev/null 2>&1; then
      ok "$label $host:$port"
    else
      fail "$label $host:$port"
    fi
    return
  fi
  fail "$label $host:$port (nc or telnet required)"
}

master_grpc_host="${MASTER_GRPC%:*}"
master_grpc_port="${MASTER_GRPC##*:}"

check_url "master health" "$MASTER_HTTP/healthz"
check_url "master readiness" "$MASTER_HTTP/ready"
check_authed_url "master workers API" "$MASTER_HTTP/workers"
check_tcp "master gRPC TCP" "$master_grpc_host" "$master_grpc_port"
check_url "MinIO health" "$MINIO_ENDPOINT/minio/health/live"
check_tcp "Postgres TCP" "$POSTGRES_HOST" "$POSTGRES_PORT"

if [ -n "${POSTGRES_DSN:-}" ]; then
  if have psql; then
    if psql "$POSTGRES_DSN" -c 'select 1;' >/dev/null; then
      ok "Postgres SQL"
    else
      fail "Postgres SQL"
    fi
  else
    echo "skip: POSTGRES_DSN set but psql is not installed"
  fi
fi

if [ "$failures" -ne 0 ]; then
  exit 1
fi
