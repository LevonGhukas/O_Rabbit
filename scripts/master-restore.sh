#!/usr/bin/env bash
# Restores the master database from its Litestream backup onto this host and
# checks it before a master may start. It automates the safe steps of the
# restore runbook in docs/master-backup-restore.md; deciding that the old
# master is gone stays with the operator.
#
# Usage:
#   scripts/master-restore.sh --from-path <old LITESTREAM_PATH> --old-master-fenced \
#     [--timestamp <RFC3339>] [--max-age <duration>] [--start] [--report <file>]
#
# Guards, each of which stops the restore:
#   - --old-master-fenced must be given: the operator confirms the old master
#     host is stopped and cannot come back. Two masters on two copies of the
#     database would both hand out work.
#   - The master and Litestream services must not be running here.
#   - The database file must not exist yet (restore onto an empty volume).
#   - LITESTREAM_PATH configured for this host must differ from --from-path
#     and must not hold a backup yet. The restored master then replicates to
#     a fresh path, so an old host that comes back by mistake cannot write
#     into the backup this host relies on.
#   - orabbit-master check-db must pass: SQLite integrity, schema version,
#     and ORABBIT_MASTER_KEY decrypting every stored secret.
#
# Environment (defaults match docker-compose.master.yml with the backup
# overlay):
#   COMPOSE_FILE         Compose files, ':'-separated
#   MASTER_SERVICE       master service name (orabbit-master)
#   LITESTREAM_SERVICE   Litestream service name (litestream)
#   MASTER_DB_PATH       database path inside the containers
#   MASTER_HTTP          master URL for the readiness check with --start
set -euo pipefail

cd "$(dirname "$0")/.."

export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.master.yml:docker-compose.master.backup.yml}"
master="${MASTER_SERVICE:-orabbit-master}"
litestream="${LITESTREAM_SERVICE:-litestream}"
db="${MASTER_DB_PATH:-/var/lib/orabbit/master.sqlite}"
master_http="${MASTER_HTTP:-http://127.0.0.1:${ORABBIT_HTTP_PUBLISHED_PORT:-9100}}"
# The master image runs as this user (Dockerfile.orabbit); Litestream runs as
# root, so the restored database and its directory must be handed over.
master_uid=10001

from_path="" fenced=0 timestamp="" max_age="0" start=0 report=""
while [ $# -gt 0 ]; do
	case "$1" in
	--from-path) from_path="$2"; shift 2 ;;
	--old-master-fenced) fenced=1; shift ;;
	--timestamp) timestamp="$2"; shift 2 ;;
	--max-age) max_age="$2"; shift 2 ;;
	--start) start=1; shift ;;
	--report) report="$2"; shift 2 ;;
	-h | --help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

fail() { echo "restore stopped: $*" >&2; exit 1; }
step() { echo "==> $*"; }
now() { date +%s; }

[ -n "$from_path" ] || fail "--from-path is required: the LITESTREAM_PATH the old master replicated to"
[ "$fenced" = 1 ] || fail "confirm with --old-master-fenced that the old master host is stopped and cannot come back"

run_ls() { docker compose run --rm --no-deps -T "$@"; }

step "checking that no master or Litestream is running here"
if [ -n "$(docker compose ps -q --status running "$master" "$litestream" 2>/dev/null)" ]; then
	fail "$master or $litestream is running; stop it first"
fi

step "checking the replica paths"
# shellcheck disable=SC2016 # expanded inside the container, not here
new_path="$(run_ls --entrypoint sh "$litestream" -c 'printf %s "$LITESTREAM_PATH"')"
[ -n "$new_path" ] || fail "LITESTREAM_PATH is not configured for $litestream"
if [ "$new_path" = "$from_path" ]; then
	fail "LITESTREAM_PATH is still $from_path; set a new path (for example ${from_path}-$(date +%Y%m%d)) so this host does not share a backup lineage with the old one"
fi
if run_ls "$litestream" generations -config /etc/litestream.yml "$db" | awk 'NR > 1 && NF' | grep -q .; then
	fail "the new replica path $new_path already holds a backup; choose an unused path"
fi

step "checking that $db does not exist yet"
if ! run_ls --entrypoint sh "$litestream" -c "test ! -e '$db' && test ! -e '$db-wal' && test ! -e '$db-shm'"; then
	fail "$db already exists on this volume; restore onto an empty volume"
fi

t_restore=$(now)
step "restoring from $from_path${timestamp:+ at $timestamp}"
restore_args=(restore -config /etc/litestream.yml -o "$db")
[ -n "$timestamp" ] && restore_args+=(-timestamp "$timestamp")
run_ls -e LITESTREAM_PATH="$from_path" "$litestream" "${restore_args[@]}" "$db"
# A fresh volume's directory is created by the root Litestream container;
# the master needs it to own both, as in its image (0700).
db_dir="$(dirname "$db")"
run_ls --entrypoint sh "$litestream" -c "chown $master_uid:$master_uid '$db_dir' '$db' && chmod 700 '$db_dir'"
restore_seconds=$(($(now) - t_restore))

step "checking the restored database"
check_file="$(mktemp)"
trap 'rm -f "$check_file"' EXIT
if ! run_ls "$master" check-db -db "$db" -max-age "$max_age" >"$check_file"; then
	cat "$check_file"
	fail "check-db found problems; do not start a master on this copy"
fi
cat "$check_file"
[ -z "$report" ] || cp "$check_file" "$report"

if [ "$start" = 1 ]; then
	t_start=$(now)
	# The copy still holds the lost master's leadership lease, and a master
	# refuses to start while it looks held. Wait out what is left of it.
	lease_left="$(sed -n 's/.*"leadership_lease_remaining_seconds": *\([0-9.]*\).*/\1/p' "$check_file")"
	lease_wait="$(awk -v s="${lease_left:-0}" 'BEGIN { w = int(s); if (s > w) w++; print w }')"
	if [ "$lease_wait" -gt 0 ]; then
		step "waiting ${lease_wait}s for the lost master's leadership lease to expire"
		sleep "$lease_wait"
	fi
	step "starting $master and $litestream (replicating to $new_path)"
	docker compose up -d --no-deps "$master" "$litestream"
	until curl -fsS "$master_http/ready" >/dev/null 2>&1; do
		[ $(($(now) - t_start)) -lt 300 ] || fail "master not ready at $master_http after 300s"
		sleep 1
	done
	echo "restore: ${restore_seconds}s, master ready after $(($(now) - t_start))s"
else
	echo "restore: ${restore_seconds}s. Start the master and Litestream when ready:"
	echo "  docker compose up -d $master $litestream"
fi
