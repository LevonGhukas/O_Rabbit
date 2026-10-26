#!/usr/bin/env bash
# Proves that the master's Litestream backup can be restored, without
# touching the running master. Run it from cron on the master host (see
# docs/master-backup-restore.md); it exits non-zero when the backup is not
# usable, so any cron mailer or monitor can alert on it.
#
# Usage: scripts/verify-master-backup.sh [--max-age <duration>] [--report <file>]
#
# It restores the latest backup from the configured LITESTREAM_PATH into a
# temporary Docker volume, then fails when:
#   - orabbit-master check-db finds a problem (integrity, schema, master key);
#   - the restored copy is older than --max-age (default 60s, the published
#     RPO). A running master writes every few seconds, so an old copy means
#     replication has stopped;
#   - more than one backup generation was updated within --max-age, which
#     means two masters are replicating to the same path.
#
# Environment: COMPOSE_FILE, MASTER_SERVICE, LITESTREAM_SERVICE and
# MASTER_DB_PATH as in scripts/master-restore.sh.
set -euo pipefail

cd "$(dirname "$0")/.."

export COMPOSE_FILE="${COMPOSE_FILE:-docker-compose.master.yml:docker-compose.master.backup.yml}"
master="${MASTER_SERVICE:-orabbit-master}"
litestream="${LITESTREAM_SERVICE:-litestream}"
db="${MASTER_DB_PATH:-/var/lib/orabbit/master.sqlite}"
master_uid=10001

max_age="60s" report=""
while [ $# -gt 0 ]; do
	case "$1" in
	--max-age) max_age="$2"; shift 2 ;;
	--report) report="$2"; shift 2 ;;
	-h | --help) sed -n '2,/^set -euo/p' "$0" | sed '$d; s/^# \{0,1\}//'; exit 0 ;;
	*) echo "unknown argument: $1" >&2; exit 2 ;;
	esac
done

run_ls() { docker compose run --rm --no-deps -T "$@"; }

# Seconds in a Go duration such as 1m30s, 450ms or 2h.
to_seconds() {
	awk -v d="$1" 'BEGIN {
		t = 0
		while (match(d, /^[0-9.]+/)) {
			n = substr(d, 1, RLENGTH); d = substr(d, RLENGTH + 1)
			if (d ~ /^ms/) { t += n / 1000; d = substr(d, 3) }
			else if (d ~ /^(us|µs|ns)/) { d = substr(d, 3) }
			else if (d ~ /^h/) { t += n * 3600; d = substr(d, 2) }
			else if (d ~ /^m/) { t += n * 60; d = substr(d, 2) }
			else if (d ~ /^s/) { t += n; d = substr(d, 2) }
			else break
		}
		print t
	}'
}

volume="orabbit-backup-verify-$(date +%s)-$$"
docker volume create "$volume" >/dev/null
trap 'docker volume rm -f "$volume" >/dev/null 2>&1 || true' EXIT

echo "==> backup generations"
generations="$(run_ls "$litestream" generations -config /etc/litestream.yml "$db")"
echo "$generations"
limit="$(to_seconds "$max_age")"
active=0
# Columns: name generation lag start end. Count generations whose last
# update is recent.
while read -r _ _ lag _; do
	[ -n "${lag:-}" ] || continue
	if awk -v a="$(to_seconds "$lag")" -v b="$limit" 'BEGIN { exit !(a <= b) }'; then
		active=$((active + 1))
	fi
done < <(printf '%s\n' "$generations" | awk 'NR > 1 && NF >= 3')
if [ "$active" -gt 1 ]; then
	echo "verify failed: $active backup generations were updated within $max_age; two masters may be replicating to the same path" >&2
	exit 1
fi

echo "==> restoring the latest backup into a temporary volume"
run_ls -v "$volume:/verify" "$litestream" restore -config /etc/litestream.yml -o /verify/master.sqlite "$db"
run_ls -v "$volume:/verify" --entrypoint chown "$litestream" -R "$master_uid:$master_uid" /verify

echo "==> checking the restored copy"
out="$(mktemp)"
trap 'rm -f "$out"; docker volume rm -f "$volume" >/dev/null 2>&1 || true' EXIT
status=0
run_ls -v "$volume:/verify" "$master" check-db -db /verify/master.sqlite -max-age "$max_age" >"$out" || status=$?
cat "$out"
[ -z "$report" ] || cp "$out" "$report"
if [ "$status" -ne 0 ]; then
	echo "verify failed: the restored backup is not usable (see problems above)" >&2
	exit 1
fi
echo "backup verified"
