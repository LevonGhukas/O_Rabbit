#!/usr/bin/env bash
# Runs govulncheck and fails on any vulnerability that the code actually
# reaches, unless it is listed in .govulncheck-allow.
set -euo pipefail
cd "$(dirname "$0")/.."

allow_file=.govulncheck-allow
out=$(mktemp)
trap 'rm -f "$out"' EXIT

go run golang.org/x/vuln/cmd/govulncheck@latest -format json ./... >"$out"

reachable=$(jq -r 'select(.finding != null and .finding.trace[0].function != null) | .finding.osv' "$out" | sort -u)
allowed=$(grep -Eo '^GO-[0-9]{4}-[0-9]+' "$allow_file" 2>/dev/null | sort -u || true)
unexpected=$(comm -23 <(printf '%s\n' "$reachable" | sed '/^$/d') <(printf '%s\n' "$allowed" | sed '/^$/d'))

if [[ -n "$unexpected" ]]; then
  echo "Reachable vulnerabilities not in $allow_file:" >&2
  while read -r id; do echo "  $id"; done <<<"$unexpected" >&2
  echo "Run: go run golang.org/x/vuln/cmd/govulncheck@latest ./..." >&2
  exit 1
fi
stale=$(comm -13 <(printf '%s\n' "$reachable" | sed '/^$/d') <(printf '%s\n' "$allowed" | sed '/^$/d'))
if [[ -n "$stale" ]]; then
  echo "Allowlisted but no longer reachable (remove from $allow_file):" >&2
  while read -r id; do echo "  $id"; done <<<"$stale" >&2
fi
echo "govulncheck: no unexpected reachable vulnerabilities"
