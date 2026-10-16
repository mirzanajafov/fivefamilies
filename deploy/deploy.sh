#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

compose=(docker compose -f docker-compose.yml -f docker-compose.prod.yml)
branch="${DEPLOY_BRANCH:-main}"
nodes=(n1 n2 n3 n4 n5)
force=false
[ "${1:-}" = "--force" ] && force=true

say() { printf '\n== %s\n' "$1"; }

cluster_ok() {
  out=$("${compose[@]}" exec -T controller ffctl -endpoints n1:3161,n2:3161,n3:3161,n4:3161,n5:3161 -timeout 5s status 2>/dev/null) || return 1
  [ "$(echo "$out" | grep -c ': node ')" -eq 5 ] || return 1
  leaders=$(echo "$out" | sed -nE 's/.* leader ([0-9]+) .*/\1/p' | sort -u)
  [ "$(echo "$leaders" | wc -l)" -eq 1 ] && [ "$leaders" != 0 ]
}

wait_for() {
  for _ in $(seq 1 60); do
    if "$@"; then return 0; fi
    sleep 2
  done
  return 1
}

web_ok() {
  "${compose[@]}" exec -T web node -e \
    "Promise.all([fetch('http://localhost:3150/'), fetch('http://controller:3159/healthz')]).then((rs) => process.exit(rs.every((r) => r.ok) ? 0 : 1)).catch(() => process.exit(1))" 2>/dev/null
}

release() {
  "${compose[@]}" build
  for n in "${nodes[@]}"; do
    "${compose[@]}" up -d --no-deps "$n"
    wait_for cluster_ok || return 1
  done
  "${compose[@]}" up -d --remove-orphans
  wait_for web_ok
}

previous=$(git rev-parse HEAD)
git fetch --quiet origin "$branch"
target=$(git rev-parse "origin/$branch")
if [ "$previous" = "$target" ] && [ "$force" = false ]; then
  echo "Already at $(git log --oneline -1). Use --force to rebuild anyway."
  exit 0
fi

if "${compose[@]}" ps --status running --services 2>/dev/null | grep -qx backup; then
  say "Backing up before the deploy"
  "${compose[@]}" exec -T backup /bin/sh /backup.sh once
fi

say "Deploying $(git log --oneline -1 "$target"), one node at a time"
git merge --ff-only --quiet "$target"
if release; then
  docker image prune -f --filter "label=com.docker.compose.project=fivefamilies" > /dev/null
  say "Live: $(git log --oneline -1)"
  exit 0
fi

say "Not healthy, rolling back to $(git log --oneline -1 "$previous")"
git reset --hard --quiet "$previous"
if release; then
  echo "Rolled back."
else
  echo "Rollback is not healthy either. Check: ${compose[*]} logs" >&2
fi
exit 1
