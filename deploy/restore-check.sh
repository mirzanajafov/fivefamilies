#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

image="${IMAGE:-fivefamilies-node}"
file="${1:-$(ls -1t backups/fivefamilies-*.backup 2>/dev/null | head -n 1)}"
[ -n "$file" ] && [ -f "$file" ] || { echo "no backup to check" >&2; exit 1; }
net="ffrestore-$$"
vol="$net-backup"

cleanup() {
  docker rm -f "$net-1" "$net-2" "$net-3" > /dev/null 2>&1 || true
  docker network rm "$net" > /dev/null 2>&1 || true
  docker volume rm "$vol" > /dev/null 2>&1 || true
}
trap cleanup EXIT

docker volume create "$vol" > /dev/null
docker run -i --rm -v "$vol:/b" --user 0 --entrypoint /bin/sh "$image" -c 'cat > /b/check.backup && chmod 644 /b/check.backup' < "$file"
want=$(docker run --rm -v "$vol:/b:ro" --entrypoint ffctl "$image" backup-check /b/check.backup)
echo "backup:   $want"
docker network create --internal "$net" > /dev/null
cluster="1=$net-1:3161,2=$net-2:3161,3=$net-3:3161"
for i in 1 2 3; do
  docker run -d --name "$net-$i" --network "$net" --network-alias "$net-$i" --tmpfs /data:uid=10001 \
    -v "$vol:/b:ro" --memory 128m --cpus 0.5 "$image" \
    -id "$i" -cluster "$cluster" -data /data -restore /b/check.backup > /dev/null
done
endpoints="$net-1:3161,$net-2:3161,$net-3:3161"
for _ in $(seq 1 60); do
  if got=$(docker run --rm --network "$net" --entrypoint ffctl "$image" -endpoints "$endpoints" -timeout 5s content-hash 2>/dev/null); then
    break
  fi
  sleep 1
done
echo "restored: ${got:-nothing}"
keys=$(echo "$want" | sed -E 's/.*, ([0-9]+) keys,.*/\1/')
content=$(echo "$want" | sed -E 's/.*content ([0-9a-f]+).*/\1/')
if [ "${got:-}" = "$keys keys, content $content" ]; then
  echo "restore check passed"
else
  echo "restore check FAILED" >&2
  exit 1
fi
