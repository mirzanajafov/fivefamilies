#!/bin/sh
set -eu

BACKUPS=/backups
KEEP_DAYS="${KEEP_DAYS:-14}"
RUN_AT="${RUN_AT:-04:00}"

log() {
  printf '{"event":"backup","time":"%s","message":"%s"}\n' "$(date -u +%Y-%m-%dT%H:%M:%SZ)" "$1"
}

run_once() {
  stamp=$(date -u +%Y-%m-%dT%H-%M-%SZ)
  target="$BACKUPS/fivefamilies-$stamp.backup"
  mkdir -p "$BACKUPS"
  if ffctl -endpoints "$ENDPOINTS" -timeout 60s backup "$target" && ffctl backup-check "$target"; then
    date -u +%Y-%m-%dT%H:%M:%SZ > "$BACKUPS/last-success"
    log "kept $(du -h "$target" | cut -f1) $target"
    find "$BACKUPS" -name 'fivefamilies-*.backup' -mtime +"$KEEP_DAYS" -delete
  else
    rm -f "$target" "$target.tmp"
    log "FAILED"
    return 1
  fi
}

seconds_until_next_run() {
  now=$(date -u +%s)
  next=$(date -u -d "$(date -u +%Y-%m-%d) $RUN_AT" +%s)
  [ "$next" -gt "$now" ] || next=$((next + 86400))
  echo $((next - now))
}

if [ "${1:-}" = "once" ]; then
  run_once
  exit
fi

log "scheduled daily at $RUN_AT UTC, keeping $KEEP_DAYS days"
while true; do
  sleep "$(seconds_until_next_run)"
  run_once || true
done
