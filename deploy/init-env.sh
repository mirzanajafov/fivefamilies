#!/usr/bin/env bash
set -euo pipefail
cd "$(dirname "$0")/.."

if [ -e .env ]; then
  echo ".env exists; leaving it alone"
  exit 0
fi
umask 077
{
  echo "FF_CHAOS_TOKEN=$(openssl rand -hex 32)"
  echo "FF_ADMIN_TOKEN=$(openssl rand -hex 32)"
  echo "EDGE_NETWORK=${EDGE_NETWORK:-edge}"
  echo "BACKUP_UID=$(id -u)"
  echo "BACKUP_GID=$(id -g)"
} > .env
chmod 600 .env
mkdir -p backups
echo "wrote .env (mode 600); the admin token is in it and was not printed"
