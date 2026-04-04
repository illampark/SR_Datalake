#!/bin/bash
# ============================================================
# 06_backup.sh — 데이터 백업 (PostgreSQL + MinIO)
# 실행: sudo bash 06_backup.sh [백업경로]
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

source "$DEPLOY_DIR/.env" 2>/dev/null
DATA_ROOT="${DATA_ROOT:-/opt/sdl-data}"
BACKUP_DIR="${1:-/opt/sdl-backup}"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)

mkdir -p "$BACKUP_DIR"

echo "======================================"
echo "  SR DataLake — 데이터 백업"
echo "  시각: $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================"
echo ""

# ── PostgreSQL 백업 ──
echo "=== [1/2] PostgreSQL 백업 ==="
PG_BACKUP="$BACKUP_DIR/sdl_pg_${TIMESTAMP}.sql.gz"
docker compose -f "$DEPLOY_DIR/docker-compose.yml" exec -T postgres \
    pg_dump -U sdl_user -d sdl | gzip > "$PG_BACKUP"
echo "  → $PG_BACKUP ($(du -h "$PG_BACKUP" | cut -f1))"

# ── MinIO 데이터 백업 ──
echo "=== [2/2] MinIO 데이터 백업 ==="
MINIO_BACKUP="$BACKUP_DIR/sdl_minio_${TIMESTAMP}.tar.gz"
tar czf "$MINIO_BACKUP" -C "$DATA_ROOT" minio/
echo "  → $MINIO_BACKUP ($(du -h "$MINIO_BACKUP" | cut -f1))"

echo ""
echo "=== 백업 완료 ==="
echo "  경로: $BACKUP_DIR"
ls -lh "$BACKUP_DIR"/sdl_*_${TIMESTAMP}*
