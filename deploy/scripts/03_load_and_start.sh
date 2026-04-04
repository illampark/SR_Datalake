#!/bin/bash
# ============================================================
# 03_load_and_start.sh — 대상 서버에서 실행
# Docker 이미지 로드 → 데이터 디렉토리 생성 → 서비스 시작
# 실행: sudo bash 03_load_and_start.sh
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

echo "======================================"
echo "  SR DataLake — 서비스 설치 및 시작"
echo "======================================"
echo ""

# ── .env 파일 확인 ──
if [ ! -f "$DEPLOY_DIR/.env" ]; then
    echo "오류: $DEPLOY_DIR/.env 파일이 없습니다."
    exit 1
fi

source "$DEPLOY_DIR/.env"
DATA_ROOT="${DATA_ROOT:-/opt/sdl-data}"

# ── 비밀번호 변경 확인 ──
if grep -q '변경필수' "$DEPLOY_DIR/.env"; then
    echo "⚠  경고: .env 파일에 '변경필수' 항목이 남아 있습니다."
    echo "   → $DEPLOY_DIR/.env 를 먼저 수정하세요."
    echo ""
    read -p "   그래도 계속하시겠습니까? (y/N): " confirm
    [ "$confirm" != "y" ] && echo "중단." && exit 1
    echo ""
fi

# ── 1. Docker 이미지 로드 ──
echo "=== [1/3] Docker 이미지 로드 (폐쇄망) ==="
IMAGE_DIR="$DEPLOY_DIR/images"
if [ -d "$IMAGE_DIR" ]; then
    for img in "$IMAGE_DIR"/*.tar.gz; do
        echo "  로드 중: $(basename "$img") ..."
        docker load -i "$img"
    done
    echo "  → 이미지 로드 완료"
    echo ""
    docker images --format "  {{.Repository}}:{{.Tag}}\t{{.Size}}" \
        | grep -E "(sdl-app|postgres|minio|mosquitto)" || true
else
    echo "  images/ 디렉토리 없음 — 이미 로드된 이미지를 사용합니다."
fi
echo ""

# ── 2. 데이터 디렉토리 생성 (엔진과 분리된 스토리지) ──
echo "=== [2/3] 데이터 디렉토리 생성 ==="
echo "  DATA_ROOT = $DATA_ROOT"

mkdir -p "$DATA_ROOT/postgres"      # PostgreSQL 데이터
mkdir -p "$DATA_ROOT/minio"         # MinIO 오브젝트 데이터

# PostgreSQL 디렉토리 권한 (컨테이너 내부 UID 999)
chown -R 999:999 "$DATA_ROOT/postgres" 2>/dev/null || true
# MinIO 디렉토리 권한 (컨테이너 내부 UID 1000)
chown -R 1000:1000 "$DATA_ROOT/minio" 2>/dev/null || true

echo "  → $DATA_ROOT/postgres  (PostgreSQL 데이터)"
echo "  → $DATA_ROOT/minio     (MinIO 오브젝트 파일)"
echo ""

# ── 3. Docker Compose 서비스 시작 ──
echo "=== [3/3] 서비스 시작 ==="
cd "$DEPLOY_DIR"
docker compose up -d

echo ""
echo "서비스 시작 대기 중 (15초) ..."
sleep 15

echo ""
echo "=== 서비스 상태 ==="
docker compose ps
echo ""

# 헬스체크
echo "=== 헬스체크 ==="
check() {
    local name="$1" cmd="$2"
    printf "  %-20s : " "$name"
    if eval "$cmd" &>/dev/null; then
        echo "OK ✓"
    else
        echo "FAIL ✗"
    fi
}

check "PostgreSQL" "docker compose exec -T postgres pg_isready -U sdl_user -d sdl"
check "MinIO" "curl -sf http://localhost:9000/minio/health/live"
check "Mosquitto" "docker compose exec -T mosquitto mosquitto_pub -t test -m ok -h localhost"
check "SDL App" "curl -sf -o /dev/null http://localhost:${SDL_PORT:-5001}/login"

echo ""
echo "======================================"
echo "  설치 완료"
echo "======================================"
echo ""
echo "  접속 URL : http://$(hostname -I 2>/dev/null | awk '{print $1}'):${SDL_PORT:-5001}"
echo "  로그 확인: cd $DEPLOY_DIR && docker compose logs -f sdl-app"
echo ""
echo "  데이터 경로 (엔진과 분리):"
echo "    PostgreSQL : $DATA_ROOT/postgres/"
echo "    MinIO      : $DATA_ROOT/minio/"
echo ""
