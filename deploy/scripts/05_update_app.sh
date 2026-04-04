#!/bin/bash
# ============================================================
# 05_update_app.sh — 앱 이미지만 교체 (업데이트 배포)
# 새 sdl-app.tar.gz를 전송한 후 실행
# 실행: sudo bash 05_update_app.sh [이미지파일경로]
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DEPLOY_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
IMAGE_FILE="${1:-$DEPLOY_DIR/images/sdl-app.tar.gz}"

echo "=== SR DataLake 앱 업데이트 ==="

# 1. 새 이미지 로드
if [ -f "$IMAGE_FILE" ]; then
    echo "[1/3] 새 이미지 로드: $IMAGE_FILE"
    docker load -i "$IMAGE_FILE"
else
    echo "오류: 이미지 파일을 찾을 수 없습니다: $IMAGE_FILE"
    echo "사용법: bash 05_update_app.sh /path/to/sdl-app.tar.gz"
    exit 1
fi

# 2. 앱 컨테이너만 재시작
echo "[2/3] 앱 컨테이너 교체 (데이터 서비스 유지)"
cd "$DEPLOY_DIR"
docker compose up -d --no-deps --force-recreate sdl-app

# 3. 상태 확인
echo "[3/3] 서비스 상태 확인"
sleep 5
docker compose ps sdl-app

echo ""
echo "=== 업데이트 완료 ==="
echo "  로그 확인: docker compose logs -f sdl-app"
