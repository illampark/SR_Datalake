#!/bin/bash
# ============================================================
# 01_build_and_export.sh — 인터넷 가능 PC에서 실행
# Docker 이미지 빌드 → tar 파일로 내보내기 (폐쇄망 전송용)
# ============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/../.." && pwd)"
DEPLOY_DIR="$SCRIPT_DIR/.."
OUTPUT_DIR="${1:-$HOME/sdl_deploy_package}"

echo "======================================"
echo "  SR DataLake — 배포 패키지 생성"
echo "======================================"
echo ""
echo "  프로젝트: $PROJECT_DIR"
echo "  출력 경로: $OUTPUT_DIR"
echo ""

# ── 1. SDL 앱 이미지 빌드 ──
echo "=== [1/4] SDL 애플리케이션 Docker 이미지 빌드 ==="
cd "$PROJECT_DIR"
docker build -t sdl-app:latest .
echo "  → sdl-app:latest 빌드 완료"

# ── 2. 필요한 외부 이미지 Pull ──
echo ""
echo "=== [2/4] 인프라 이미지 다운로드 ==="
docker pull postgres:16-alpine
docker pull minio/minio:latest
docker pull minio/mc:latest
docker pull eclipse-mosquitto:2
echo "  → 인프라 이미지 다운로드 완료"

# ── 3. 모든 이미지를 tar로 내보내기 ──
echo ""
echo "=== [3/4] Docker 이미지 → tar 파일 내보내기 ==="
mkdir -p "$OUTPUT_DIR/images"

docker save sdl-app:latest         | gzip > "$OUTPUT_DIR/images/sdl-app.tar.gz"
docker save postgres:16-alpine     | gzip > "$OUTPUT_DIR/images/postgres-16-alpine.tar.gz"
docker save minio/minio:latest     | gzip > "$OUTPUT_DIR/images/minio.tar.gz"
docker save minio/mc:latest        | gzip > "$OUTPUT_DIR/images/minio-mc.tar.gz"
docker save eclipse-mosquitto:2    | gzip > "$OUTPUT_DIR/images/mosquitto.tar.gz"

echo "  → 이미지 내보내기 완료"
ls -lh "$OUTPUT_DIR/images/"

# ── 4. 배포 구성 파일 복사 ──
echo ""
echo "=== [4/4] 배포 구성 파일 복사 ==="
cp "$DEPLOY_DIR/docker-compose.yml" "$OUTPUT_DIR/"
cp "$DEPLOY_DIR/.env"               "$OUTPUT_DIR/"
cp -r "$DEPLOY_DIR/config"          "$OUTPUT_DIR/"
cp -r "$DEPLOY_DIR/scripts"         "$OUTPUT_DIR/"

# 서버 설치 스크립트만 복사 (빌드 스크립트 제외)
rm -f "$OUTPUT_DIR/scripts/01_build_and_export.sh"

echo ""
echo "======================================"
echo "  배포 패키지 생성 완료"
echo "======================================"
echo ""
echo "  경로: $OUTPUT_DIR"
du -sh "$OUTPUT_DIR"
echo ""
echo "  다음 단계:"
echo "  1. $OUTPUT_DIR 를 USB 또는 SFTP로 대상 서버에 전송"
echo "  2. 서버에서 bash scripts/02_install_docker.sh 실행 (Docker 미설치 시)"
echo "  3. 서버에서 bash scripts/03_load_and_start.sh 실행"
echo ""
