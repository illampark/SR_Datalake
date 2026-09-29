#!/usr/bin/env bash
# 신규 aarch64 호스트에 SDL 을 설치한다 (2026-09-29 spark-6783 설치 절차를 고정).
#
#   사용: 대상 서버에서  bash scripts/install-newhost.sh
#
# 전제
#   - 리포가 이미 대상 서버에 있다 (git clone 또는 스테이징에서 tar 반입)
#   - docker 가 설치돼 있고 실행 계정이 docker 그룹에 속한다 (sudo 불필요)
#   - 외부망이 된다. 단 minio/minio · minio/mc 는 Docker Hub 에서 받을 수 없다
#     (결정 기록 0002) — 보유 호스트에서 save/load 로 반입해야 한다.
set -uo pipefail

REPO_DIR="${REPO_DIR:-$HOME/Workspace/sr_datalake}"
DATA_ROOT="${DATA_ROOT:-$HOME/sdl-data}"
SDL_PORT="${SDL_PORT:-5001}"

cd "$REPO_DIR/deploy" || { echo "!! $REPO_DIR/deploy 없음"; exit 1; }
echo "── 대상: $(hostname) · $(uname -m) · 커밋 $(cd "$REPO_DIR" && git rev-parse --short HEAD)"

# ── 1. 데이터 디렉터리 ──
# compose 의 pgdata/minio-data 는 ${DATA_ROOT}/... bind 이고 bind 볼륨은
# 디렉터리를 자동 생성하지 않는다. 기본값 /opt/sdl-data 는 root 소유라 sudo 가
# 필요하므로 홈 아래로 돌린다.
mkdir -p "$DATA_ROOT/postgres" "$DATA_ROOT/minio"
echo "DATA_ROOT = $DATA_ROOT"

# ── 2. MinIO SFTP host key ──
# compose 가 read-only 로 마운트하지만 .gitignore 대상이라 리포에 없다.
# 없으면 docker 가 그 경로에 디렉터리를 만들어 MinIO 가 기동 실패한다.
if [ -f config/sftp_host_key ]; then
  echo "sftp_host_key 존재 — 건너뜀"
else
  ssh-keygen -t ed25519 -f config/sftp_host_key -N "" -C "sdl-minio-sftp@$(hostname)" -q
  chmod 600 config/sftp_host_key
  rm -f config/sftp_host_key.pub
  echo "sftp_host_key 생성"
fi

# ── 3. .env ──
if [ -f .env ]; then
  echo ".env 존재 — 덮어쓰지 않음"
else
  umask 077
  cat > .env <<EOF
# SR DataLake — $(hostname) · 생성 $(date '+%F %T %Z')
# 배포 기준 커밋: $(cd "$REPO_DIR" && git rev-parse --short HEAD)
SECRET_KEY=$(openssl rand -hex 32)
DB_PASSWORD=$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)
MINIO_ROOT_USER=sdladmin
MINIO_ROOT_PASSWORD=$(openssl rand -base64 24 | tr -d '/+=' | head -c 24)
SDL_PORT=${SDL_PORT}
DATA_ROOT=${DATA_ROOT}
EOF
  chmod 600 .env
  echo ".env 생성 (비밀번호 무작위 · 값은 이 파일에만)"
fi

# ── 4. 베이스 이미지 ──
for img in postgres:16-alpine eclipse-mosquitto:2; do
  docker image inspect "$img" >/dev/null 2>&1 && { echo "$img 보유"; continue; }
  docker pull "$img" || { echo "!! pull 실패: $img"; exit 1; }
done
for img in minio/minio:latest minio/mc:latest; do
  docker image inspect "$img" >/dev/null 2>&1 || {
    echo "!! $img 가 없다. Docker Hub 에서 받을 수 없으므로(결정기록 0002)"
    echo "   보유 호스트에서 반입하라:"
    echo "   보유측> docker save minio/minio:latest minio/mc:latest | gzip -1 > /tmp/minio_images.tgz"
    echo "   대상측> gunzip -c /tmp/minio_images.tgz | docker load   # md5 대조 후"
    exit 1
  }
done

# ── 5. 빌드 (아키텍처가 다르면 이미지를 공유할 수 없으므로 자체 빌드) ──
cd "$REPO_DIR" || exit 1
docker build -t sdl-app:latest . || { echo "!! sdl-app 빌드 실패"; exit 1; }
[ -d sim-all ] && { docker build -t sim-all:latest sim-all/ || echo "!! sim-all 빌드 실패"; }
echo "sdl-app arch: $(docker image inspect sdl-app:latest --format '{{.Architecture}}/{{.Os}}')"

# ── 6. 기동 ──
cd "$REPO_DIR/deploy" || exit 1
docker compose up -d || { echo "!! compose up 실패"; exit 1; }
sleep 25

# ── 7. 검증 — "컨테이너가 떴는가" 가 아니라 실제 동작을 본다 ──
echo "── 검증"
docker compose ps --format '{{.Name}} | {{.Status}}'
echo -n "GET /login            -> "; curl -s -o /dev/null -w '%{http_code}\n' "http://localhost:${SDL_PORT##*:}/login"
echo -n "POST 로그인 (기본계정) -> "
curl -s -o /dev/null -w '%{http_code}\n' -H 'Content-Type: application/json' \
  -d '{"username":"admin@sdm-factory.co.kr","password":"admin1234"}' \
  "http://localhost:${SDL_PORT##*:}/api/admin/auth/login"
echo -n "public 테이블 수       -> "
docker exec sdl-postgres psql -U sdl_user -d sdl -Atc \
  "select count(*) from information_schema.tables where table_schema='public';"
echo "MinIO 버킷:"; docker logs sdl-minio-init 2>&1 | grep -c "Bucket created" | sed 's/^/  생성 /'
echo
echo "주의: 신규 설치는 커넥터·파이프라인이 0개다. 데이터 흐름 검증은 별도로 한다."
