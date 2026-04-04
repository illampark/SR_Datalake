# SR DataLake 시스템 — 외부 서버 배포 가이드

SFTP 파일 전송 후 서버 현장 설치 방식의 배포 절차서

---

## 개요

이 가이드는 다음 흐름으로 진행됩니다:

```
[로컬 PC]                              [대상 서버]
   │                                       │
   ├─ 1. 배포 패키지 구성                    │
   ├─ 2. 설치 스크립트 준비                   │
   ├─ 3. SFTP로 전체 파일 전송 ──────────────→│
   │                                       ├─ 4. 인프라 설치 (서버 현장 작업)
   │                                       ├─ 5. 애플리케이션 설치
   │                                       ├─ 6. 환경 설정
   │                                       ├─ 7. 서비스 등록 및 실행
   │                                       └─ 8. 검증
```

> 서버에 SSH 원격 접속 없이, **SFTP로 파일을 올린 뒤 서버 현장(또는 콘솔)에서 설치 스크립트를 실행**하는 방식입니다.

---

## 목차

1. [서버 요구사항](#1-서버-요구사항)
2. [배포 패키지 구성 (로컬)](#2-배포-패키지-구성-로컬)
3. [설치 스크립트 준비 (로컬)](#3-설치-스크립트-준비-로컬)
4. [SFTP 전송](#4-sftp-전송)
5. [서버 설치 실행](#5-서버-설치-실행)
6. [검증 및 헬스체크](#6-검증-및-헬스체크)
7. [업데이트 배포](#7-업데이트-배포)
8. [운영 참고사항](#8-운영-참고사항)

---

## 1. 서버 요구사항

### 하드웨어
| 항목 | 최소 | 권장 |
|------|------|------|
| CPU | 2 Core | 4 Core |
| RAM | 4 GB | 8 GB |
| Disk | 50 GB | 100 GB+ (시계열 데이터 축적 고려) |

### 필요 소프트웨어
| 구성요소 | 버전 | 용도 |
|----------|------|------|
| OS | Ubuntu 22.04+ / RHEL 8+ | 서버 OS |
| Python | 3.11+ | 애플리케이션 런타임 |
| PostgreSQL | 14+ | 메인 데이터베이스 |
| MinIO | 최신 | 파일 오브젝트 스토리지 (S3 호환) |
| Mosquitto | 2.0+ | MQTT 브로커 |
| Redpanda Connect | 최신 | 데이터 스트림 처리 |

### 사용 포트
| 포트 | 서비스 | 외부 개방 |
|------|--------|-----------|
| 5001 | SDL 애플리케이션 | O (필수) |
| 5432 | PostgreSQL | X (로컬만) |
| 9000 | MinIO API | X (로컬만) |
| 9001 | MinIO Console | 선택 |
| 1883 | MQTT (Mosquitto) | 선택 (외부 장비 연결 시) |
| 4195 | Redpanda Connect API | X (로컬만) |

---

## 2. 배포 패키지 구성 (로컬)

### 2-1. 디렉토리 구조

로컬에서 아래 구조의 배포 디렉토리를 만듭니다.

```
sdl_deploy/
├── app/                         ← 애플리케이션 소스
│   ├── app.py
│   ├── requirements.txt
│   ├── backend/
│   ├── templates/
│   └── static/
├── infra/                       ← 인프라 바이너리 (인터넷 없는 서버용)
│   ├── minio                    ← MinIO 서버 바이너리
│   ├── mc                       ← MinIO Client 바이너리
│   └── redpanda-connect         ← Redpanda Connect 바이너리
├── config/                      ← 설정 파일 템플릿
│   ├── .env.template
│   ├── minio.conf
│   ├── mosquitto-sdl.conf
│   ├── sdl-app.service
│   └── minio.service
├── scripts/                     ← 설치 스크립트
│   ├── 01_install_infra.sh
│   ├── 02_install_app.sh
│   ├── 03_register_services.sh
│   └── 99_healthcheck.sh
└── README.txt                   ← 간단 설치 안내
```

### 2-2. requirements.txt 정리

```text
# requirements.txt (운영 서버용)
flask==3.1.0
flask-cors==4.0.0
sqlalchemy==2.0.41
psycopg2-binary==2.9.10
pymysql==1.1.0
minio==7.2.15
paho-mqtt==2.1.0
requests==2.32.4
bcrypt==4.3.0
beautifulsoup4==4.14.3
openpyxl==3.1.5
pandas==2.2.1
gunicorn==23.0.0
python-dotenv==1.1.1
lxml==6.0.2
```

### 2-3. 배포 패키지 생성 스크립트

로컬에서 실행합니다:

```bash
#!/bin/bash
# build_deploy_package.sh — 배포 패키지 생성

SRC_DIR="/Users/mac/Work/sr_datalake"
DEPLOY_DIR="/Users/mac/Work/sdl_deploy"

echo "=== SDL 배포 패키지 생성 ==="

# 1) 디렉토리 구성
rm -rf "$DEPLOY_DIR"
mkdir -p "$DEPLOY_DIR"/{app,infra,config,scripts}

# 2) 애플리케이션 소스 복사
rsync -a \
  --exclude='venv' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='minio_data' \
  --exclude='.git' \
  --exclude='docs' \
  "$SRC_DIR/" "$DEPLOY_DIR/app/"

# 3) 인프라 바이너리 다운로드 (Linux amd64 대상)
echo "MinIO 바이너리 다운로드..."
curl -so "$DEPLOY_DIR/infra/minio" \
  https://dl.min.io/server/minio/release/linux-amd64/minio

curl -so "$DEPLOY_DIR/infra/mc" \
  https://dl.min.io/client/mc/release/linux-amd64/mc

echo "Redpanda Connect 다운로드..."
curl -sL https://github.com/redpanda-data/connect/releases/latest/download/redpanda-connect_linux_amd64.tar.gz \
  | tar xz -C "$DEPLOY_DIR/infra/" redpanda-connect

chmod +x "$DEPLOY_DIR/infra/"*

# 4) 설정 파일 템플릿 복사 (아래 3절에서 생성한 파일들)
# → config/ 디렉토리에 이미 준비되어 있어야 함

# 5) 압축
cd /Users/mac/Work
tar czf sdl_deploy.tar.gz sdl_deploy/
echo "=== 완료: sdl_deploy.tar.gz ($(du -h sdl_deploy.tar.gz | cut -f1)) ==="
```

---

## 3. 설치 스크립트 준비 (로컬)

아래 파일들을 `sdl_deploy/config/`, `sdl_deploy/scripts/`에 미리 만들어 둡니다.

### 3-1. config/.env.template

```bash
# ===== SR DataLake 환경 설정 =====
# 설치 시 실제 값으로 수정하세요.

# Flask
SECRET_KEY=변경필수-강력한-시크릿키-입력

# PostgreSQL
DATABASE_URL=postgresql+psycopg2://sdl_user:변경필수-DB비밀번호@localhost:5432/sdl

# MinIO
MINIO_ENDPOINT=localhost:9000
MINIO_ACCESS_KEY=sdladmin
MINIO_SECRET_KEY=변경필수-MinIO비밀번호
MINIO_SECURE=false

# Benthos (Redpanda Connect)
BENTHOS_BIN=/usr/local/bin/redpanda-connect
BENTHOS_API=http://localhost:4195

# MQTT
MQTT_DEFAULT_HOST=localhost
MQTT_DEFAULT_PORT=1883
```

### 3-2. config/minio.conf

```bash
MINIO_ROOT_USER=sdladmin
MINIO_ROOT_PASSWORD=변경필수-MinIO비밀번호
MINIO_VOLUMES="/opt/minio/data"
MINIO_OPTS="--address :9000 --console-address :9001"
```

### 3-3. config/mosquitto-sdl.conf

```
listener 1883
allow_anonymous true
```

### 3-4. config/minio.service

```ini
[Unit]
Description=MinIO Object Storage
After=network-online.target
Wants=network-online.target

[Service]
User=minio-user
Group=minio-user
EnvironmentFile=/etc/default/minio
ExecStart=/usr/local/bin/minio server $MINIO_VOLUMES $MINIO_OPTS
Restart=always
RestartSec=5
LimitNOFILE=65536

[Install]
WantedBy=multi-user.target
```

### 3-5. config/sdl-app.service

```ini
[Unit]
Description=SR DataLake Application
After=network.target postgresql.service minio.service mosquitto.service
Wants=postgresql.service minio.service mosquitto.service

[Service]
User=sdl
Group=sdl
WorkingDirectory=/opt/sdl/app
EnvironmentFile=/opt/sdl/app/.env
ExecStart=/opt/sdl/app/venv/bin/gunicorn \
  -w 4 \
  -b 0.0.0.0:5001 \
  --timeout 120 \
  --access-logfile /var/log/sdl/access.log \
  --error-logfile /var/log/sdl/error.log \
  app:app
Restart=always
RestartSec=5

[Install]
WantedBy=multi-user.target
```

### 3-6. scripts/01_install_infra.sh

```bash
#!/bin/bash
# ============================================================
# 01_install_infra.sh — 인프라 구성요소 설치
# 실행: sudo bash 01_install_infra.sh
# ============================================================
set -e

DEPLOY_BASE="$(cd "$(dirname "$0")/.." && pwd)"
echo "=== [1/4] 시스템 패키지 설치 ==="
apt update
apt install -y python3 python3-pip python3-venv \
               libpq-dev gcc python3-dev \
               libmagic1 curl

echo "=== [2/4] PostgreSQL 설치 ==="
apt install -y postgresql postgresql-contrib
systemctl enable --now postgresql

# DB 사용자 및 데이터베이스 생성
sudo -u postgres psql -tc "SELECT 1 FROM pg_roles WHERE rolname='sdl_user'" \
  | grep -q 1 || sudo -u postgres psql -c "CREATE USER sdl_user WITH PASSWORD 'sdl_password_2025';"
sudo -u postgres psql -tc "SELECT 1 FROM pg_database WHERE datname='sdl'" \
  | grep -q 1 || sudo -u postgres psql -c "CREATE DATABASE sdl OWNER sdl_user;"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE sdl TO sdl_user;"
echo "  → PostgreSQL 완료 (sdl / sdl_user)"
echo "  ⚠ 운영 환경에서는 비밀번호를 반드시 변경하세요"

echo "=== [3/4] MinIO 설치 ==="
cp "$DEPLOY_BASE/infra/minio" /usr/local/bin/minio
cp "$DEPLOY_BASE/infra/mc"    /usr/local/bin/mc
chmod +x /usr/local/bin/minio /usr/local/bin/mc

mkdir -p /opt/minio/data
id minio-user &>/dev/null || useradd -r -s /sbin/nologin minio-user
chown minio-user:minio-user /opt/minio/data

cp "$DEPLOY_BASE/config/minio.conf" /etc/default/minio
cp "$DEPLOY_BASE/config/minio.service" /etc/systemd/system/minio.service
systemctl daemon-reload
systemctl enable --now minio

# 버킷 생성 (MinIO 시작 대기)
echo "  MinIO 시작 대기 중..."
sleep 3
MINIO_PW=$(grep MINIO_ROOT_PASSWORD /etc/default/minio | cut -d= -f2)
mc alias set local http://localhost:9000 sdladmin "$MINIO_PW" 2>/dev/null
mc mb --ignore-existing local/sdl-files
mc mb --ignore-existing local/sdl-archive
mc mb --ignore-existing local/sdl-backup
echo "  → MinIO 완료 (버킷 3개 생성)"

echo "=== [4/4] Mosquitto 설치 ==="
apt install -y mosquitto mosquitto-clients
cp "$DEPLOY_BASE/config/mosquitto-sdl.conf" /etc/mosquitto/conf.d/sdl.conf
systemctl enable --now mosquitto
systemctl restart mosquitto
echo "  → Mosquitto 완료 (포트 1883)"

echo ""
echo "=== 인프라 설치 완료 ==="
```

### 3-7. scripts/02_install_app.sh

```bash
#!/bin/bash
# ============================================================
# 02_install_app.sh — 애플리케이션 설치
# 실행: sudo bash 02_install_app.sh
# ============================================================
set -e

DEPLOY_BASE="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="/opt/sdl/app"

echo "=== [1/4] 설치 디렉토리 구성 ==="
# 서비스 실행 계정 생성
id sdl &>/dev/null || useradd -r -m -s /bin/bash sdl

mkdir -p "$INSTALL_DIR"
cp -r "$DEPLOY_BASE/app/"* "$INSTALL_DIR/"
chown -R sdl:sdl /opt/sdl

echo "=== [2/4] Redpanda Connect 설치 ==="
cp "$DEPLOY_BASE/infra/redpanda-connect" /usr/local/bin/redpanda-connect
chmod +x /usr/local/bin/redpanda-connect
echo "  → $(redpanda-connect --version 2>&1 | head -1)"

echo "=== [3/4] Python 가상환경 및 의존성 설치 ==="
sudo -u sdl bash -c "
  cd $INSTALL_DIR
  python3 -m venv venv
  source venv/bin/activate
  pip install --upgrade pip -q
  pip install -r requirements.txt -q
"
echo "  → Python 패키지 설치 완료"

echo "=== [4/4] 환경 설정 ==="
if [ ! -f "$INSTALL_DIR/.env" ]; then
  cp "$DEPLOY_BASE/config/.env.template" "$INSTALL_DIR/.env"
  chown sdl:sdl "$INSTALL_DIR/.env"
  chmod 600 "$INSTALL_DIR/.env"
  echo "  → .env 파일 생성됨 (템플릿)"
  echo "  ⚠ 반드시 $INSTALL_DIR/.env 파일을 열어 비밀번호를 수정하세요!"
else
  echo "  → .env 파일이 이미 존재합니다 (덮어쓰지 않음)"
fi

# 로그 디렉토리
mkdir -p /var/log/sdl
chown sdl:sdl /var/log/sdl

echo ""
echo "=== 애플리케이션 설치 완료 ==="
echo ""
echo "다음 단계:"
echo "  1. vi $INSTALL_DIR/.env   ← 비밀번호 및 설정값 수정"
echo "  2. bash scripts/03_register_services.sh  ← 서비스 등록 및 시작"
```

### 3-8. scripts/03_register_services.sh

```bash
#!/bin/bash
# ============================================================
# 03_register_services.sh — systemd 서비스 등록 및 시작
# 실행: sudo bash 03_register_services.sh
# ============================================================
set -e

DEPLOY_BASE="$(cd "$(dirname "$0")/.." && pwd)"
INSTALL_DIR="/opt/sdl/app"

echo "=== 서비스 등록 ==="

# .env 설정 확인
if grep -q '변경필수' "$INSTALL_DIR/.env" 2>/dev/null; then
  echo "⚠ 경고: .env에 '변경필수' 항목이 남아 있습니다."
  echo "  → $INSTALL_DIR/.env 를 먼저 수정하세요."
  read -p "  그래도 계속하시겠습니까? (y/N): " confirm
  [ "$confirm" != "y" ] && echo "중단." && exit 1
fi

# systemd 서비스 파일 복사
cp "$DEPLOY_BASE/config/sdl-app.service" /etc/systemd/system/sdl-app.service
systemctl daemon-reload

# 방화벽 설정
if command -v ufw &>/dev/null; then
  ufw allow 22/tcp   2>/dev/null
  ufw allow 5001/tcp 2>/dev/null
  echo "  → UFW 방화벽: 22, 5001 포트 개방"
fi

# 서비스 시작
systemctl enable sdl-app
systemctl start sdl-app

echo ""
echo "=== 서비스 시작 완료 ==="
echo ""
systemctl status sdl-app --no-pager -l
echo ""
echo "로그 확인: journalctl -u sdl-app -f"
echo "접속 확인: http://$(hostname -I | awk '{print $1}'):5001"
```

### 3-9. scripts/99_healthcheck.sh

```bash
#!/bin/bash
# ============================================================
# 99_healthcheck.sh — 전체 시스템 상태 점검
# 실행: bash 99_healthcheck.sh
# ============================================================

echo "======================================"
echo "  SR DataLake 시스템 상태 점검"
echo "  $(date '+%Y-%m-%d %H:%M:%S')"
echo "======================================"
echo ""

check() {
  local name="$1" cmd="$2"
  printf "  %-20s : " "$name"
  if eval "$cmd" &>/dev/null; then
    echo "OK ✓"
  else
    echo "FAIL ✗"
  fi
}

echo "[인프라]"
check "PostgreSQL" "pg_isready -h localhost -p 5432"
check "MinIO" "curl -sf http://localhost:9000/minio/health/live"
check "Mosquitto" "systemctl is-active --quiet mosquitto"
echo ""

echo "[애플리케이션]"
check "SDL App (systemd)" "systemctl is-active --quiet sdl-app"
check "SDL App (HTTP)" "curl -sf -o /dev/null http://localhost:5001/login"
check "Redpanda Connect" "curl -sf -o /dev/null http://localhost:4195/ready"
echo ""

echo "[디스크]"
df -h /opt/sdl /opt/minio/data 2>/dev/null | tail -n +2 | \
  awk '{printf "  %-20s : %s used / %s total (%s)\n", $6, $3, $2, $5}'
echo ""

echo "[서비스 포트]"
ss -tlnp 2>/dev/null | grep -E ':(5001|5432|9000|1883|4195) ' | \
  awk '{printf "  %-20s : %s\n", $4, $7}'
echo ""

echo "======================================"
echo "  점검 완료"
echo "======================================"
```

### 3-10. README.txt

```text
============================================================
  SR DataLake 시스템 — 설치 안내
============================================================

[설치 순서]

  1. 이 디렉토리 전체를 서버의 임의 경로에 복사합니다.
     예: /home/user/sdl_deploy/

  2. 터미널에서 scripts/ 디렉토리로 이동합니다.
     cd /home/user/sdl_deploy/scripts/

  3. 인프라 설치 (PostgreSQL, MinIO, Mosquitto)
     sudo bash 01_install_infra.sh

  4. 애플리케이션 설치
     sudo bash 02_install_app.sh

  5. 환경 설정 수정 (필수)
     vi /opt/sdl/app/.env
     → SECRET_KEY, DATABASE_URL 비밀번호, MINIO_SECRET_KEY 변경

  6. 서비스 등록 및 시작
     sudo bash 03_register_services.sh

  7. 상태 점검
     bash 99_healthcheck.sh

  8. 브라우저 접속
     http://서버IP:5001


[설치 경로]

  애플리케이션  : /opt/sdl/app/
  MinIO 데이터  : /opt/minio/data/
  로그          : /var/log/sdl/
  환경 설정     : /opt/sdl/app/.env


[주의사항]

  - .env 파일의 비밀번호를 반드시 변경하세요
  - PostgreSQL 비밀번호도 변경 시 .env의 DATABASE_URL과 일치시키세요
  - MinIO 비밀번호 변경 시 /etc/default/minio 와 .env 양쪽 수정 필요
============================================================
```

---

## 4. SFTP 전송

### 4-1. 배포 패키지 압축 (로컬)

```bash
cd /Users/mac/Work
tar czf sdl_deploy.tar.gz sdl_deploy/
ls -lh sdl_deploy.tar.gz
```

### 4-2. SFTP 업로드

```bash
sftp user@서버IP
```

```
sftp> put sdl_deploy.tar.gz
sftp> exit
```

또는 GUI SFTP 클라이언트(FileZilla, Cyberduck 등)로 `sdl_deploy.tar.gz`를 서버의 `/home/user/`에 업로드합니다.

### 4-3. (참고) 인터넷 가능 서버의 경우

서버에 인터넷이 있다면 `infra/` 디렉토리의 바이너리를 제외하고 패키지 크기를 줄일 수 있습니다. 이 경우 `01_install_infra.sh`에서 바이너리를 직접 다운로드하도록 수정합니다.

---

## 5. 서버 설치 실행

서버 콘솔(또는 터미널)에서 직접 수행합니다.

### 5-1. 압축 해제

```bash
cd ~
tar xzf sdl_deploy.tar.gz
cd sdl_deploy
```

### 5-2. 설치 실행 (3단계)

```bash
# Step 1: 인프라 (PostgreSQL, MinIO, Mosquitto)
sudo bash scripts/01_install_infra.sh

# Step 2: 애플리케이션 (소스 복사, Python venv, 의존성)
sudo bash scripts/02_install_app.sh

# Step 3: .env 수정 (필수!)
sudo vi /opt/sdl/app/.env
#   → SECRET_KEY, DATABASE_URL 비밀번호, MINIO_SECRET_KEY 수정

# Step 4: 서비스 등록 및 시작
sudo bash scripts/03_register_services.sh
```

### 5-3. 설치 확인

```bash
bash scripts/99_healthcheck.sh
```

기대 출력:
```
======================================
  SR DataLake 시스템 상태 점검
======================================

[인프라]
  PostgreSQL           : OK ✓
  MinIO                : OK ✓
  Mosquitto            : OK ✓

[애플리케이션]
  SDL App (systemd)    : OK ✓
  SDL App (HTTP)       : OK ✓
  Redpanda Connect     : FAIL ✗    ← 정상 (커넥터 생성 시 자동 시작)
```

---

## 6. 검증 및 헬스체크

### 웹 접속 확인

1. 브라우저에서 `http://서버IP:5001` 접속
2. 로그인 페이지 표시 확인
3. 기본 계정으로 로그인 (seed_default_users에 의해 자동 생성)
4. 대시보드 정상 로딩 확인

### 로그 확인 (문제 발생 시)

```bash
# 애플리케이션 로그
sudo journalctl -u sdl-app -f

# Gunicorn 로그
tail -f /var/log/sdl/error.log

# PostgreSQL 로그
sudo journalctl -u postgresql -f

# MinIO 로그
sudo journalctl -u minio -f
```

---

## 7. 업데이트 배포

소스 코드 변경 시 반복 배포 절차입니다.

### 7-1. 로컬에서 업데이트 패키지 생성

```bash
# 앱 소스만 압축 (인프라 바이너리 제외)
cd /Users/mac/Work
tar czf sdl_update.tar.gz \
  --exclude='venv' \
  --exclude='__pycache__' \
  --exclude='*.pyc' \
  --exclude='minio_data' \
  --exclude='.git' \
  sr_datalake/
```

### 7-2. SFTP 전송

```bash
sftp user@서버IP
```
```
sftp> put sdl_update.tar.gz
sftp> exit
```

### 7-3. 서버에서 업데이트 적용

```bash
cd ~
tar xzf sdl_update.tar.gz

# .env 보존하면서 소스만 덮어쓰기
sudo rsync -a --exclude='.env' --exclude='venv' \
  sr_datalake/ /opt/sdl/app/

sudo chown -R sdl:sdl /opt/sdl/app
sudo systemctl restart sdl-app
sudo systemctl status sdl-app
```

> `.env`와 `venv`는 제외하여 서버 환경 설정과 가상환경이 유지됩니다.

> **자동 마이그레이션**: 앱 시작 시 `init_db()`가 자동으로 테이블 스키마 업데이트 및 데이터 마이그레이션을 수행합니다. 기존 `sdm` 접두사 데이터가 남아 있으면 `sdl`로 자동 변환되므로, 별도의 DB 마이그레이션 작업은 필요 없습니다.

---

## 8. 운영 참고사항

### 서비스 관리 명령

```bash
sudo systemctl status sdl-app     # 상태 확인
sudo systemctl restart sdl-app    # 재시작
sudo systemctl stop sdl-app       # 중지
sudo journalctl -u sdl-app -f     # 실시간 로그
```

### 데이터베이스 백업/복원

```bash
# 백업
sudo -u sdl pg_dump -U sdl_user -h localhost sdl \
  > /opt/sdl/backup_$(date +%Y%m%d).sql

# 복원
psql -U sdl_user -h localhost sdl < backup_YYYYMMDD.sql
```

### 로그 로테이션

```bash
sudo tee /etc/logrotate.d/sdl <<'EOF'
/var/log/sdl/*.log {
    daily
    rotate 14
    compress
    delaycompress
    missingok
    notifempty
    copytruncate
}
EOF
```

### 서비스 시작 순서 (부팅 시 자동)

```
PostgreSQL → MinIO → Mosquitto → SDL App
                                  ├── init_db() (테이블 자동 생성/마이그레이션)
                                  ├── seed_default_users()
                                  ├── retention_scheduler (백그라운드)
                                  ├── mqtt_manager.connect()
                                  ├── alarm_engine.start()
                                  └── backup_scheduler (백그라운드)
```

### MinIO 설정 변경 (UI)

설치 후 MinIO 접속 정보를 변경해야 할 경우, `.env` 파일 수정 없이 **관리자 UI**에서 변경할 수 있습니다:

1. 브라우저에서 `http://서버IP:5001` 접속 → 관리자 로그인
2. **시스템 관리 > 시스템 설정** 메뉴 이동
3. MinIO 관련 설정값 수정 (`minio.endpoint`, `minio.access_key`, `minio.secret_key`, `minio.secure`)
4. 저장 즉시 반영 (앱 재시작 불필요)

> UI에서 설정한 값은 `.env` 값보다 우선 적용됩니다. UI 설정을 삭제하면 `.env` 기본값으로 폴백합니다.

### (선택) Nginx 리버스 프록시

80 포트로 접속하려면 Nginx를 추가합니다:

```bash
sudo apt install -y nginx

sudo tee /etc/nginx/sites-available/sdl <<'EOF'
server {
    listen 80;
    server_name _;
    client_max_body_size 500M;

    location / {
        proxy_pass http://127.0.0.1:5001;
        proxy_set_header Host $host;
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header X-Forwarded-Proto $scheme;
        proxy_read_timeout 120s;
    }
}
EOF

sudo ln -s /etc/nginx/sites-available/sdl /etc/nginx/sites-enabled/
sudo rm -f /etc/nginx/sites-enabled/default
sudo nginx -t && sudo systemctl enable --now nginx
```

### 주의사항

- `.env`의 비밀번호 항목(`SECRET_KEY`, DB 비밀번호, MinIO 비밀번호)은 반드시 변경
- MinIO 비밀번호 변경 시 `/etc/default/minio`와 `.env` **양쪽 모두** 수정 (또는 설치 후 **관리자 UI > 시스템 설정**에서 MinIO 접속 정보 변경 가능 — UI 설정이 `.env`보다 우선 적용됨)
- PostgreSQL 비밀번호 변경 시 DB에서 ALTER USER 실행 후 `.env`의 DATABASE_URL도 수정
- `app.py`의 `debug=True`는 Gunicorn 실행 시 무시됨 (정상)
- 대용량 파일 업로드(500MB)를 위해 Nginx 사용 시 `client_max_body_size` 확인
- Redpanda Connect는 커넥터 생성 시 Flask 앱에서 자동 시작하므로 별도 서비스 등록 불필요
