# SR DataLake — 폐쇄망 컨테이너 배포 가이드

Docker 기반 배포. PostgreSQL/MinIO는 엔진(컨테이너)과 데이터(호스트 볼륨)를 분리하여 구성합니다.

---

## 아키텍처

```
┌─────────────────────────────────────────────────────────┐
│  대상 서버 (Ubuntu 22.04+ / RHEL 8+)                     │
│                                                         │
│  ┌─── Docker Network (sdl-net) ──────────────────────┐  │
│  │                                                   │  │
│  │  ┌──────────┐  ┌──────────┐  ┌──────────────────┐│  │
│  │  │PostgreSQL│  │  MinIO   │  │    Mosquitto     ││  │
│  │  │  :5432   │  │:9000/:901│  │      :1883       ││  │
│  │  └────┬─────┘  └────┬─────┘  └──────────────────┘│  │
│  │       │              │                             │  │
│  │  ┌────┴──────────────┴─────────────────────────┐  │  │
│  │  │         SDL App (gunicorn :5001)             │  │  │
│  │  │   Flask + SQLAlchemy + Background Services   │  │  │
│  │  └─────────────────────────────────────────────┘  │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  ┌─── 호스트 파일시스템 (데이터 스토리지) ──────────────┐  │
│  │  /opt/sdl-data/                                    │  │
│  │  ├── postgres/   ← PostgreSQL 데이터 파일          │  │
│  │  └── minio/      ← MinIO 오브젝트 파일             │  │
│  └────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 엔진/데이터 분리 구조

| 서비스 | 엔진 (컨테이너) | 데이터 (호스트 볼륨) |
|--------|----------------|---------------------|
| PostgreSQL | `postgres:16-alpine` 컨테이너 | `/opt/sdl-data/postgres/` |
| MinIO | `minio/minio:latest` 컨테이너 | `/opt/sdl-data/minio/` |

컨테이너를 삭제/재생성해도 데이터는 호스트에 유지됩니다. 엔진 업그레이드 시 이미지만 교체하면 됩니다.

---

## 서버 요구사항

| 항목 | 최소 | 권장 |
|------|------|------|
| CPU | 2 Core | 4 Core |
| RAM | 4 GB | 8 GB |
| Disk | 50 GB | 100 GB+ |
| OS | Ubuntu 22.04+ / RHEL 8+ | Ubuntu 24.04 LTS |
| Docker | 24.0+ | 최신 |
| Docker Compose | v2 (플러그인) | 최신 |

### 사용 포트

| 포트 | 서비스 | 외부 노출 |
|------|--------|-----------|
| 5001 | SDL 애플리케이션 | O (필수) |
| 5432 | PostgreSQL | X (컨테이너 내부) |
| 9000 | MinIO API | X (컨테이너 내부) |
| 9001 | MinIO Console | X (컨테이너 내부) |
| 1883 | MQTT (Mosquitto) | 선택 (외부 장비 연결 시) |

---

## 배포 절차 (전체 흐름)

```
[인터넷 가능 PC]                         [폐쇄망 서버]
     │                                       │
     ├─ 1. Docker 이미지 빌드 & 내보내기       │
     │     (01_build_and_export.sh)           │
     │                                       │
     ├─ 2. USB/SFTP로 전송 ──────────────────→│
     │                                       │
     │                                       ├─ 3. Docker 설치 (최초 1회)
     │                                       │     (02_install_docker.sh)
     │                                       │
     │                                       ├─ 4. .env 비밀번호 수정
     │                                       │
     │                                       ├─ 5. 이미지 로드 & 서비스 시작
     │                                       │     (03_load_and_start.sh)
     │                                       │
     │                                       └─ 6. 브라우저 접속 확인
```

---

## Step 1. 빌드 및 내보내기 (인터넷 가능 PC)

### 사전 조건
- Docker Desktop 또는 Docker Engine이 설치된 PC
- 인터넷 연결

### 실행

```bash
cd sr_datalake/deploy/scripts
bash 01_build_and_export.sh
```

생성되는 배포 패키지 (`~/sdl_deploy_package/`):

```
sdl_deploy_package/
├── images/                      ← Docker 이미지 tar.gz
│   ├── sdl-app.tar.gz           (~200MB)
│   ├── postgres-16-alpine.tar.gz (~90MB)
│   ├── minio.tar.gz             (~150MB)
│   ├── minio-mc.tar.gz          (~30MB)
│   └── mosquitto.tar.gz         (~10MB)
├── config/                      ← 설정 파일
│   ├── pg-init.sql
│   └── mosquitto.conf
├── scripts/                     ← 서버 실행 스크립트
│   ├── 02_install_docker.sh
│   ├── 03_load_and_start.sh
│   ├── 04_stop.sh
│   ├── 05_update_app.sh
│   └── 06_backup.sh
├── docker-compose.yml
└── .env                         ← 환경 설정 (비밀번호 변경 필수)
```

### 전송

```bash
# USB 복사
cp -r ~/sdl_deploy_package /media/usb/

# 또는 SFTP
sftp user@서버IP
sftp> put -r sdl_deploy_package
sftp> exit
```

---

## Step 2. Docker 설치 (대상 서버, 최초 1회)

### 인터넷 가능한 경우

```bash
cd sdl_deploy_package/scripts
sudo bash 02_install_docker.sh
```

### 폐쇄망인 경우 (Docker 패키지 사전 준비)

인터넷 가능 PC에서 Docker 패키지를 미리 다운로드합니다:

```bash
# Ubuntu 22.04 예시 — 인터넷 가능 PC에서 실행
mkdir docker-debs && cd docker-debs
apt-get download docker-ce docker-ce-cli containerd.io \
    docker-buildx-plugin docker-compose-plugin
# 의존 패키지도 포함
apt-cache depends docker-ce docker-ce-cli containerd.io | \
    grep Depends | awk '{print $2}' | xargs apt-get download 2>/dev/null
```

서버에서 설치:
```bash
cd docker-debs
sudo dpkg -i *.deb
sudo systemctl enable --now docker
```

---

## Step 3. 환경 설정 수정

```bash
cd sdl_deploy_package
vi .env
```

**반드시 변경해야 할 항목:**

```bash
SECRET_KEY=여기에-강력한-랜덤-문자열-입력
DB_PASSWORD=PostgreSQL-비밀번호
MINIO_ROOT_PASSWORD=MinIO-비밀번호
```

선택 항목:
```bash
SDL_PORT=5001              # 앱 포트 (기본값 5001)
DATA_ROOT=/opt/sdl-data    # 데이터 저장 경로
```

---

## Step 4. 이미지 로드 및 서비스 시작

```bash
cd sdl_deploy_package/scripts
sudo bash 03_load_and_start.sh
```

실행 결과:
```
=== [1/3] Docker 이미지 로드 (폐쇄망) ===
  로드 중: sdl-app.tar.gz ...
  로드 중: postgres-16-alpine.tar.gz ...
  로드 중: minio.tar.gz ...
  로드 중: minio-mc.tar.gz ...
  로드 중: mosquitto.tar.gz ...
  → 이미지 로드 완료

=== [2/3] 데이터 디렉토리 생성 ===
  → /opt/sdl-data/postgres  (PostgreSQL 데이터)
  → /opt/sdl-data/minio     (MinIO 오브젝트 파일)

=== [3/3] 서비스 시작 ===

=== 헬스체크 ===
  PostgreSQL           : OK ✓
  MinIO                : OK ✓
  Mosquitto            : OK ✓
  SDL App              : OK ✓

  접속 URL : http://192.168.1.100:5001
```

---

## Step 5. 접속 확인

1. 브라우저에서 `http://서버IP:5001` 접속
2. 기본 계정으로 로그인: `admin` / `admin1234`
3. 대시보드 정상 표시 확인
4. **시스템 관리 > 시스템 설정**에서 비밀번호 변경

---

## 운영 가이드

### 서비스 관리

```bash
cd sdl_deploy_package

docker compose ps                  # 상태 확인
docker compose logs -f sdl-app     # 앱 로그
docker compose logs -f postgres    # DB 로그
docker compose restart sdl-app     # 앱만 재시작
bash scripts/04_stop.sh            # 전체 중지
bash scripts/03_load_and_start.sh  # 전체 시작
```

### 앱 업데이트 (소스 변경 시)

인터넷 가능 PC에서:
```bash
# 앱 이미지만 재빌드 & 내보내기
cd sr_datalake
docker build -t sdl-app:latest .
docker save sdl-app:latest | gzip > sdl-app.tar.gz
# → sdl-app.tar.gz 를 서버로 전송
```

서버에서:
```bash
sudo bash scripts/05_update_app.sh /path/to/sdl-app.tar.gz
```

PostgreSQL/MinIO 컨테이너는 재시작하지 않고 앱 컨테이너만 교체됩니다.

### 데이터 백업

```bash
sudo bash scripts/06_backup.sh
```

출력:
```
=== [1/2] PostgreSQL 백업 ===
  → /opt/sdl-backup/sdl_pg_20260330_143000.sql.gz

=== [2/2] MinIO 데이터 백업 ===
  → /opt/sdl-backup/sdl_minio_20260330_143000.tar.gz
```

### 데이터 복원

```bash
# PostgreSQL 복원
gunzip -c /opt/sdl-backup/sdl_pg_YYYYMMDD.sql.gz | \
    docker compose exec -T postgres psql -U sdl_user -d sdl

# MinIO 데이터 복원
sudo bash scripts/04_stop.sh
sudo tar xzf /opt/sdl-backup/sdl_minio_YYYYMMDD.tar.gz -C /opt/sdl-data/
sudo bash scripts/03_load_and_start.sh
```

### 엔진 버전 업그레이드 (PostgreSQL / MinIO)

데이터가 호스트 볼륨에 분리되어 있으므로, 엔진 이미지만 교체합니다:

```bash
# 인터넷 가능 PC에서 새 이미지를 tar.gz로 내보내기
docker pull postgres:17-alpine
docker save postgres:17-alpine | gzip > postgres-17-alpine.tar.gz

# 서버에서
docker load -i postgres-17-alpine.tar.gz
# docker-compose.yml에서 이미지 태그 수정 후
docker compose up -d --no-deps --force-recreate postgres
```

**주의:** PostgreSQL 메이저 버전 업그레이드(예: 16→17)는 데이터 마이그레이션이 필요할 수 있습니다. `pg_upgrade`를 먼저 수행하세요.

### 디스크 사용량 확인

```bash
# 데이터 볼륨 사용량
du -sh /opt/sdl-data/postgres /opt/sdl-data/minio

# Docker 시스템 사용량
docker system df
```

---

## 디렉토리 구조 요약

```
서버 파일시스템
├── ~/sdl_deploy_package/          ← 배포 패키지 (설정+스크립트)
│   ├── docker-compose.yml
│   ├── .env
│   ├── config/
│   └── scripts/
│
└── /opt/sdl-data/                 ← 영속 데이터 (엔진과 분리)
    ├── postgres/                  ← PostgreSQL 데이터 파일
    │   ├── base/
    │   ├── global/
    │   ├── pg_wal/
    │   └── ...
    └── minio/                     ← MinIO 오브젝트
        ├── sdl-files/
        ├── sdl-archive/
        └── sdl-backup/
```

---

## 주의사항

- `.env`의 `SECRET_KEY`, `DB_PASSWORD`, `MINIO_ROOT_PASSWORD`는 반드시 변경
- MinIO 비밀번호를 `.env`에서 변경한 후에는 `docker compose up -d`로 재시작
- 설치 후 MinIO 접속 정보는 **관리자 UI > 시스템 설정**에서도 변경 가능 (UI 설정 우선)
- `DATA_ROOT` 경로에 충분한 디스크 공간 확보 (시계열 데이터 축적 고려)
- 앱 시작 시 `init_db()`가 테이블 자동 생성 및 sdm→sdl 데이터 마이그레이션 수행
- 폐쇄망 Docker 설치 시 `.deb`/`.rpm` 패키지를 사전에 준비해야 함
