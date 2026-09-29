# 환경 사실표

> 세션마다 다시 알아내야 했던 것을 고정한다. 여기가 단일 출처이며
> `server_info.txt`(로컬) · `~/server_info.md`(스테이징) · `~/BUILD.md`(프로덕션) 은
> 이 문서를 가리키는 포인터로만 남긴다.
>
> 최종 확인: 2026-08-10 (세 서버 직접 조회)

## 개요

| 항목            | 스테이징                | 프로덕션              | DGX 데모                |
|-----------------|-------------------------|-----------------------|-------------------------|
| 역할            | 개발 · 빌드 · **원본**  | 고객 운영            | 데모 · AI Agent 연동    |
| 호스트명        | admin01                 | ubuntu04 (QEMU)      | spark-55c4 (DGX Spark)  |
| SSH             | 119.207.126.16:2222     | 106.254.248.74:51204 | 119.207.126.16:9022     |
| 계정 · 인증     | inpark · SSH 키         | admin01 · 비밀번호   | inpark · SSH 키         |
| docker 권한     | sudo 필요 (비밀번호)    | 직접                 | 직접                    |
| git 클론        | ~/Workspace/sr_datalake | **없음**             | ~/Workspace/sr_datalake |
| 빌드 컨텍스트   | ~/sdl-build (심볼릭 링크) | — (반입만)         | 클론에서 직접           |
| 배포 디렉터리   | ~/sdl_deploy_package/   | ~/sdl_keti_deploy/   | 클론의 deploy/          |
| 브로커 설정     | 위 디렉터리의 config/   | 위 디렉터리의 config/ | deploy/config/          |
| CPU 아키텍처    | x86_64                  | x86_64               | **aarch64**             |
| 이미지 확보     | 로컬 빌드 (정본)        | 스테이징 이미지 반입 | 로컬 빌드 (아키텍처 상이) |
| 롤백 태그       | rollback-YYYYMMDD       | rollback-YYYYMMDD    | rollback-YYYYMMDD-dgx   |
| gunicorn worker | 4                       | 4                    | 4                       |
| 메모리          | 62 GiB                  | 31 GiB               | 121 GiB                 |

접속 비밀은 여기에 적지 않는다. 방법만 적고 값은 비밀번호 관리자에 둔다.

## 빌드 · 반출 흐름

프로덕션에는 git 저장소가 없다. 코드가 아니라 이미지 tarball 을 받는다.

    스테이징  cd ~/sdl-build && docker build -t sdl-app:latest .
              docker save sdl-app:latest | gzip -1 > /tmp/sdl-app.tar.gz
              scp /tmp/sdl-app.tar.gz admin01@<prod>:/tmp/
    프로덕션  docker load -i /tmp/sdl-app.tar.gz
              cd ~/sdl_keti_deploy && docker compose up -d --no-deps --force-recreate sdl-app

**DGX 는 `aarch64` 라 이 흐름을 탈 수 없다.** 스테이징·프로덕션은 `x86_64` 이므로
스테이징에서 만든 이미지를 DGX 에서 실행할 수 없다. DGX 는 클론을 pull 한 뒤
자체 빌드하는 것이 정상이며, 롤백 태그는 `-dgx` 접미어로 구분한다.

## 이미지 대조 방법 — ID 를 쓰지 말 것

`docker save`/`load` 를 거치면 **이미지 ID 와 SIZE 가 호스트마다 달라진다.** 데몬이
이미지를 재구성하기 때문이다(스테이징 715MB ↔ 프로덕션 960MB 처럼 크기까지 다르다).
반면 이미지 config 의 `created` 는 보존되므로, 환경 간 동일성은 **빌드 시각(UTC)** 으로
판정한다. 실제로 과거 태그 9개(`rollback-20260810`, `aasx`, `mqtt-jsonpath`,
`with-benthos-v1`, `phase8-storage-v1~v4` 등)의 빌드 시각이 스테이징과 프로덕션에서
초 단위까지 일치한다 — 반입이 정상 작동해 왔다는 증거다.

DGX 는 별도 빌드이므로 애초에 시각이 다르다. **DGX 는 커밋 해시로 추적한다.**

## 함정

- **`minio/minio` · `minio/mc` 를 Docker Hub 에서 받을 수 없다** (2026-09-29 확인).
  레지스트리 manifest 조회가 401 이며 DGX · 스테이징 어디서도 pull 되지 않는다.
  `deploy/scripts/01_build_and_export.sh` 가 이 둘을 pull 하므로 신규 설치·재빌드가
  막힌다. 보유 호스트에서 `docker save`/`load` 로 반입한다. 결정 기록 `0002` 참조.

- **리포의 `deploy/config/` 는 실행 설정이 아니다.** 배포 디렉터리는 리포에서 갈라져
  나온 사본이며 각자 표류했다. 리포를 고쳐도 스테이징 · 프로덕션 컨테이너에는
  반영되지 않는다. 2026-08-10 에 이 착각으로 엉뚱한 파일을 고쳤다.
- **스테이징 inpark 은 sudo 그룹이지만 무암호 실행이 안 된다.** `sudo -l` 에
  `(ALL) NOPASSWD: ALL` 이 보이지만 뒤따르는 `(ALL : ALL) ALL` 규칙이 이겨서
  비밀번호를 요구한다. docker 그룹에도 속해 있지 않다(구성원: yhlee, yjoh, tuyang).
- **DGX 의 시뮬레이터 DB(`legacy-sim-db`)는 `sr_ai_agent` 리포 소유다.**
  정의는 `~/Workspace/sr_ai_agent/sim/`. 이 리포지토리만 봐서는 찾을 수 없다.
  init 스크립트는 볼륨이 비어 있을 때만 실행되며 데이터는 정적이다(2026-07-01~07).
- **Benthos 는 앱 컨테이너의 자식 프로세스다.** 재배포하면 함께 사라지고 복원 로직이
  없다. 커넥터 status 는 running 으로 남으므로 DB 만 보면 정상으로 보인다.
- **DGX 의 git remote URL 에 GitHub 토큰이 평문으로 박혀 있다.** 토큰 교체 시 함께 정리.

## 파일 배치 — 각 파일의 집은 하나다

두 곳에 두면 반드시 갈라진다. 실제로 서버 정보가 세 곳(`server_info.txt` ·
`~/server_info.md` · `~/BUILD.md`)에 각기 다르게 낡아 있던 것이 그 사례다.

| 파일 | 위치 | 이유 |
|---|---|---|
| `CLAUDE.md` | **로컬 작업 폴더** | Claude Code 는 로컬 작업 디렉터리에서만 읽는다. 서버에 두면 효과가 없다 |
| `HANDOFF.md` | **로컬 작업 폴더** | 세션 시작 즉시 읽어야 한다. 매 세션 덮어쓰므로 git 이력이 불필요하고 기기 간 전달은 Drive 동기화가 맡는다 |
| `working_temp/` | **로컬** | 왕복 편집용 스크래치. 평상시 비어 있다 |
| `docs/worklogs/` · `docs/runbook/` · `docs/decisions/` · `docs/deploy-log.md` | 이 리포 | 이력이 필요하다 |
| `scripts/` | 이 리포 | 서버에서 실행한다 |

`HANDOFF.md` 는 덮어써서 사라지므로, 남겨야 할 내용은 세션 종료 시
`docs/worklogs/<오늘>.md` 로 옮긴다.

## 테스트 서버 — spark-6783 (2026-09-29 신설)

승격 대상이 아니므로 위 개요 표(스테이징·프로덕션·DGX)에 넣지 않는다.

| 항목 | 값 |
|---|---|
| 호스트명 | `spark-6783` (DGX Spark) |
| 접속 | LAN `192.168.1.18` · blunex · 비밀번호 (스테이징 경유) |
| OS · 아키텍처 | Ubuntu 24.04.5 LTS · **aarch64** |
| CPU · RAM · Disk | 20 Core · 121 GiB · 3.7 TB |
| docker 권한 | `blunex` ∈ `docker` → sudo 불필요 |
| git 클론 | `~/Workspace/sr_datalake` (스테이징에서 tar 반입, `.env` 제외) |
| 배포 디렉터리 | 클론의 `deploy/` |
| DATA_ROOT | **`/home/blunex/sdl-data`** (기본값 `/opt/sdl-data` 는 sudo 필요) |
| 앱 포트 | `0.0.0.0:5001` (LAN 노출 — DGX 는 loopback 전용이라 다름) |
| 이미지 확보 | 로컬 빌드 (아키텍처 상이) + minio 2종은 DGX 반입 |
| 동거 스택 | BaSyx AAS 5개 · ThingsBoard 2개 (포트 충돌 없음) |

**망**: WiFi `SOFTRAIN_STATIC-5G`. DGX 는 같은 SSID 를 `ipv4.method=manual` ·
gw `192.168.1.1` · dns `168.126.63.1` 로 쓰는데, 이 서버는 `auto`(DHCP) 다.
설치 착수 시 DHCP 가 gw `192.168.1.254` 를 줘 외부망이 전부 막혀 있었다.
현재는 DHCP 가 올바른 값을 주지만 **호스트에 고정된 것이 아니므로 재발 가능**하다.
고정하려면 DGX 와 동일하게 `manual` 로 바꾼다.

**WiFi 가 불안정하다** — 설치 중 SSH 가 3회 끊겼다. 장시간 작업은 `setsid nohup` 으로
분리한다.

설치는 `scripts/install-newhost.sh` 가 절차를 고정한다.
