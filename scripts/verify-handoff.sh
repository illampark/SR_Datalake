#!/usr/bin/env bash
# HANDOFF.md 의 기대값과 대조하기 위한 재확인 명령.
# "DB 가 뭐라고 하는지" 가 아니라 "실제로 살아 있는지" 를 본다.
#
#   사용: scripts/verify-handoff.sh [staging|prod|dgx]
#
# 대상 서버에서 실행한다. 출력 순서는 HANDOFF.md 의 기대값 표와 같다.
set -uo pipefail

ENV_NAME="${1:-current}"
echo "── 대상: ${ENV_NAME} · $(hostname) · $(date '+%Y-%m-%d %H:%M %Z')"
echo

if git rev-parse --git-dir >/dev/null 2>&1; then
  echo "리포 HEAD        : $(git rev-parse --short HEAD)"
  git fetch -q origin 2>/dev/null || true
  echo "origin 대비      : $(git rev-list --left-right --count origin/main...HEAD 2>/dev/null \
    | awk '{print "origin 앞선 " $1 " / 로컬 앞선 " $2}')"
  DIRTY=$(git status --short | wc -l)
  echo "미커밋 변경      : ${DIRTY} 건"
else
  echo "리포 HEAD        : (git 저장소 없음 — 이미지 반입 환경)"
fi

# docker 접근 가능 여부를 먼저 판정한다. 스테이징의 inpark 은 docker 그룹이
# 아니므로 sudo 로 실행해야 아래 항목들이 채워진다.
if ! docker info >/dev/null 2>&1; then
  echo
  echo "!! docker 에 접근할 수 없습니다. 아래 항목은 조회하지 못합니다."
  echo "   스테이징이라면 'sudo scripts/verify-handoff.sh ${ENV_NAME}' 로 다시 실행하세요."
  echo
  DOCKER_OK=0
else
  DOCKER_OK=1
fi

# 출력은 항상 한 줄로 유지한다 — HANDOFF.md 기대값 표와 줄 단위로 대조하기 위함.
one(){ tr -d '\n' | tr -s ' '; }

if [ "$DOCKER_OK" -eq 1 ]; then
  # 이미지 ID 가 "어느 빌드가 도는지" 의 유일한 신원이다. 세 환경에서 이 값이
  # 같아야 같은 빌드다. 컨테이너 생성 시각과 혼동하지 말 것 — 이미지를 반입해도
  # 컨테이너는 새로 만들어지므로 시각이 달라진다.
  # 서버마다 docker 가 로컬시각/UTC 를 섞어 보고하므로 UTC 로 정규화한다.
  # 정규화하지 않으면 환경 간 비교에서 9시간을 오해하게 된다.
  utc(){ date -u -d "$1" '+%Y-%m-%dT%H:%M:%SZ' 2>/dev/null || printf '%s' "$1"; }

  IMG_REF=$(docker inspect sdl-app --format '{{.Config.Image}}' 2>/dev/null | one)
  IMG_ID=$(docker inspect sdl-app --format '{{.Image}}' 2>/dev/null | one | cut -c8-19)
  IMG_BUILT=$(utc "$(docker image inspect "${IMG_REF:-sdl-app:latest}" \
    --format '{{.Created}}' 2>/dev/null | one)")
  CTR_MADE=$(utc "$(docker inspect sdl-app --format '{{.Created}}' 2>/dev/null | one)")
  echo "배포 이미지      : ${IMG_REF:-조회 실패}  ${IMG_ID}  빌드 ${IMG_BUILT}"
  echo "컨테이너 생성    : ${CTR_MADE}"

  BENTHOS=$(docker exec sdl-app curl -s -m3 -o /dev/null -w '%{http_code}' \
    http://localhost:4195/ready 2>/dev/null | one)
  echo "Benthos 실체     : ${BENTHOS:----}   # 200 정상 / 000 미기동"

  DROPS=$(docker exec sdl-mosquitto \
    grep -c 'being dropped' /mosquitto/log/mosquitto.log 2>/dev/null | one)
  echo "브로커 유실 누적 : ${DROPS:-0}"
else
  echo "배포 이미지      : (docker 권한 없음)"
  echo "Benthos 실체     : (docker 권한 없음)"
  echo "브로커 유실 누적 : (docker 권한 없음)"
fi

if [ "$DOCKER_OK" -eq 1 ]; then
  echo
  echo "── 커넥터 (DB status 와 마지막 수집 시각)"
  docker exec sdl-postgres psql -U sdl_user -d sdl -Atc \
    "select id||'  '||name||'  '||status||'  '||coalesce(last_collected_at::text,'never')
       from db_connector order by id" 2>/dev/null || echo "  조회 실패"

  echo
  echo "── 파이프라인"
  docker exec sdl-postgres psql -U sdl_user -d sdl -Atc \
    "select count(*) filter (where status='running')||' running / '||count(*)||' total'
       from pipeline" 2>/dev/null || echo "  조회 실패"
fi

echo
echo "기대값과 다르면 docs/deploy-log.md 를 먼저 보고, 없으면"
echo "컨테이너 Created 시각으로 git log 범위를 좁힌다."
