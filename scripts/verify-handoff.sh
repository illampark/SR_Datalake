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

echo "배포 이미지      : $(docker inspect sdl-app --format '{{.Config.Image}} {{.Created}}' 2>/dev/null || echo '조회 실패')"

BENTHOS=$(docker exec sdl-app curl -s -m3 -o /dev/null -w '%{http_code}' \
  http://localhost:4195/ready 2>/dev/null || echo "---")
echo "Benthos 실체     : ${BENTHOS}   # 200 정상 / 000 미기동"

echo "브로커 유실 누적 : $(docker exec sdl-mosquitto \
  grep -c 'being dropped' /mosquitto/log/mosquitto.log 2>/dev/null || echo 0)"

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

echo
echo "기대값과 다르면 docs/deploy-log.md 를 먼저 보고, 없으면"
echo "컨테이너 Created 시각으로 git log 범위를 좁힌다."
