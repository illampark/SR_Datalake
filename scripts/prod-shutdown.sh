#!/usr/bin/env bash
# 프로덕션 SDL 서비스 전체를 순서대로 내린다 (2026-10-01 TTA 성능 시험 대비 종료 절차를 고정).
#
#   사용: 프로덕션에서  bash prod-shutdown.sh        (리포가 없으므로 이 파일만 반입한다)
#
# 하는 일
#   1. 종료 전 상태를 ~/sdl_shutdown_<날짜>/ 에 남긴다 (재개 시 대조 기준)
#   2. nginx 를 내리고 부팅 시 자동 기동을 끈다 (sudo 비밀번호를 묻는다)
#   3. sdl-app → sim-all → mosquitto → minio → postgres 순으로 정지한다
#   4. compose down 으로 컨테이너를 제거한다. restart: always 라서 stop 만 하면
#      호스트 재부팅 때 되살아난다. 볼륨은 지우지 않는다 (-v 를 붙이지 말 것).
#   5. 볼륨 · 이미지 · 설정 파일이 그대로인지와 포트가 닫혔는지 확인한다
#
# 전제
#   - 실행 계정이 docker 그룹에 속한다 (admin01 은 속한다 — docker 에 sudo 불필요)
#   - 커넥터 · 파이프라인을 먼저 앱에서 stop 해 둔다. DB status 가 running 인 채로
#     내리면 재개 후 "running 인데 수집은 없는" 상태가 된다. 1단계가 이를 검사한다.
#
# 재개: 스냅샷 디렉터리의 RESUME.txt
set -uo pipefail

DEPLOY_DIR="${DEPLOY_DIR:-$HOME/sdl_keti_deploy}"
SNAP="${SNAP:-$HOME/sdl_shutdown_$(date +%Y%m%d)}"
CONNECTOR_TABLES="api_connector db_connector file_collector import_collector modbus_connector mqtt_connector opcua_connector"

psql_q() {
  docker exec sdl-postgres sh -c 'psql -U "$POSTGRES_USER" -d "$POSTGRES_DB" -At -F " | " -c "$1"' sh "$1" </dev/null
}

cd "$DEPLOY_DIR" || { echo "!! $DEPLOY_DIR 없음"; exit 1; }
echo "── 대상: $(hostname) · 스냅샷 $SNAP"

# ── 1. 종료 전 상태 ──
mkdir -p "$SNAP"
date -u +%FT%TZ > "$SNAP/snapshot_time.txt"
docker ps -a --no-trunc --format '{{.Names}}\t{{.Image}}\t{{.Status}}\t{{.Ports}}' > "$SNAP/ps_before.txt"
docker images --format '{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}\t{{.Size}}' > "$SNAP/images.txt"
docker volume ls > "$SNAP/volumes.txt"
md5sum docker-compose.yml .env config/* > "$SNAP/deploy_md5.txt" 2>&1
cp -p docker-compose.yml "$SNAP/compose_at_shutdown.yml"
psql_q "select id, name, status from pipeline order by id" > "$SNAP/pipelines.txt"
: > "$SNAP/connectors.txt"
for t in $CONNECTOR_TABLES; do
  psql_q "select '$t', id, name, status from $t order by id" >> "$SNAP/connectors.txt"
done
for c in sdl-app sim-all sdl-postgres sdl-mosquitto sdl-minio; do
  docker logs --tail 3000 "$c" > "$SNAP/log_$c.txt" 2>&1 </dev/null
done

if grep -h -E '\| running$' "$SNAP/pipelines.txt" "$SNAP/connectors.txt"; then
  echo "!! 위 항목이 running 이다. 앱에서 먼저 stop 한 뒤 다시 실행한다."
  exit 1
fi
active=$(psql_q "select count(*) from pg_stat_activity where datname=current_database() and pid<>pg_backend_pid() and state='active'")
echo "DB 활성 쿼리: $active  (0 이 아니면 진행 중인 작업을 먼저 확인)"

# ── 2. 진입점 ──
sudo systemctl disable --now nginx
echo "nginx: $(systemctl is-active nginx) / $(systemctl is-enabled nginx)"

# ── 3. 정지 ── 앱이 먼저, DB 가 마지막
docker compose stop -t 60  sdl-app   </dev/null
docker compose stop -t 30  sim-all   </dev/null   # SIGTERM 을 처리하지 않아 137 로 끝난다. 상태가 없어 무해
docker compose stop -t 30  mosquitto </dev/null
docker compose stop -t 60  minio     </dev/null
docker compose stop -t 180 postgres  </dev/null
for c in sdl-app sim-all sdl-mosquitto sdl-minio sdl-postgres; do
  docker inspect "$c" --format '{{.Name}} {{.State.Status}} exit={{.State.ExitCode}}'
  docker logs --tail 40 "$c" > "$SNAP/shutdown_log_$c.txt" 2>&1 </dev/null
done
grep -q "database system is shut down" "$SNAP/shutdown_log_sdl-postgres.txt" \
  && echo "postgres: 정상 종료 확인" || echo "!! postgres 종료 로그에 'database system is shut down' 없음"

# ── 4. 제거 ── 볼륨 유지
docker compose down </dev/null

# ── 5. 검증 ──
docker volume ls > "$SNAP/volumes_after.txt"
diff "$SNAP/volumes.txt" "$SNAP/volumes_after.txt" && echo "볼륨: 동일"
docker images --format '{{.Repository}}:{{.Tag}}\t{{.ID}}\t{{.CreatedAt}}\t{{.Size}}' | diff "$SNAP/images.txt" - && echo "이미지: 동일"
md5sum -c "$SNAP/deploy_md5.txt" 2>&1 | grep -v ': OK$' || echo "배포 파일: 동일"
echo "남은 컨테이너: $(docker ps -aq | wc -l)"
ss -ltn | grep -E ':(80|443|5001|1883|5432|8022|9000|9001)\b' || echo "SDL 관련 포트 LISTEN 없음"

cat > "$SNAP/RESUME.txt" <<R
SDL 서비스 재개 절차 — $(date +%F) 종료분

1. 달라진 것이 없는지 확인
     cd $SNAP && md5sum -c deploy_md5.txt
     docker volume ls | diff volumes.txt -
2. 컨테이너 기동
     cd $DEPLOY_DIR && docker compose up -d
     docker compose ps        # sdl-app · sdl-postgres · sdl-minio healthy, sdl-minio-init Exited(0)
3. nginx 기동 + 부팅 시 자동 기동 복원
     sudo systemctl enable --now nginx
4. 확인
     curl -s -o /dev/null -w '%{http_code}\n' http://127.0.0.1:5001/login     # 200
     curl -k -s -o /dev/null -w '%{http_code}\n' https://127.0.0.1/login      # 200
5. 파이프라인 · 커넥터는 pipelines.txt · connectors.txt 의 종료 전 상태로 되돌린다.
   수집을 다시 켤 때는 ① 파이프라인 start ② 커넥터 start 순으로 하고 데이터가 흐르는지 본다.
R
echo "── 완료. 재개 절차: $SNAP/RESUME.txt"
