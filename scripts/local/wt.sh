#!/usr/bin/env bash
# working_temp 왕복 도구 — 로컬(개발 PC)에서 실행한다.
#
#   wt.sh fetch <슬롯> <리포상대경로>...   내려받기 + MANIFEST 기록
#   wt.sh push  <슬롯>                     해시 확인 후 되돌려 보내기
#   wt.sh close <슬롯>                     _done 으로 이동
#
# 환경변수
#   WT_ROOT   working_temp 경로 (필수)
#   WT_KEY    SSH 키 경로 (필수)
#   WT_REMOTE 기본 inpark@119.207.126.16
#   WT_PORT   기본 2222
#
# 규칙: 평상시 working_temp 는 비어 있다. 슬롯이 남아 있다는 것은 되돌려 보내지 않은
#       편집이 있다는 뜻이며, HANDOFF.md 의 "열린 슬롯" 에 기재돼 있어야 한다.
#
# push 는 내려받을 때 기록한 해시와 서버의 현재 해시를 대조한다. 다르면
# 아무것도 전송하지 않고 중단한다 — 다른 기기의 작업을 말없이 되돌리는 사고를 막는다.
set -euo pipefail

: "${WT_ROOT:?WT_ROOT 에 working_temp 경로를 설정하세요}"
: "${WT_KEY:?WT_KEY 에 SSH 키 경로를 설정하세요}"
REMOTE="${WT_REMOTE:-inpark@119.207.126.16}"
PORT="${WT_PORT:-2222}"
REPO='~/Workspace/sr_datalake'

rsh(){ ssh -i "$WT_KEY" -p "$PORT" -o BatchMode=yes "$REMOTE" "$@"; }
rhash(){ rsh "sha256sum $REPO/$1 2>/dev/null | cut -d' ' -f1"; }

cmd="${1:?fetch|push|close}"; slot="${2:?슬롯명}"
dir="$WT_ROOT/$slot"; man="$dir/MANIFEST.tsv"

case "$cmd" in
  fetch)
    shift 2; [ $# -gt 0 ] || { echo "내려받을 경로를 지정하세요"; exit 1; }
    mkdir -p "$dir"
    head=$(rsh "cd $REPO && git rev-parse --short HEAD")
    { echo "# slot    $slot"
      echo "# source  $REMOTE:$REPO @ $head"
      echo "# fetched $(date '+%Y-%m-%d %H:%M %Z')"
      echo "#"
      echo "# 주의  리포의 deploy/config/ 는 실행 설정이 아니다."
      echo "#       실행본 스테이징 ~/sdl_deploy_package/config/"
      echo "#            프로덕션 ~/sdl_keti_deploy/config/"
    } > "$man"
    for p in "$@"; do
      mkdir -p "$dir/$(dirname "$p")"
      scp -q -i "$WT_KEY" -P "$PORT" "$REMOTE:$REPO/$p" "$dir/$p"
      printf '%s\t%s\n' "$p" "$(rhash "$p")" >> "$man"
      echo "받음  $p"
    done
    ;;

  push)
    [ -f "$man" ] || { echo "매니페스트가 없습니다: $man"; exit 1; }
    fail=0
    while IFS=$'\t' read -r p want; do
      case "$p" in \#*|"") continue;; esac
      have=$(rhash "$p")
      if [ "$have" != "$want" ]; then
        echo "중단: 내려받은 뒤 서버 원본이 바뀌었습니다 — $p"
        echo "  받을 때  $want"
        echo "  현재     $have"
        fail=1
      fi
    done < "$man"
    if [ "$fail" -ne 0 ]; then
      echo
      echo "아무것도 전송하지 않았습니다. 다시 받아 편집을 옮기세요."
      exit 1
    fi
    while IFS=$'\t' read -r p want; do
      case "$p" in \#*|"") continue;; esac
      scp -q -i "$WT_KEY" -P "$PORT" "$dir/$p" "$REMOTE:$REPO/$p"
      echo "전송  $p  →  $(rhash "$p")"
    done < "$man"
    echo
    echo "스테이징에서 커밋하는 것을 잊지 마세요."
    ;;

  close)
    mkdir -p "$WT_ROOT/_done"
    mv "$dir" "$WT_ROOT/_done/"
    echo "닫음  $slot → _done/"
    # 불변식 확인: 평상시 working_temp 는 비어 있어야 한다.
    left=$(find "$WT_ROOT" -mindepth 1 -maxdepth 1 -type d \
             ! -name '_bin' ! -name '_done' | wc -l)
    if [ "$left" -eq 0 ]; then
      echo "working_temp 가 비었습니다 — 정상 상태입니다."
    else
      echo "열린 슬롯 ${left}개가 남았습니다. HANDOFF.md 에 기재하세요:"
      find "$WT_ROOT" -mindepth 1 -maxdepth 1 -type d \
        ! -name '_bin' ! -name '_done' -exec basename {} \; | sed 's/^/  /'
    fi
    ;;

  *) echo "알 수 없는 명령: $cmd"; exit 1;;
esac
