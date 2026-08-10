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
| 이미지 확보     | 로컬 빌드               | tar 반입             | 로컬 빌드 (정책 이탈)   |
| 롤백 태그       | —                       | rollback-YYYYMMDD 보유 | 없음 (latest 덮어씀)  |
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

DGX 는 현재 이 흐름을 따르지 않고 클론에서 직접 빌드한다. 반입형으로 통일이 필요하다.

## 함정

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
