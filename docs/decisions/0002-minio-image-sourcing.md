# 0002. MinIO 이미지는 Docker Hub 대신 보유분 반입으로 확보한다

- **상태** 채택 (2026-09-29)

## 맥락

신규 테스트 서버(`spark-6783`) 설치 중 `docker pull minio/minio:latest` 가 거부됐다.

    Error response from daemon: pull access denied for minio/minio,
    repository does not exist or may require 'docker login'

**기존 DGX 에서도 동일하게 실패한다.** 즉 우리 환경 문제가 아니라 MinIO 측이 Docker Hub
공개 배포를 제한한 외부 변경이다. 레지스트리 직접 조회로 확인했다.

    minio/minio manifest -> 401
    minio/mc    manifest -> 401

`deploy/scripts/01_build_and_export.sh` 는 `minio/minio:latest` 와 `minio/mc:latest` 를
`docker pull` 한다. 따라서 **모든 환경의 신규 설치와 재빌드가 이 지점에서 막힌다.**
현재 돌고 있는 환경들은 과거에 받아둔 이미지로 동작 중일 뿐이다.

## 결정

- 당장은 **보유 이미지를 `docker save`/`load` 로 반입**한다. DGX 가 2025-09 에 받은
  aarch64 이미지(`minio/minio:latest` 167MB · `minio/mc:latest` 82.3MB)를 사용했다.
- `deploy/docker-compose.yml` 의 이미지 참조는 **바꾸지 않는다.**
- 반입 시 전송 전후 md5 를 대조한다.

## 기각한 대안

**`quay.io/minio/minio` 로 이미지 출처 교체.** 태그 체계와 릴리스 정책이 Docker Hub
배포와 다르고, 세 환경(스테이징 x86_64 · 프로덕션 x86_64 · DGX aarch64)에서 같은
버전이 동일하게 동작하는지 재검증해야 한다. 검증 없이 지금 바꾸면
`docs/runbook/environments.md` 의 "빌드 시각으로 환경 간 동일성을 판정한다" 기준이
흔들린다. 이미지 출처 변경은 별도 과제로 분리한다.

**`latest` 대신 특정 버전 태그 고정.** 어차피 같은 저장소가 401 이라 효과가 없다.

## 후속 과제

- **스테이징·프로덕션에 x86_64 minio 이미지 보유분이 남아 있는지 확인한다.**
  남아 있지 않으면 다음 재배포에서 같은 벽에 막힌다. 스테이징은 `inpark` 이 docker
  그룹이 아니라 이번 세션에서 확인하지 못했다(`sudo docker images | grep minio`).
- 보유분이 있다면 **tarball 로 보존**해 두어야 한다. 이미지가 로컬에서 사라지면
  복구 경로가 없다.
- 중기적으로 이미지 출처를 정하고(quay.io 또는 사설 레지스트리) 세 환경에서 검증한다.
