"""테넌트 단위 MinIO 버킷 이름·생성·정리 헬퍼 — Phase 5.

claudedocs/multitenant-design-v1.md § 4 / migration-guide.md § 6 참조.

설계:
- tenant 1 (현 운영 고객) 은 legacy 이름 (sdl-files, sdl-archive, sdl-backup,
  sdl-exports) 을 그대로 사용 → 데이터 마이그 비용 0.
- tenant 2+ 는 t-{id}-{role} 패턴 (예: t-2-files, t-2-archive).
- 신규 tenant 생성 시 4 종 버킷 자동 생성. 삭제 시 자동 정리.

사용 예:
    from backend.services.minio_buckets import bucket_for
    bucket_for("files")              # → 현재 g.tenant_id 기준
    bucket_for("files", tenant_id=1) # → "sdl-files"
    bucket_for("files", tenant_id=2) # → "t-2-files"
"""
from __future__ import annotations

from typing import Optional


# 5 종 표준 role — Phase 8 B-4 에 imports 추가 (minio_bucket 모드 import source)
ROLES = ("files", "archive", "backup", "exports", "imports")

# tenant 1 의 legacy 이름 — 데이터 마이그 회피용 매핑
_LEGACY = {
    "files":   "sdl-files",
    "archive": "sdl-archive",
    "backup":  "sdl-backup",
    "exports": "sdl-exports",
    "imports": "sdl-imports",
}


def bucket_for(role: str, tenant_id: Optional[int] = None) -> str:
    """role + tenant_id 에 해당하는 bucket 이름을 반환.

    tenant_id 가 None 이면 현재 요청 컨텍스트 (g.tenant_id) 사용. 그도 없으면 1.

    role: files / archive / backup / exports (또는 임의 문자열 — sdl-<role> 로 폴백)
    """
    if tenant_id is None:
        from backend.services.tenant_filter import _current_tenant_id
        tenant_id = _current_tenant_id()

    if tenant_id == 1:
        # 기본 4 종은 legacy 이름. 그 외 임의 role 은 sdl- 접두사.
        return _LEGACY.get(role, f"sdl-{role}")
    return f"t-{tenant_id}-{role}"


def all_buckets_for(tenant_id: int) -> list[str]:
    """해당 tenant 가 가져야 할 4 종 버킷 이름 목록."""
    return [bucket_for(r, tenant_id) for r in ROLES]


def parse_tenant_from_bucket(name: str) -> Optional[int]:
    """버킷 이름에서 tenant_id 추출 (webhook 핸들러용).

    `sdl-files` 등 legacy → 1
    `t-N-files` 등 → N
    그 외 → None (시스템 버킷, 모르는 패턴)
    """
    if name.startswith("sdl-") and name.split("-", 1)[1] in _LEGACY or name in _LEGACY.values():
        return 1
    if name.startswith("t-"):
        # t-<id>-<role>
        parts = name.split("-", 2)
        if len(parts) >= 2 and parts[1].isdigit():
            return int(parts[1])
    return None


def ensure_tenant_buckets(client, tenant_id: int) -> dict[str, str]:
    """해당 tenant 의 4 종 버킷이 없으면 생성. 결과를 role→bucket dict 로 반환.

    client: minio.Minio 인스턴스
    """
    result = {}
    for role in ROLES:
        name = bucket_for(role, tenant_id)
        result[role] = name
        try:
            if not client.bucket_exists(name):
                client.make_bucket(name)
        except Exception as e:
            # 멱등 - 동시 생성 race condition 시 무시
            import logging
            logging.getLogger(__name__).warning(
                "ensure_tenant_buckets: %s 생성 실패 (이미 존재 가능): %s", name, e,
            )
        # MinIO event subscription (webhook -> minio_object 인덱스)
        try:
            _subscribe_bucket_events(client, name)
        except Exception as e:
            import logging
            logging.getLogger(__name__).warning(
                "ensure_tenant_buckets: %s 이벤트 구독 실패: %s", name, e,
            )
    return result


def _subscribe_bucket_events(client, bucket: str) -> None:
    """버킷에 MinIO webhook 이벤트 구독 (PUT/DELETE).

    sdl-files 등 legacy 버킷은 minio-init 이 셋업했지만 신규 t-N-* 는
    프로비저닝 시점에 명시 구독해야 minio_object 인덱스가 채워진다.
    """
    from minio.notificationconfig import (
        NotificationConfig, QueueConfig,
    )
    cfg = NotificationConfig(
        queue_config_list=[QueueConfig(
            events=["s3:ObjectCreated:*", "s3:ObjectRemoved:*"],
            queue="arn:minio:sqs::PRIMARY:webhook",
        )]
    )
    client.set_bucket_notification(bucket, cfg)


def drop_tenant_buckets(client, tenant_id: int, force: bool = False) -> None:
    """해당 tenant 의 4 종 버킷을 삭제.

    force=False (기본): 비어있을 때만 삭제. 데이터 있으면 RuntimeError.
    force=True: 안의 객체까지 모두 제거 후 버킷 삭제 (위험).

    tenant 1 (legacy 버킷) 은 절대 삭제 금지.
    """
    if tenant_id == 1:
        raise ValueError("refuse to drop tenant 1 legacy buckets")

    for role in ROLES:
        name = bucket_for(role, tenant_id)
        if not client.bucket_exists(name):
            continue
        objs = list(client.list_objects(name, recursive=True))
        if objs and not force:
            raise RuntimeError(f"bucket {name} not empty ({len(objs)} objects); pass force=True to drop")
        if force and objs:
            for o in objs:
                client.remove_object(name, o.object_name)
        client.remove_bucket(name)
