"""MinIO IAM 자격증명 격리 (Phase 8 B-1).

tenant 별 MinIO IAM 사용자 + 정책 자동 발급. SFTP/S3 API 양쪽 모두에 적용.

설계 결정:
- 비밀번호 보관: rotate-only — sdl-app DB 에는 username 만 (Tenant.minio_username).
  MinIO 가 hash 자체 보관. 분실 시 재발급 (새 비번 생성).
- 사용자명 패턴: t-{tid}-sftp (super_admin 의 root sdladmin 과 분리)
- 정책: 자기 tenant 의 4 종 bucket (files/archive/backup/exports) + ListAllMyBuckets

관련: minio_buckets.all_buckets_for(tid), Tenant 모델.
"""

import json
import logging
import secrets
from typing import Optional

from minio import MinioAdmin
from minio.credentials import StaticProvider

from backend.config import (
    MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE,
)
from backend.services.minio_buckets import all_buckets_for

logger = logging.getLogger(__name__)


def _admin() -> MinioAdmin:
    return MinioAdmin(
        endpoint=MINIO_ENDPOINT,
        credentials=StaticProvider(MINIO_ACCESS_KEY, MINIO_SECRET_KEY),
        secure=MINIO_SECURE,
    )


def username_for(tid: int) -> str:
    """tenant_id 별 IAM 사용자명. super_admin root (sdladmin) 와 분리."""
    return f"t-{tid}-sftp"


def policy_name_for(tid: int) -> str:
    return f"t-{tid}-policy"


def _build_policy(tid: int) -> dict:
    """자기 tenant 의 5 종 bucket 만 허용 — ListAllMyBuckets 미부여.

    SFTP root level ls 는 AccessDenied. 사용자가 자기 bucket 이름을
    명시 cd 하면 정상 접근. 다른 tenant 의 bucket 이름 정보 자체를 차단.
    """
    buckets = all_buckets_for(tid)
    resources: list[str] = []
    for b in buckets:
        resources.append(f"arn:aws:s3:::{b}")
        resources.append(f"arn:aws:s3:::{b}/*")
    return {
        "Version": "2012-10-17",
        "Statement": [
            {
                "Effect": "Allow",
                "Action": ["s3:*"],
                "Resource": resources,
            },
        ],
    }


def _generate_password() -> str:
    """SFTP 비밀번호 (URL-safe 24 bytes ≈ 32 char, ~192-bit entropy)."""
    return secrets.token_urlsafe(24)


def ensure_tenant_iam(tid: int) -> str:
    """tenant 의 IAM user + policy 가 존재함을 보장.

    이미 있으면 그대로 두고 username 반환. 없으면 새로 만들고 초기 비번은
    버려진다 (rotate-only 정책 — 사용자가 rotate 호출 시 새 비번 발급).

    반환: username (예: 't-2-sftp')
    """
    username = username_for(tid)
    policy_name = policy_name_for(tid)
    admin = _admin()

    # 정책 갱신 — policy_add 가 silent overwrite 안 하는 케이스가 있어
    # 안전하게 remove 후 add. attach 는 아래 policy_set 으로 재부여.
    policy_dict = _build_policy(tid)
    try:
        admin.policy_remove(policy_name=policy_name)
    except Exception:
        pass  # 정책 없으면 무시
    admin.policy_add(policy_name=policy_name, policy=policy_dict)

    # 사용자 존재 확인 (없으면 추가)
    try:
        admin.user_info(access_key=username)
        logger.info("MinIO IAM user already exists: %s", username)
    except Exception:
        # 초기 비번은 임시 — rotate 전엔 사용 불가 (운영자가 첫 rotate 로 발급)
        initial_pw = _generate_password()
        admin.user_add(access_key=username, secret_key=initial_pw)
        logger.info("MinIO IAM user created: %s (initial password not exposed)", username)
        # 보안: initial_pw 는 응답으로 노출 안 함. 첫 rotate 호출까지 사용 불가.

    # 정책 attach (idempotent — 이미 붙어 있어도 noop)
    try:
        admin.policy_set(policy_name=policy_name, user=username)
    except Exception as e:
        logger.warning("policy_set failed (may already attached): %s", e)

    return username


def rotate_password(tid: int) -> tuple[str, str]:
    """비밀번호 재발급. (username, new_password) 반환.

    new_password 는 호출 즉시 응답으로 1 회만 전달. sdl-app DB 에는 미저장.
    """
    username = ensure_tenant_iam(tid)
    new_pw = _generate_password()
    admin = _admin()
    # user_add 는 기존 사용자에 대해서도 비번 갱신 동작 (MinIO admin 의 idempotent set)
    admin.user_add(access_key=username, secret_key=new_pw)
    admin.user_enable(access_key=username)
    logger.info("MinIO IAM password rotated: %s", username)
    return username, new_pw


def disable_tenant_iam(tid: int) -> None:
    """tenant archive 시 IAM 사용자 비활성화 (삭제 X — 복구 가능)."""
    username = username_for(tid)
    admin = _admin()
    try:
        admin.user_disable(access_key=username)
        logger.info("MinIO IAM user disabled: %s", username)
    except Exception as e:
        logger.warning("user_disable failed for %s: %s", username, e)


def get_iam_info(tid: int) -> dict:
    """현 tenant 의 IAM 상태 (username, enabled, attached policies).

    응답에 비밀번호 포함 안 함 (rotate-only 정책).
    """
    username = username_for(tid)
    admin = _admin()
    try:
        info_str = admin.user_info(access_key=username)
        info = json.loads(info_str) if isinstance(info_str, str) else info_str
    except Exception:
        info = None
    return {
        "username": username,
        "exists": info is not None,
        "status": info.get("status") if info else None,
        "policy": info.get("policyName") if info else policy_name_for(tid),
        "allowedBuckets": all_buckets_for(tid),
    }
