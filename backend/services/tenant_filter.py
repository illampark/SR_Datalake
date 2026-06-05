"""테넌트 필터 헬퍼 — Phase 4 (모듈별 라우트 마이그)

claudedocs/multitenant-test-policy.md § 2.1 계층 1 — 서비스 계층 필터.
Belt-and-suspenders 의 1차 방어 (다른 계층: ORM event, PG schema, CI 테스트).

설계:
- g.tenant_id 는 inject_tenant_context 가 항상 세팅 (Phase 2). off 모드는 1 고정.
- 따라서 본 헬퍼는 MULTITENANT_MODE 분기 없이 항상 tenant_id 필터를 적용한다.
  off 모드에서도 자동으로 tenant_id=1 필터 = 기존 단일테넌트 동작 (회귀 0).

API:
- filter_by_tenant(query, model)       — 쿼리에 tenant_id 필터 적용 (None-safe)
- get_by_id_tenant(db, model, item_id) — ID 조회 + tenant 가드
- assert_tenant(obj)                   — 객체의 tenant_id 가 컨텍스트와 일치하는지
- inject_tenant(obj)                   — 신규 객체에 tenant_id 주입
"""

from __future__ import annotations

from typing import Any, Optional

from flask import g
from sqlalchemy.orm import Query, Session


def _current_tenant_id() -> int:
    """g.tenant_id (없으면 1) — 단일 진실의 원천."""
    return getattr(g, "tenant_id", 1) or 1


def filter_by_tenant(query: Query, model: Any) -> Query:
    """쿼리에 model.tenant_id == g.tenant_id 필터를 추가한다.

    model 에 tenant_id 속성이 없으면 그대로 반환 (글로벌 테이블 케이스).
    """
    if not hasattr(model, "tenant_id"):
        return query
    return query.filter(model.tenant_id == _current_tenant_id())


def get_by_id_tenant(db: Session, model: Any, item_id: Any) -> Optional[Any]:
    """ID + tenant 로 조회. 다른 tenant 의 ID 면 None.

    `db.query(Model).get(id)` 의 멀티테넌트 안전 대체. None 처리는 호출 측에서.
    """
    q = db.query(model).filter(model.id == item_id)
    q = filter_by_tenant(q, model)
    return q.first()


def assert_tenant(obj: Any) -> None:
    """객체의 tenant_id 가 현재 컨텍스트와 일치하는지 검증.

    cross-reference 가드 — 예: pipeline 이 다른 tenant 의 collector_id 를
    참조하는 시도를 차단.

    super_admin 은 통과 (impersonate 흐름 등을 위해).
    """
    if not hasattr(obj, "tenant_id"):
        return
    if obj.tenant_id is None:
        return  # 시스템 레벨 행 (system_log 등) — 통과
    if getattr(g, "is_super", False):
        return
    expected = _current_tenant_id()
    if obj.tenant_id != expected:
        from backend.services.tenant_guard import CrossTenantWrite
        raise CrossTenantWrite(
            f"{type(obj).__name__}.tenant_id={obj.tenant_id} but context={expected}"
        )


def inject_tenant(obj: Any) -> Any:
    """신규 객체에 tenant_id 를 주입 (없거나 None 인 경우만).

    ORM event(tenant_guard) 가 flush 시점에 자동 주입하지만, 라우트 단에서도
    명시적으로 호출해 두면 디버깅·로그가 명확.

    반환: 같은 객체 (체이닝용).
    """
    if hasattr(obj, "tenant_id") and getattr(obj, "tenant_id", None) is None:
        obj.tenant_id = _current_tenant_id()
    return obj


def filter_by_tenant_or_null(query, model):
    """현재 tenant 의 행 + tenant_id IS NULL 행 모두 포함 - system_log 등 nullable 모델용.

    NULL = 시스템 레벨 행 (부팅, 마이그, 관리자 작업). tenant_admin 도 시스템 로그는
    조회할 수 있도록 OR 조건을 사용. super_admin 도 동일 (자기 tenant + NULL).
    """
    from sqlalchemy import or_
    if not hasattr(model, "tenant_id"):
        return query
    tid = _current_tenant_id()
    return query.filter(or_(model.tenant_id == tid, model.tenant_id.is_(None)))
