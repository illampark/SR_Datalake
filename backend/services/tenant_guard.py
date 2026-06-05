"""테넌트 격리 ORM 가드 — Phase 3 (belt-and-suspenders)

claudedocs/multitenant-test-policy.md § 2 계층 2 — SQLAlchemy 이벤트로
cross-tenant write 를 자동 차단·자동 주입.

활성 조건: MULTITENANT_MODE=on 일 때만. off 일 땐 노옵 (단일테넌트 동작 호환).

기본 흐름:
  before_flush 이벤트에서 session.new (신규 객체) / session.dirty (수정 객체) 검사:
    - tenant_id 속성을 가진 모델이고
    - g.tenant_id 가 컨텍스트로 설정돼 있을 때
  → 신규: tenant_id None 이면 g.tenant_id 자동 주입,
          다른 tenant_id 면 super 가 아닐 경우 CrossTenantWrite 발생
  → 수정: tenant_id 가 컨텍스트와 다르면 super 가 아닐 경우 CrossTenantWrite

Phase 4 이후 모듈별 라우트 마이그가 진행되면서 서비스 계층 필터가 1차 방어가 되고
이 가드는 안전망 역할을 한다.
"""

from __future__ import annotations
from sqlalchemy import event
from sqlalchemy.orm import Session


class CrossTenantWrite(Exception):
    """다른 tenant 의 데이터를 쓰려 한 시도. 무조건 차단."""
    pass


_installed = False


def install_tenant_guard() -> None:
    """SQLAlchemy Session 에 before_flush 이벤트 핸들러 등록.

    멱등 — 여러 번 호출돼도 한 번만 설치.
    """
    global _installed
    if _installed:
        return
    _installed = True

    @event.listens_for(Session, "before_flush")
    def _enforce_tenant(session, flush_context, instances):
        # 지연 import — 모듈 로딩 순서 의존 회피
        from flask import current_app, g

        try:
            if not current_app.config.get("MULTITENANT_MODE"):
                return
        except RuntimeError:
            # request context 외부 (마이그·시드 등) — 가드 비활성
            return

        tid = getattr(g, "tenant_id", None)
        if tid is None:
            # 컨텍스트 미확립 (예: 시스템 작업) — 가드 비활성
            return

        is_super = bool(getattr(g, "is_super", False))

        # 신규 객체 검사
        for obj in session.new:
            if not hasattr(obj, "tenant_id"):
                continue
            cur = obj.tenant_id
            if cur is None:
                # 자동 주입 — 단일테넌트 호환 + 누락 방어
                obj.tenant_id = tid
            elif cur != tid and not is_super:
                raise CrossTenantWrite(
                    f"new {type(obj).__name__}.tenant_id={cur} but context={tid}"
                )

        # 수정 객체 검사
        for obj in session.dirty:
            if not hasattr(obj, "tenant_id"):
                continue
            cur = obj.tenant_id
            if cur is not None and cur != tid and not is_super:
                raise CrossTenantWrite(
                    f"update {type(obj).__name__}.tenant_id={cur} but context={tid}"
                )
