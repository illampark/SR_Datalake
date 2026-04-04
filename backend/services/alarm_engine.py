"""알람 평가 엔진 — 규칙 평가 + 알림 발송

헬스체크 결과(_run_healthcheck)를 입력으로 받아 알람 규칙과 대조한 뒤
신규 알람 이벤트를 생성하고 알림 채널로 발송한다.

평가 주기: 60초 (백그라운드 스레드)
"""

import json
import logging
import re
import threading
import time
from datetime import datetime, timedelta

import requests

from backend.database import SessionLocal
from backend.models.alarm import AlarmRule, AlarmEvent, AlarmChannel

logger = logging.getLogger(__name__)

_timer = None
_running = False
_INTERVAL = 60  # 평가 주기 (초)


# ══════════════════════════════════════════════
# 엔진 시작 / 정지
# ══════════════════════════════════════════════

def start():
    """백그라운드 평가 루프 시작"""
    global _running
    if _running:
        return
    _running = True
    logger.info("알람 엔진 시작 (주기: %ds)", _INTERVAL)
    _schedule_next()


def stop():
    global _running, _timer
    _running = False
    if _timer:
        _timer.cancel()
        _timer = None
    logger.info("알람 엔진 정지")


def _schedule_next():
    global _timer
    if not _running:
        return
    _timer = threading.Timer(_INTERVAL, _tick)
    _timer.daemon = True
    _timer.start()


def _tick():
    """주기적 평가 실행"""
    try:
        evaluate()
    except Exception as e:
        logger.error("알람 평가 오류: %s", e)
    _schedule_next()


# ══════════════════════════════════════════════
# 평가 로직
# ══════════════════════════════════════════════

def evaluate():
    """전체 규칙 평가 수행 — 외부에서 수동 호출도 가능"""
    from backend.routes.monitoring import _run_healthcheck_raw

    db = SessionLocal()
    try:
        # 1) 현재 컴포넌트 상태 수집
        components = _run_healthcheck_raw(db)

        # 2) 활성 규칙 로드
        rules = db.query(AlarmRule).filter(AlarmRule.enabled == True).all()  # noqa: E712
        if not rules:
            return

        now = datetime.utcnow()
        fired_count = 0

        for rule in rules:
            # 3) 규칙에 해당하는 컴포넌트 필터링
            targets = _match_targets(components, rule)
            for comp in targets:
                # 4) 조건 평가
                if not _evaluate_condition(rule.condition, comp):
                    # 조건 해소 → 기존 active 알람 자동 해제
                    _auto_resolve(db, rule, comp["name"], now)
                    continue

                # 5) cooldown 체크 — 최근 N초 이내 같은 규칙+소스 알람이 있으면 스킵
                recent = db.query(AlarmEvent).filter(
                    AlarmEvent.rule_id == rule.id,
                    AlarmEvent.source_name == comp["name"],
                    AlarmEvent.fired_at >= now - timedelta(seconds=rule.cooldown_sec),
                ).first()
                if recent:
                    continue

                # 6) 신규 알람 이벤트 생성
                msg = _build_message(rule, comp)
                event = AlarmEvent(
                    rule_id=rule.id,
                    rule_name=rule.name,
                    source_type=comp.get("type", ""),
                    source_name=comp["name"],
                    severity=rule.severity,
                    message=msg,
                    status="active",
                    fired_at=now,
                )
                db.add(event)
                fired_count += 1

                # 7) 알림 발송
                _dispatch(db, event)

        if fired_count > 0:
            db.commit()
            logger.info("알람 %d건 신규 발생", fired_count)
        else:
            db.commit()

    except Exception as e:
        db.rollback()
        logger.error("알람 평가 실패: %s", e)
    finally:
        db.close()


# ══════════════════════════════════════════════
# 조건 매칭 / 평가
# ══════════════════════════════════════════════

def _match_targets(components, rule):
    """규칙의 source_type/source_name에 맞는 컴포넌트 필터"""
    results = []
    for c in components:
        if rule.source_type != "*" and c.get("type") != rule.source_type:
            continue
        if rule.source_name != "*" and c.get("name") != rule.source_name:
            continue
        results.append(c)
    return results


def _evaluate_condition(condition, comp):
    """조건 문자열을 평가하여 True/False 반환

    지원 패턴:
      status == down
      status != healthy
      cpu > 90
      memory > 90
      disk > 90
    """
    cond = condition.strip()

    # status 비교
    m = re.match(r'^status\s*(==|!=)\s*(\w+)$', cond)
    if m:
        op, val = m.group(1), m.group(2)
        actual = comp.get("status", "")
        if op == "==":
            return actual == val
        else:
            return actual != val

    # 시스템 리소스 숫자 비교 (detail 문자열에서 추출)
    m = re.match(r'^(cpu|memory|disk)\s*(>|>=|<|<=)\s*(\d+\.?\d*)$', cond)
    if m:
        metric, op, threshold = m.group(1), m.group(2), float(m.group(3))
        value = _extract_metric(metric, comp)
        if value is None:
            return False
        if op == ">":
            return value > threshold
        if op == ">=":
            return value >= threshold
        if op == "<":
            return value < threshold
        if op == "<=":
            return value <= threshold

    # error_count > N
    m = re.match(r'^error_count\s*(>|>=)\s*(\d+)$', cond)
    if m:
        op, threshold = m.group(1), int(m.group(2))
        # detail에서 "오류" 키워드 포함 여부로 판단
        detail = comp.get("detail", "")
        if "오류" in detail:
            return True
        return False

    return False


def _extract_metric(metric, comp):
    """시스템 리소스 컴포넌트의 detail에서 CPU/메모리/디스크 수치 추출"""
    detail = comp.get("detail", "")
    patterns = {
        "cpu": r'CPU\s+([\d.]+)%',
        "memory": r'메모리\s+([\d.]+)%',
        "disk": r'디스크\s+([\d.]+)%',
    }
    p = patterns.get(metric)
    if not p:
        return None
    m = re.search(p, detail)
    if m:
        return float(m.group(1))
    return None


# ══════════════════════════════════════════════
# 메시지 생성 / 자동 해제
# ══════════════════════════════════════════════

def _build_message(rule, comp):
    """알람 메시지 생성"""
    if rule.message_template:
        return rule.message_template.replace("{name}", comp.get("name", "")).replace(
            "{detail}", comp.get("detail", ""))
    return "[%s] %s — %s" % (rule.severity.upper(), comp.get("name", ""), comp.get("detail", ""))


def _auto_resolve(db, rule, source_name, now):
    """조건이 해소된 active 알람을 자동 resolved 처리"""
    active = db.query(AlarmEvent).filter(
        AlarmEvent.rule_id == rule.id,
        AlarmEvent.source_name == source_name,
        AlarmEvent.status == "active",
    ).all()
    for ev in active:
        ev.status = "resolved"
        ev.resolved_at = now


# ══════════════════════════════════════════════
# 알림 발송
# ══════════════════════════════════════════════

def _dispatch(db, event):
    """알림 채널로 알람 발송"""
    channels = db.query(AlarmChannel).filter(AlarmChannel.enabled == True).all()  # noqa: E712
    for ch in channels:
        # 심각도 필터 확인
        sev_filter = ch.severity_filter or []
        if sev_filter and event.severity not in sev_filter:
            continue

        try:
            if ch.channel_type == "webhook":
                _send_webhook(ch, event)
            elif ch.channel_type == "email":
                _send_email(ch, event)
        except Exception as e:
            logger.warning("알림 발송 실패 [%s/%s]: %s", ch.channel_type, ch.name, e)


def _send_webhook(channel, event):
    """Webhook POST 발송"""
    url = (channel.config or {}).get("url", "")
    if not url:
        return
    payload = {
        "severity": event.severity,
        "source": event.source_name,
        "message": event.message,
        "fired_at": event.fired_at.isoformat() + "Z" if event.fired_at else "",
        "rule": event.rule_name,
    }
    requests.post(url, json=payload, timeout=5)


def _send_email(channel, event):
    """이메일 발송 (SMTP) — 설정이 있을 때만 동작"""
    import smtplib
    from email.mime.text import MIMEText

    cfg = channel.config or {}
    host = cfg.get("smtp_host", "")
    port = int(cfg.get("smtp_port", 587))
    user = cfg.get("smtp_user", "")
    password = cfg.get("smtp_password", "")
    sender = cfg.get("sender", user)
    recipients = cfg.get("recipients", [])

    if not host or not recipients:
        return

    subject = "[SDL 알람] [%s] %s" % (event.severity.upper(), event.source_name)
    body = "규칙: %s\n소스: %s\n메시지: %s\n발생시각: %s" % (
        event.rule_name, event.source_name, event.message,
        event.fired_at.strftime("%Y-%m-%d %H:%M:%S") if event.fired_at else "-",
    )

    msg = MIMEText(body, "plain", "utf-8")
    msg["Subject"] = subject
    msg["From"] = sender
    msg["To"] = ", ".join(recipients)

    with smtplib.SMTP(host, port, timeout=10) as smtp:
        if user and password:
            smtp.starttls()
            smtp.login(user, password)
        smtp.send_message(msg)


def send_test(channel):
    """채널 테스트 발송"""
    test_event = type("TestEvent", (), {
        "severity": "info",
        "source_name": "테스트",
        "message": "SDL 알람 시스템 테스트 메시지입니다.",
        "fired_at": datetime.utcnow(),
        "rule_name": "테스트 규칙",
    })()

    if channel.channel_type == "webhook":
        _send_webhook(channel, test_event)
    elif channel.channel_type == "email":
        _send_email(channel, test_event)
