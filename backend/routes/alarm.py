"""알람 관리 API — 이벤트 조회/관리, 규칙 CRUD, 채널 CRUD, 수동 평가"""

import logging
from datetime import datetime

from flask import Blueprint, jsonify, request

from backend.database import SessionLocal
from backend.models.alarm import AlarmRule, AlarmEvent, AlarmChannel
from backend.services import alarm_engine
from backend.services.audit_logger import audit_route

logger = logging.getLogger(__name__)

alarm_bp = Blueprint("alarm", __name__, url_prefix="/api/alarm")


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


# ══════════════════════════════════════════════
# 알람 이벤트
# ══════════════════════════════════════════════

@alarm_bp.route("/events", methods=["GET"])
def list_events():
    """알람 이벤트 목록 (최신순, 최대 200건)"""
    db = SessionLocal()
    try:
        q = db.query(AlarmEvent)

        severity = request.args.get("severity")
        if severity and severity != "all":
            q = q.filter(AlarmEvent.severity == severity)

        status = request.args.get("status")
        if status and status != "all":
            q = q.filter(AlarmEvent.status == status)

        events = q.order_by(AlarmEvent.fired_at.desc()).limit(200).all()

        items = []
        for e in events:
            items.append({
                "id": e.id,
                "rule_id": e.rule_id,
                "rule_name": e.rule_name,
                "source_type": e.source_type,
                "source_name": e.source_name,
                "severity": e.severity,
                "message": e.message,
                "status": e.status,
                "fired_at": e.fired_at.isoformat() + "Z" if e.fired_at else None,
                "acked_at": e.acked_at.isoformat() + "Z" if e.acked_at else None,
                "resolved_at": e.resolved_at.isoformat() + "Z" if e.resolved_at else None,
            })

        # 요약
        all_events = db.query(AlarmEvent).filter(AlarmEvent.status == "active").all()
        summary = {"active": 0, "critical": 0, "warning": 0, "info": 0}
        for e in all_events:
            summary["active"] += 1
            if e.severity in summary:
                summary[e.severity] += 1

        return _ok({"events": items, "summary": summary})
    finally:
        db.close()


@alarm_bp.route("/events/<int:event_id>/ack", methods=["POST"])
@audit_route("alarm", "alarm.event.ack", target_type="alarm_event",
             target_name_kwarg="event_id")
def ack_event(event_id):
    db = SessionLocal()
    try:
        ev = db.query(AlarmEvent).get(event_id)
        if not ev:
            return _err("이벤트를 찾을 수 없습니다", "NOT_FOUND", 404)
        ev.status = "acknowledged"
        ev.acked_at = datetime.utcnow()
        db.commit()
        return _ok({"id": ev.id, "status": ev.status})
    finally:
        db.close()


@alarm_bp.route("/events/<int:event_id>/resolve", methods=["POST"])
@audit_route("alarm", "alarm.event.resolve", target_type="alarm_event",
             target_name_kwarg="event_id")
def resolve_event(event_id):
    db = SessionLocal()
    try:
        ev = db.query(AlarmEvent).get(event_id)
        if not ev:
            return _err("이벤트를 찾을 수 없습니다", "NOT_FOUND", 404)
        ev.status = "resolved"
        ev.resolved_at = datetime.utcnow()
        db.commit()
        return _ok({"id": ev.id, "status": ev.status})
    finally:
        db.close()


@alarm_bp.route("/events/ack-all", methods=["POST"])
@audit_route("alarm", "alarm.event.ack_all", target_type="alarm_event")
def ack_all_events():
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        updated = db.query(AlarmEvent).filter(
            AlarmEvent.status == "active"
        ).update({"status": "acknowledged", "acked_at": now})
        db.commit()
        return _ok({"updated": updated})
    finally:
        db.close()


# ══════════════════════════════════════════════
# 알람 규칙 CRUD
# ══════════════════════════════════════════════

@alarm_bp.route("/rules", methods=["GET"])
def list_rules():
    db = SessionLocal()
    try:
        rules = db.query(AlarmRule).order_by(AlarmRule.id).all()
        items = []
        for r in rules:
            items.append({
                "id": r.id,
                "name": r.name,
                "enabled": r.enabled,
                "source_type": r.source_type,
                "source_name": r.source_name,
                "condition": r.condition,
                "severity": r.severity,
                "cooldown_sec": r.cooldown_sec,
                "message_template": r.message_template or "",
                "created_at": r.created_at.isoformat() + "Z" if r.created_at else None,
            })
        return _ok(items)
    finally:
        db.close()


@alarm_bp.route("/rules", methods=["POST"])
@audit_route("alarm", "alarm.rule.create", target_type="alarm_rule",
             detail_keys=["name", "severity", "metric", "tagName", "comparator", "threshold", "enabled"])
def create_rule():
    db = SessionLocal()
    try:
        d = request.get_json(force=True)
        rule = AlarmRule(
            name=d.get("name", ""),
            enabled=d.get("enabled", True),
            source_type=d.get("source_type", "*"),
            source_name=d.get("source_name", "*"),
            condition=d.get("condition", ""),
            severity=d.get("severity", "warning"),
            cooldown_sec=int(d.get("cooldown_sec", 300)),
            message_template=d.get("message_template", ""),
        )
        if not rule.name or not rule.condition:
            return _err("규칙명과 조건은 필수입니다")
        db.add(rule)
        db.commit()
        return _ok({"id": rule.id, "name": rule.name})
    finally:
        db.close()


@alarm_bp.route("/rules/<int:rule_id>", methods=["PUT"])
@audit_route("alarm", "alarm.rule.update", target_type="alarm_rule",
             target_name_kwarg="rule_id",
             detail_keys=["name", "severity", "metric", "tagName", "comparator", "threshold", "enabled"])
def update_rule(rule_id):
    db = SessionLocal()
    try:
        rule = db.query(AlarmRule).get(rule_id)
        if not rule:
            return _err("규칙을 찾을 수 없습니다", "NOT_FOUND", 404)
        d = request.get_json(force=True)
        for key in ("name", "enabled", "source_type", "source_name",
                     "condition", "severity", "cooldown_sec", "message_template"):
            if key in d:
                setattr(rule, key, d[key])
        db.commit()
        return _ok({"id": rule.id})
    finally:
        db.close()


@alarm_bp.route("/rules/<int:rule_id>", methods=["DELETE"])
@audit_route("alarm", "alarm.rule.delete", target_type="alarm_rule",
             target_name_kwarg="rule_id")
def delete_rule(rule_id):
    db = SessionLocal()
    try:
        rule = db.query(AlarmRule).get(rule_id)
        if not rule:
            return _err("규칙을 찾을 수 없습니다", "NOT_FOUND", 404)
        db.delete(rule)
        db.commit()
        return _ok({"deleted": rule_id})
    finally:
        db.close()


@alarm_bp.route("/rules/<int:rule_id>/toggle", methods=["POST"])
@audit_route("alarm", "alarm.rule.toggle", target_type="alarm_rule",
             target_name_kwarg="rule_id")
def toggle_rule(rule_id):
    db = SessionLocal()
    try:
        rule = db.query(AlarmRule).get(rule_id)
        if not rule:
            return _err("규칙을 찾을 수 없습니다", "NOT_FOUND", 404)
        rule.enabled = not rule.enabled
        db.commit()
        return _ok({"id": rule.id, "enabled": rule.enabled})
    finally:
        db.close()


# ══════════════════════════════════════════════
# 알림 채널 CRUD
# ══════════════════════════════════════════════

@alarm_bp.route("/channels", methods=["GET"])
def list_channels():
    db = SessionLocal()
    try:
        channels = db.query(AlarmChannel).order_by(AlarmChannel.id).all()
        items = []
        for ch in channels:
            items.append({
                "id": ch.id,
                "name": ch.name,
                "enabled": ch.enabled,
                "channel_type": ch.channel_type,
                "config": ch.config or {},
                "severity_filter": ch.severity_filter or [],
                "created_at": ch.created_at.isoformat() + "Z" if ch.created_at else None,
            })
        return _ok(items)
    finally:
        db.close()


@alarm_bp.route("/channels", methods=["POST"])
@audit_route("alarm", "alarm.channel.create", target_type="alarm_channel",
             detail_keys=["name", "channelType", "enabled"])
def create_channel():
    db = SessionLocal()
    try:
        d = request.get_json(force=True)
        ch = AlarmChannel(
            name=d.get("name", ""),
            enabled=d.get("enabled", True),
            channel_type=d.get("channel_type", "webhook"),
            config=d.get("config", {}),
            severity_filter=d.get("severity_filter", ["critical", "warning"]),
        )
        if not ch.name:
            return _err("채널명은 필수입니다")
        db.add(ch)
        db.commit()
        return _ok({"id": ch.id, "name": ch.name})
    finally:
        db.close()


@alarm_bp.route("/channels/<int:channel_id>", methods=["PUT"])
@audit_route("alarm", "alarm.channel.update", target_type="alarm_channel",
             target_name_kwarg="channel_id",
             detail_keys=["name", "channelType", "enabled"])
def update_channel(channel_id):
    db = SessionLocal()
    try:
        ch = db.query(AlarmChannel).get(channel_id)
        if not ch:
            return _err("채널을 찾을 수 없습니다", "NOT_FOUND", 404)
        d = request.get_json(force=True)
        for key in ("name", "enabled", "channel_type", "config", "severity_filter"):
            if key in d:
                setattr(ch, key, d[key])
        db.commit()
        return _ok({"id": ch.id})
    finally:
        db.close()


@alarm_bp.route("/channels/<int:channel_id>", methods=["DELETE"])
@audit_route("alarm", "alarm.channel.delete", target_type="alarm_channel",
             target_name_kwarg="channel_id")
def delete_channel(channel_id):
    db = SessionLocal()
    try:
        ch = db.query(AlarmChannel).get(channel_id)
        if not ch:
            return _err("채널을 찾을 수 없습니다", "NOT_FOUND", 404)
        db.delete(ch)
        db.commit()
        return _ok({"deleted": channel_id})
    finally:
        db.close()


@alarm_bp.route("/channels/<int:channel_id>/test", methods=["POST"])
@audit_route("alarm", "alarm.channel.test", target_type="alarm_channel",
             target_name_kwarg="channel_id")
def test_channel(channel_id):
    db = SessionLocal()
    try:
        ch = db.query(AlarmChannel).get(channel_id)
        if not ch:
            return _err("채널을 찾을 수 없습니다", "NOT_FOUND", 404)
        alarm_engine.send_test(ch)
        return _ok({"message": "테스트 발송 완료"})
    except Exception as e:
        return _err("테스트 발송 실패: %s" % str(e))
    finally:
        db.close()


# ══════════════════════════════════════════════
# 수동 평가
# ══════════════════════════════════════════════

@alarm_bp.route("/evaluate", methods=["POST"])
@audit_route("alarm", "alarm.evaluate", target_type="")
def manual_evaluate():
    try:
        alarm_engine.evaluate()
        return _ok({"message": "평가 완료"})
    except Exception as e:
        return _err("평가 실패: %s" % str(e))
