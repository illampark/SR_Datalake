"""공지사항 API — CRUD"""

import logging
from datetime import datetime

from flask import Blueprint, jsonify, request, session

from backend.database import SessionLocal
from backend.models.notice import Notice

logger = logging.getLogger(__name__)

notice_bp = Blueprint("notice", __name__, url_prefix="/api/notice")


def _ok(data=None):
    return jsonify({"success": True, "data": data, "error": None})


def _err(msg, code="ERROR", status=400):
    return jsonify({"success": False, "data": None,
                    "error": {"code": code, "message": msg}}), status


def _is_admin():
    return session.get("role") == "admin"


# ── NOC-001: GET /api/notice ──
@notice_bp.route("", methods=["GET"])
def list_notices():
    """공지 목록 — 핀 우선, 최신순, 만료되지 않은 항목만 (최대 20건)"""
    db = SessionLocal()
    try:
        now = datetime.utcnow()
        q = db.query(Notice).filter(
            (Notice.expires_at == None) | (Notice.expires_at > now)  # noqa: E711
        ).order_by(
            Notice.is_pinned.desc(),
            Notice.created_at.desc()
        ).limit(20)
        items = [n.to_dict() for n in q.all()]
        return _ok(items)
    finally:
        db.close()


# ── NOC-002: POST /api/notice ──
@notice_bp.route("", methods=["POST"])
def create_notice():
    """공지 작성 (admin 전용)"""
    if not _is_admin():
        return _err("관리자만 공지를 작성할 수 있습니다.", "FORBIDDEN", 403)

    body = request.get_json(silent=True) or {}
    title   = (body.get("title") or "").strip()
    content = (body.get("content") or "").strip()
    if not title:
        return _err("제목을 입력하세요.", "VALIDATION")
    if not content:
        return _err("내용을 입력하세요.", "VALIDATION")

    category  = body.get("category", "general")
    if category not in ("general", "maintenance", "urgent"):
        category = "general"
    is_pinned = bool(body.get("isPinned", False))
    author    = session.get("display_name") or session.get("username") or "admin"

    expires_at = None
    if body.get("expiresAt"):
        try:
            expires_at = datetime.fromisoformat(body["expiresAt"].replace("Z", "+00:00").replace("+00:00", ""))
        except ValueError:
            pass

    db = SessionLocal()
    try:
        notice = Notice(
            title=title,
            content=content,
            category=category,
            is_pinned=is_pinned,
            author=author,
            expires_at=expires_at,
        )
        db.add(notice)
        db.commit()
        db.refresh(notice)
        return _ok(notice.to_dict())
    finally:
        db.close()


# ── NOC-003: GET /api/notice/<id> ──
@notice_bp.route("/<int:nid>", methods=["GET"])
def get_notice(nid):
    """공지 단건 조회"""
    db = SessionLocal()
    try:
        notice = db.query(Notice).filter(Notice.id == nid).first()
        if not notice:
            return _err("공지를 찾을 수 없습니다.", "NOT_FOUND", 404)
        return _ok(notice.to_dict())
    finally:
        db.close()


# ── NOC-004: PUT /api/notice/<id> ──
@notice_bp.route("/<int:nid>", methods=["PUT"])
def update_notice(nid):
    """공지 수정 (admin 전용)"""
    if not _is_admin():
        return _err("관리자만 공지를 수정할 수 있습니다.", "FORBIDDEN", 403)

    body = request.get_json(silent=True) or {}
    db = SessionLocal()
    try:
        notice = db.query(Notice).filter(Notice.id == nid).first()
        if not notice:
            return _err("공지를 찾을 수 없습니다.", "NOT_FOUND", 404)

        if "title" in body:
            title = (body["title"] or "").strip()
            if not title:
                return _err("제목을 입력하세요.", "VALIDATION")
            notice.title = title
        if "content" in body:
            content = (body["content"] or "").strip()
            if not content:
                return _err("내용을 입력하세요.", "VALIDATION")
            notice.content = content
        if "category" in body:
            cat = body["category"]
            notice.category = cat if cat in ("general", "maintenance", "urgent") else "general"
        if "isPinned" in body:
            notice.is_pinned = bool(body["isPinned"])
        if "expiresAt" in body:
            if body["expiresAt"]:
                try:
                    notice.expires_at = datetime.fromisoformat(
                        body["expiresAt"].replace("Z", "+00:00").replace("+00:00", ""))
                except ValueError:
                    pass
            else:
                notice.expires_at = None

        notice.updated_at = datetime.utcnow()
        db.commit()
        db.refresh(notice)
        return _ok(notice.to_dict())
    finally:
        db.close()


# ── NOC-005: DELETE /api/notice/<id> ──
@notice_bp.route("/<int:nid>", methods=["DELETE"])
def delete_notice(nid):
    """공지 삭제 (admin 전용)"""
    if not _is_admin():
        return _err("관리자만 공지를 삭제할 수 있습니다.", "FORBIDDEN", 403)

    db = SessionLocal()
    try:
        notice = db.query(Notice).filter(Notice.id == nid).first()
        if not notice:
            return _err("공지를 찾을 수 없습니다.", "NOT_FOUND", 404)
        db.delete(notice)
        db.commit()
        return _ok({"id": nid})
    finally:
        db.close()
