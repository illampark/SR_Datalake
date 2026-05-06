"""
Backup Scheduler Service.

Background thread that checks backup schedule from AdminSetting KV
and auto-executes backups at configured times.
Follows the retention_scheduler.py threading pattern.
"""

import logging
import threading
from datetime import datetime

from sqlalchemy import text as _sql_text

from backend.database import SessionLocal
from backend.models.user import AdminSetting
from backend.services.backup_executor import execute_backup

logger = logging.getLogger(__name__)

# Module-level state
_scheduler_thread = None
_stop_event = None
_last_executed = None

# 멀티 워커 환경에서 단일 워커만 실제 백업을 수행하도록 advisory lock key.
# 임의 고정 int64. 다른 모듈과 충돌하지 않는 값으로 선택.
_BACKUP_LOCK_KEY = 0x53444C5F4255  # "SDL_BU"

# 스케줄 문자열 → 실행 파라미터 매핑
_SCHEDULE_MAP = {
    "매일 새벽 2시":         {"freq": "daily",   "hour": 2, "minute": 0, "weekday": None, "day": None},
    "매일 새벽 3시":         {"freq": "daily",   "hour": 3, "minute": 0, "weekday": None, "day": None},
    "매일 새벽 4시":         {"freq": "daily",   "hour": 4, "minute": 0, "weekday": None, "day": None},
    "매주 일요일 새벽 3시":  {"freq": "weekly",  "hour": 3, "minute": 0, "weekday": 6,    "day": None},
    "매주 일요일 새벽 4시":  {"freq": "weekly",  "hour": 4, "minute": 0, "weekday": 6,    "day": None},
    "매월 1일 새벽 3시":     {"freq": "monthly", "hour": 3, "minute": 0, "weekday": None, "day": 1},
}


# ── Public API ──────────────────────────────────────


def start_scheduler():
    """백업 스케줄러 백그라운드 스레드 시작."""
    global _scheduler_thread, _stop_event

    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.info("Backup scheduler already running")
        return True

    _stop_event = threading.Event()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(_stop_event,),
        daemon=True,
        name="backup-scheduler",
    )
    _scheduler_thread.start()
    logger.info("Backup scheduler thread started")
    return True


def stop_scheduler():
    """백업 스케줄러 스레드 중지."""
    global _scheduler_thread, _stop_event

    if _stop_event:
        _stop_event.set()
    if _scheduler_thread:
        _scheduler_thread.join(timeout=10)
        _scheduler_thread = None
    _stop_event = None
    logger.info("Backup scheduler stopped")


def is_scheduler_running():
    """스케줄러 실행 여부."""
    return _scheduler_thread is not None and _scheduler_thread.is_alive()


def get_scheduler_status():
    """스케줄러 상태 정보."""
    return {
        "running": is_scheduler_running(),
        "lastExecuted": _last_executed,
    }


# ── Schedule Parsing ────────────────────────────────


def _should_execute_now(schedule_str, now=None):
    """현재 시각이 스케줄과 일치하는지 확인."""
    if now is None:
        now = datetime.now()

    sched = _SCHEDULE_MAP.get(schedule_str)
    if not sched:
        return False

    if now.hour != sched["hour"] or now.minute != sched["minute"]:
        return False

    if sched["freq"] == "daily":
        return True
    elif sched["freq"] == "weekly":
        return now.weekday() == sched["weekday"]
    elif sched["freq"] == "monthly":
        return now.day == sched["day"]

    return False


def get_schedule_options():
    """프론트엔드용 스케줄 옵션 목록."""
    return list(_SCHEDULE_MAP.keys())


# ── Scheduler Loop ──────────────────────────────────


def _scheduler_loop(stop_event):
    """메인 스케줄러 루프. 60초마다 스케줄 확인."""
    global _last_executed
    logger.info("Backup scheduler loop started")

    while not stop_event.is_set():
        try:
            # AdminSetting에서 스케줄 읽기
            db = SessionLocal()
            try:
                schedule_row = db.query(AdminSetting).filter(
                    AdminSetting.key == "backup.schedule"
                ).first()
                targets_row = db.query(AdminSetting).filter(
                    AdminSetting.key == "backup.targets"
                ).first()
            finally:
                db.close()

            schedule_str = schedule_row.value if schedule_row else "매일 새벽 3시"
            targets_str = targets_row.value if targets_row else "postgresql,config"

            now = datetime.now()
            now_key = now.strftime("%Y-%m-%d %H:%M")

            if _should_execute_now(schedule_str, now) and _last_executed != now_key:
                _last_executed = now_key
                targets = [t.strip() for t in targets_str.split(",") if t.strip()]
                # Postgres advisory lock 으로 멀티 워커 중 1개만 실제 백업 수행.
                # 나머지 워커는 lock 획득 실패 → 로그만 남기고 skip (실패 레코드 미생성).
                lock_db = SessionLocal()
                try:
                    got_lock = lock_db.execute(
                        _sql_text("SELECT pg_try_advisory_lock(:k)"),
                        {"k": _BACKUP_LOCK_KEY},
                    ).scalar()
                    if not got_lock:
                        logger.info(
                            "Backup scheduler: another worker holds the lock — skip"
                        )
                    else:
                        try:
                            logger.info("Backup scheduler: executing scheduled backup")
                            execute_backup(targets, "scheduled")
                        except Exception as e:
                            logger.error("Scheduled backup failed: %s", e)
                        finally:
                            try:
                                lock_db.execute(
                                    _sql_text("SELECT pg_advisory_unlock(:k)"),
                                    {"k": _BACKUP_LOCK_KEY},
                                )
                                lock_db.commit()
                            except Exception:
                                lock_db.rollback()
                finally:
                    lock_db.close()

        except Exception as e:
            logger.error("Backup scheduler loop error: %s", e)

        stop_event.wait(60)

    logger.info("Backup scheduler loop ended")
