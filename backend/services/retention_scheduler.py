"""
Retention Scheduler Service.

Background thread that checks Korean schedule strings and
auto-executes retention transitions at configured times.
Follows the file_scanner.py threading pattern.
"""
import logging
import threading
from datetime import datetime

from backend.database import SessionLocal
from backend.models.storage import RetentionPolicy
from backend.services.retention_executor import (
    execute_downsampling, execute_archiving, execute_cold_expiry,
)

logger = logging.getLogger(__name__)

# Module-level state (file_scanner.py pattern)
_scheduler_thread = None
_stop_event = None
_last_executed = {
    "hot_to_warm": None,
    "warm_to_cold": None,
    "expiry_cleanup": None,
}

# 프론트엔드 select option 값 → 스케줄 파라미터 매핑
_SCHEDULE_MAP = {
    # Hot → Warm
    "매일 자정":            {"freq": "daily",   "hour": 0, "minute": 0, "weekday": None, "day": None},
    "매일 새벽 3시":        {"freq": "daily",   "hour": 3, "minute": 0, "weekday": None, "day": None},
    "매주 일요일 새벽 3시": {"freq": "weekly",  "hour": 3, "minute": 0, "weekday": 6,    "day": None},
    # Warm → Cold
    "매일 새벽 4시":        {"freq": "daily",   "hour": 4, "minute": 0, "weekday": None, "day": None},
    "매주 일요일 새벽 4시": {"freq": "weekly",  "hour": 4, "minute": 0, "weekday": 6,    "day": None},
    "매월 1일 새벽 4시":    {"freq": "monthly", "hour": 4, "minute": 0, "weekday": None, "day": 1},
    # Expiry cleanup
    "매일":                 {"freq": "daily",   "hour": 2, "minute": 0, "weekday": None, "day": None},
    "매주":                 {"freq": "weekly",  "hour": 2, "minute": 0, "weekday": 0,    "day": None},
    "매월":                 {"freq": "monthly", "hour": 2, "minute": 0, "weekday": None, "day": 1},
}


# ── Public API ──────────────────────────────────────


def start_scheduler():
    """스케줄러 백그라운드 스레드 시작."""
    global _scheduler_thread, _stop_event

    if _scheduler_thread and _scheduler_thread.is_alive():
        logger.info("Retention scheduler already running")
        return True

    _stop_event = threading.Event()
    _scheduler_thread = threading.Thread(
        target=_scheduler_loop,
        args=(_stop_event,),
        daemon=True,
        name="retention-scheduler",
    )
    _scheduler_thread.start()
    logger.info("Retention scheduler thread started")
    return True


def stop_scheduler():
    """스케줄러 백그라운드 스레드 중지."""
    global _scheduler_thread, _stop_event

    if _stop_event:
        _stop_event.set()
    if _scheduler_thread:
        _scheduler_thread.join(timeout=10)
        _scheduler_thread = None
    _stop_event = None
    logger.info("Retention scheduler stopped")


def is_scheduler_running():
    """스케줄러 실행 여부 확인."""
    return _scheduler_thread is not None and _scheduler_thread.is_alive()


def get_scheduler_status():
    """스케줄러 상태 정보 반환."""
    return {
        "running": is_scheduler_running(),
        "last_executed": dict(_last_executed),
    }


# ── Schedule Parsing ────────────────────────────────


def _should_execute_now(schedule_str, now=None):
    """현재 시각이 스케줄과 일치하는지 확인. 매 분마다 호출."""
    if now is None:
        now = datetime.now()

    sched = _SCHEDULE_MAP.get(schedule_str)
    if not sched:
        logger.warning("Unknown schedule string: %s", schedule_str)
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


# ── Scheduler Loop ──────────────────────────────────


def _scheduler_loop(stop_event):
    """메인 스케줄러 루프. 60초마다 스케줄 확인."""
    logger.info("Retention scheduler loop started")

    while not stop_event.is_set():
        try:
            db = SessionLocal()
            try:
                policy = db.query(RetentionPolicy).first()
            finally:
                db.close()

            if policy:
                now = datetime.now()
                now_key = now.strftime("%Y-%m-%d %H:%M")

                # Hot → Warm (downsampling)
                if (_should_execute_now(policy.hot_to_warm_schedule, now)
                        and _last_executed.get("hot_to_warm") != now_key):
                    logger.info("Scheduler: executing downsampling")
                    _last_executed["hot_to_warm"] = now_key
                    try:
                        execute_downsampling()
                    except Exception as e:
                        logger.error("Scheduled downsampling failed: %s", e)

                # Warm → Cold (archiving)
                if (_should_execute_now(policy.warm_to_cold_schedule, now)
                        and _last_executed.get("warm_to_cold") != now_key):
                    logger.info("Scheduler: executing archiving")
                    _last_executed["warm_to_cold"] = now_key
                    try:
                        execute_archiving()
                    except Exception as e:
                        logger.error("Scheduled archiving failed: %s", e)

                # Cold expiry (auto_delete_enabled 체크)
                if (policy.auto_delete_enabled
                        and _should_execute_now(policy.expiry_cleanup_schedule, now)
                        and _last_executed.get("expiry_cleanup") != now_key):
                    logger.info("Scheduler: executing cold expiry")
                    _last_executed["expiry_cleanup"] = now_key
                    try:
                        execute_cold_expiry()
                    except Exception as e:
                        logger.error("Scheduled cold expiry failed: %s", e)

        except Exception as e:
            logger.error("Scheduler loop error: %s", e)

        stop_event.wait(60)

    logger.info("Retention scheduler loop ended")
