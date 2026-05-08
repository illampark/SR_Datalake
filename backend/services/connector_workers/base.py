"""BaseConnectorWorker — Modbus/OPC-UA 워커 공통 라이프사이클."""
import logging
import threading
import time
from datetime import datetime

from backend.database import SessionLocal
from backend.services import mqtt_manager

logger = logging.getLogger(__name__)


class BaseConnectorWorker(threading.Thread):
    connector_type = ""
    model = None
    META_REFRESH_SEC = 30

    def __init__(self, connector_id):
        super().__init__(
            daemon=True,
            name=f"{self.connector_type}-worker-{connector_id}",
        )
        self.connector_id = connector_id
        self._stop_evt = threading.Event()
        self._error_streak = 0
        self._client = None
        self._cached_meta = None

    # ── public ──
    def stop(self):
        self._stop_evt.set()

    # ── subclass hooks ──
    def _connect(self, meta):
        raise NotImplementedError

    def _close_client(self):
        if self._client is None:
            return
        try:
            self._client.close()
        except Exception:
            pass

    def _disconnect(self):
        try:
            self._close_client()
        finally:
            self._client = None

    def _read_all_tags(self, meta):
        """Return [(tag_name, value, dtype, unit, quality), ...]. May raise."""
        raise NotImplementedError

    def _snapshot_meta(self, c, tags):
        raise NotImplementedError

    # ── helpers ──
    def _load_meta(self):
        db = SessionLocal()
        try:
            c = db.query(self.model).get(self.connector_id)
            if c is None:
                return None
            tags = list(c.tags)
            return self._snapshot_meta(c, tags)
        finally:
            db.close()

    def _interval_sec(self):
        base_ms = 1000
        if self._cached_meta:
            base_ms = max(int(self._cached_meta.get("polling_interval", 1000)), 100)
        base = base_ms / 1000.0
        if self._error_streak == 0:
            return base
        # exponential backoff cap 60s
        return min(base * (2 ** min(self._error_streak, 6)), 60.0)

    def _update_stats(self, points_added=0, error=None):
        db = SessionLocal()
        try:
            c = db.query(self.model).get(self.connector_id)
            if not c:
                return
            if error is None:
                if points_added:
                    c.point_count = (c.point_count or 0) + points_added
                    c.last_collected_at = datetime.utcnow()
                if c.last_error:
                    c.last_error = ""
            else:
                c.error_count = (c.error_count or 0) + 1
                c.last_error = (str(error) or "")[:500]
            db.commit()
        except Exception:
            db.rollback()
        finally:
            db.close()

    # ── main loop ──
    def run(self):
        self._cached_meta = self._load_meta()
        if not self._cached_meta:
            logger.warning("[%s/%d] connector not found, exiting",
                           self.connector_type, self.connector_id)
            return

        logger.info("[%s/%d] worker started", self.connector_type, self.connector_id)
        meta_refresh_at = time.time()

        try:
            while not self._stop_evt.is_set():
                now = time.time()
                if now - meta_refresh_at > self.META_REFRESH_SEC:
                    m = self._load_meta()
                    if m:
                        self._cached_meta = m
                    meta_refresh_at = now

                meta = self._cached_meta
                try:
                    if self._client is None:
                        self._connect(meta)
                    values = self._read_all_tags(meta) or []
                    for tag_name, value, dtype, unit, quality in values:
                        try:
                            mqtt_manager.publish_raw(
                                self.connector_type, self.connector_id, tag_name, value,
                                data_type=dtype, unit=unit, quality=quality,
                            )
                        except Exception:
                            logger.exception("[%s/%d] publish failed: %s",
                                             self.connector_type, self.connector_id, tag_name)
                    if values:
                        self._update_stats(points_added=len(values))
                    self._error_streak = 0
                except Exception as e:
                    logger.warning("[%s/%d] poll error: %s",
                                   self.connector_type, self.connector_id, e)
                    self._update_stats(error=e)
                    self._error_streak += 1
                    self._disconnect()

                self._stop_evt.wait(self._interval_sec())
        finally:
            self._disconnect()
            logger.info("[%s/%d] worker stopped", self.connector_type, self.connector_id)
