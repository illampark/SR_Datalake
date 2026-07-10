"""File writer 시뮬레이터.

MinIO SFTP (기본 8022) 로 CSV 파일을 주기적으로 업로드.
파일 하나에 최근 write_interval_sec 초 동안의 스냅샷을 담아 업로드.
"""
import csv
import io
import logging
import time
from datetime import datetime, timezone

import paramiko

log = logging.getLogger("sim.file")


HEADER = ["ts", "asset_id", "temperature", "humidity", "pressure",
          "mode", "total_cycle", "last_error", "firmware_ver"]


def _sftp_client(cfg):
    host = cfg.get("minio_host", "sdl-minio")
    port = int(cfg.get("minio_sftp_port", 8022))
    user = cfg.get("minio_user", "sdladmin")
    password = cfg.get("minio_password", "")

    transport = paramiko.Transport((host, port))
    transport.connect(username=user, password=password)
    return paramiko.SFTPClient.from_transport(transport), transport


def _upload_csv(sftp, remote_path, rows):
    buf = io.StringIO()
    w = csv.writer(buf, quoting=csv.QUOTE_MINIMAL)
    w.writerow(HEADER)
    for s in rows:
        w.writerow([s.ts, s.asset_id, s.temperature, s.humidity, s.pressure,
                    s.mode, s.total_cycle, s.last_error, s.firmware_ver])
    data = buf.getvalue().encode("utf-8")
    with sftp.file(remote_path, "wb") as f:
        f.write(data)
    return len(data)


def _mkdirs(sftp, path):
    parts = [p for p in path.split("/") if p]
    cur = ""
    for p in parts:
        cur = cur + "/" + p if cur else p
        try:
            sftp.mkdir(cur)
        except IOError:
            pass  # 이미 존재하면 무시


def run(state, cfg):
    if not cfg.get("enabled", True):
        log.info("file disabled")
        return

    bucket = cfg.get("bucket", "sim-files")
    prefix = cfg.get("path_prefix", "press01/").rstrip("/") + "/"
    interval = float(cfg.get("write_interval_sec", 30.0))

    # 최근 스냅샷을 버퍼링 (다음 파일에 담음)
    buffered = []
    tick_period = 1.0  # 초당 하나씩 SimState.snapshot

    # 처음엔 연결 재시도
    sftp = transport = None
    while sftp is None:
        try:
            sftp, transport = _sftp_client(cfg)
            log.info("file sftp connected to %s", cfg.get("minio_host"))
            # 버킷·prefix 폴더 준비
            try:
                sftp.mkdir(bucket)
            except IOError:
                pass
            _mkdirs(sftp, f"{bucket}/{prefix.rstrip('/')}")
        except Exception as e:
            log.warning("sftp connect fail (%s), retry in 5s", e)
            time.sleep(5)

    next_flush = time.time() + interval
    while True:
        buffered.append(state.snapshot())
        now = time.time()
        if now >= next_flush and buffered:
            fname = datetime.now(timezone.utc).strftime("press01_%Y%m%dT%H%M%SZ.csv")
            remote = f"{bucket}/{prefix}{fname}"
            try:
                size = _upload_csv(sftp, remote, buffered)
                log.info("uploaded %s (%d bytes, %d rows)", remote, size, len(buffered))
            except Exception as e:
                log.warning("sftp upload error: %s; reconnecting", e)
                try:
                    transport.close()
                except Exception:
                    pass
                sftp = transport = None
                while sftp is None:
                    try:
                        sftp, transport = _sftp_client(cfg)
                    except Exception as e2:
                        log.warning("sftp reconnect fail (%s)", e2)
                        time.sleep(5)
            buffered.clear()
            next_flush = time.time() + interval
        time.sleep(tick_period)
