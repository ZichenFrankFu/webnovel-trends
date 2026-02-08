# tasks/scheduler.py
from __future__ import annotations

import os
import sys
import time
import schedule
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta
from pathlib import Path
from typing import Optional, Callable
from tasks.run_spiders_once import run_once

# Ensure project root import works no matter where you run from
PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

import config

@dataclass
class SchedulerConfig:
    """
    Daily scheduler configuration.

    - run_time: local HH:MM (e.g. "03:30")
    - retry_attempts: how many additional retries if the run fails
    - retry_backoff_sec: base backoff seconds; actual delay is backoff * attempt_index
    - jitter_sec: small jitter to avoid always hitting exact second
    """
    run_time: str = "03:30"
    retry_attempts: int = 2
    retry_backoff_sec: int = 120
    jitter_sec: int = 5


def _parse_hhmm(hhmm: str) -> tuple[int, int]:
    hhmm = (hhmm or "").strip()
    if ":" not in hhmm:
        raise ValueError(f"Invalid run_time={hhmm!r}, expected 'HH:MM'")
    h, m = hhmm.split(":", 1)
    return int(h), int(m)


def _next_run_at(run_time: str, now: Optional[datetime] = None) -> datetime:
    now = now or datetime.now()
    h, m = _parse_hhmm(run_time)
    candidate = now.replace(hour=h, minute=m, second=0, microsecond=0)
    if candidate <= now:
        candidate = candidate + timedelta(days=1)
    return candidate


def _sleep_until(target: datetime, *, logger: logging.Logger, jitter_sec: int = 0):
    while True:
        now = datetime.now()
        remaining = (target - now).total_seconds()
        if remaining <= 0:
            break

        # sleep in chunks so Ctrl+C reacts fast
        chunk = min(remaining, 60)
        time.sleep(max(0.0, chunk))

    if jitter_sec > 0:
        time.sleep(min(jitter_sec, 5))


def _build_logger() -> logging.Logger:
    logs_dir = PROJECT_ROOT / "outputs" / "logs"
    logs_dir.mkdir(parents=True, exist_ok=True)
    log_path = logs_dir / "scheduler.log"

    logger = logging.getLogger("webnovel_trends_scheduler")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()

    fmt = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")

    # Console
    sh = logging.StreamHandler()
    sh.setFormatter(fmt)
    logger.addHandler(sh)

    # File
    fh = logging.FileHandler(str(log_path), encoding="utf-8")
    fh.setFormatter(fmt)
    logger.addHandler(fh)

    logger.info(f"[scheduler] log file: {log_path}")
    logger.info(f"[scheduler] project root: {PROJECT_ROOT}")
    logger.info(f"[scheduler] config file: {getattr(config, '__file__', '<unknown>')}")
    return logger


class TaskScheduler:
    """
    Minimal daily scheduler.

    Usage:
      - from main.py: TaskScheduler().start()
      - or directly: python tasks/scheduler.py --time 03:30
    """

    def __init__(self, job: Optional[Callable[[], None]] = None) -> None:
        self.job = job or run_once

    def run_once(self) -> None:
        self.job()

    def run_forever(self, interval_minutes: int = 60) -> None:
        schedule.every(interval_minutes).minutes.do(self.job)
        while True:
            schedule.run_pending()
            time.sleep(1)


def _cli():
    import argparse

    p = argparse.ArgumentParser(description="WebNovel Trends daily scheduler")
    p.add_argument("--time", default="03:30", help="daily run time in HH:MM (local)")
    p.add_argument("--retries", type=int, default=2, help="retry attempts on failure")
    p.add_argument("--backoff", type=int, default=120, help="base backoff seconds")
    args = p.parse_args()

    sched = TaskScheduler(
        SchedulerConfig(
            run_time=args.time,
            retry_attempts=args.retries,
            retry_backoff_sec=args.backoff,
        )
    )
    sched.start()


if __name__ == "__main__":
    _cli()
