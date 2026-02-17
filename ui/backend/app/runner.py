from __future__ import annotations
import subprocess
import threading
import time
from pathlib import Path
from typing import List, Optional

from .store import Task, TaskStore

class ProcessRunner:
    def __init__(self, store: TaskStore):
        self.store = store

    def run_background(self, *, task: Task, cmd: List[str], log_path: Path) -> None:
        # 更新任务为 running
        task.status = "running"
        task.started_at = time.time()
        task.command = cmd
        task.log_path = str(log_path)
        self.store.upsert(task)

        def _worker():
            log_path.parent.mkdir(parents=True, exist_ok=True)
            with log_path.open("w", encoding="utf-8") as lf:
                try:
                    p = subprocess.Popen(
                        cmd,
                        stdout=lf,
                        stderr=subprocess.STDOUT,
                        cwd=str(Path(cmd[0]).resolve().parent) if False else None,
                        text=True,
                    )
                    code = p.wait()
                    task.exit_code = code
                    task.status = "succeeded" if code == 0 else "failed"
                    task.ended_at = time.time()
                    self.store.upsert(task)
                except Exception as e:
                    lf.write(f"\n[ui-runner] exception: {e}\n")
                    task.status = "failed"
                    task.ended_at = time.time()
                    self.store.upsert(task)

        th = threading.Thread(target=_worker, daemon=True)
        th.start()
