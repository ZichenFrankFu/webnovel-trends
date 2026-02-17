from __future__ import annotations
import json
import time
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Optional, Dict, Any, List

@dataclass
class Task:
    task_id: str
    task_type: str              # "spider"
    status: str                 # "queued"|"running"|"succeeded"|"failed"
    created_at: float
    started_at: Optional[float] = None
    ended_at: Optional[float] = None
    config_run_id: Optional[str] = None
    command: Optional[list[str]] = None
    log_path: Optional[str] = None
    exit_code: Optional[int] = None

class TaskStore:
    def __init__(self, base_dir: Path):
        self.base_dir = base_dir
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.tasks_file = self.base_dir / "tasks.jsonl"

    def _read_all(self) -> List[Task]:
        if not self.tasks_file.exists():
            return []
        tasks: List[Task] = []
        with self.tasks_file.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                obj = json.loads(line)
                tasks.append(Task(**obj))
        return tasks

    def list(self) -> List[Task]:
        return sorted(self._read_all(), key=lambda t: t.created_at, reverse=True)

    def get(self, task_id: str) -> Optional[Task]:
        for t in self._read_all():
            if t.task_id == task_id:
                return t
        return None

    def upsert(self, task: Task) -> None:
        tasks = self._read_all()
        found = False
        for i, t in enumerate(tasks):
            if t.task_id == task.task_id:
                tasks[i] = task
                found = True
                break
        if not found:
            tasks.append(task)
        with self.tasks_file.open("w", encoding="utf-8") as f:
            for t in tasks:
                f.write(json.dumps(asdict(t), ensure_ascii=False) + "\n")

def new_task_id() -> str:
    return f"t{int(time.time()*1000)}"
