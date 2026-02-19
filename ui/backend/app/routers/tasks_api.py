from __future__ import annotations
import json
from pathlib import Path
from fastapi import APIRouter, HTTPException, Query

from ..settings import settings
from ..utils import load_repo_config, get_output_paths
from ..store import TaskStore, Task, new_task_id
from ..runner import ProcessRunner

router = APIRouter(prefix="/tasks", tags=["tasks"])

def _runs_dir(repo_cfg) -> Path:
    out = get_output_paths(repo_cfg)
    runs_dir = Path(out.get("reports", str(settings.repo_root / "outputs" / "reports"))).parent / "config_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)
    return runs_dir

def _logs_dir(repo_cfg) -> Path:
    out = get_output_paths(repo_cfg)
    p = out.get("logs", str(settings.repo_root / "outputs" / "logs"))
    return Path(p)

def _ui_tasks_dir(repo_cfg) -> Path:
    out = get_output_paths(repo_cfg)
    base = Path(out.get("logs", str(settings.repo_root / "outputs" / "logs"))).parent / "ui_tasks"
    base.mkdir(parents=True, exist_ok=True)
    return base

def _build_main_cmd(repo_root: Path, python_bin: str, override: dict) -> list[str]:
    cmd = [python_bin, str(repo_root / "main.py"), "once"]

    # optional switches
    if override.get("platform"):
        cmd += ["--platform", override["platform"]]
    if override.get("rank_key"):
        cmd += ["--rank_key", override["rank_key"]]

    # pages: 起点优先 pages，否则 qidian_pages
    if override.get("platform") == "qidian":
        if override.get("pages") is not None:
            cmd += ["--pages", str(int(override["pages"]))]

    if override.get("qidian_pages") is not None:
        cmd += ["--qidian_pages", str(int(override["qidian_pages"]))]

    cmd += ["--chapter_count", str(int(override.get("chapter_count", 5)))]
    cmd += ["--newbook_chapter_count", str(int(override.get("newbook_chapter_count", 2)))]

    if override.get("no_detail"):
        cmd.append("--no_detail")
    if override.get("no_chapters"):
        cmd.append("--no_chapters")

    return cmd

@router.post("/spider")
def start_spider(run_id: str):
    repo_cfg = load_repo_config(settings.repo_root)
    runs_dir = _runs_dir(repo_cfg)
    run_path = runs_dir / f"{run_id}.json"
    if not run_path.exists():
        raise HTTPException(status_code=404, detail=f"config run not found: {run_id}")

    override = json.loads(run_path.read_text(encoding="utf-8"))
    cmd = _build_main_cmd(settings.repo_root, settings.python_bin, override)

    store = TaskStore(_ui_tasks_dir(repo_cfg))
    runner = ProcessRunner(store)

    task_id = new_task_id()
    log_path = _logs_dir(repo_cfg) / f"ui_{task_id}.log"

    task = Task(task_id=task_id, task_type="spider", status="queued", created_at=__import__("time").time(), config_run_id=run_id)
    store.upsert(task)
    runner.run_background(task=task, cmd=cmd, log_path=log_path)

    return {"task_id": task_id, "log_path": str(log_path), "command": cmd}

@router.get("")
def list_tasks():
    repo_cfg = load_repo_config(settings.repo_root)
    store = TaskStore(_ui_tasks_dir(repo_cfg))
    return {"tasks": [t.__dict__ for t in store.list()]}

@router.get("/{task_id}")
def get_task(task_id: str):
    repo_cfg = load_repo_config(settings.repo_root)
    store = TaskStore(_ui_tasks_dir(repo_cfg))
    t = store.get(task_id)
    if not t:
        raise HTTPException(status_code=404, detail="task not found")
    return t.__dict__

@router.get("/{task_id}/logs")
def get_logs(task_id: str, offset: int = Query(default=0, ge=0)):
    repo_cfg = load_repo_config(settings.repo_root)
    store = TaskStore(_ui_tasks_dir(repo_cfg))
    t = store.get(task_id)
    if not t or not t.log_path:
        raise HTTPException(status_code=404, detail="task/log not found")

    p = Path(t.log_path)
    if not p.exists():
        return {"offset": offset, "text": ""}

    data = p.read_bytes()
    if offset >= len(data):
        return {"offset": offset, "text": ""}

    chunk = data[offset:]
    try:
        text = chunk.decode("utf-8", errors="replace")
    except Exception:
        text = chunk.decode(errors="replace")
    return {"offset": len(data), "text": text}
