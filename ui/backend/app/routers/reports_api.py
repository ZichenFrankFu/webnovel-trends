from __future__ import annotations
from pathlib import Path
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from ..settings import settings
from ..utils import load_repo_config, get_output_paths

router = APIRouter(prefix="/api/reports", tags=["reports"])

def _reports_dir(repo_cfg) -> Path:
    out = get_output_paths(repo_cfg)
    return Path(out.get("reports", str(settings.repo_root / "outputs" / "reports")))

@router.get("")
def list_reports():
    repo_cfg = load_repo_config(settings.repo_root)
    root = _reports_dir(repo_cfg)
    if not root.exists():
        return {"root": str(root), "items": []}

    items = []
    for p in sorted(root.rglob("*")):
        if p.is_file():
            rel = p.relative_to(root).as_posix()
            items.append({"path": rel, "size": p.stat().st_size})
    return {"root": str(root), "items": items}

@router.get("/read")
def read_report(path: str):
    repo_cfg = load_repo_config(settings.repo_root)
    root = _reports_dir(repo_cfg)
    target = (root / path).resolve()
    if root.resolve() not in target.parents and target != root.resolve():
        raise HTTPException(status_code=400, detail="invalid path")
    if not target.exists() or not target.is_file():
        raise HTTPException(status_code=404, detail="file not found")

    suffix = target.suffix.lower()
    if suffix not in [".md", ".txt", ".html"]:
        raise HTTPException(status_code=400, detail="only .md/.txt/.html preview supported in v1")

    return {"path": path, "content": target.read_text(encoding="utf-8", errors="replace")}
