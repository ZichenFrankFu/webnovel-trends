from __future__ import annotations
import json
import time
from pathlib import Path
from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from ..settings import settings
from ..utils import load_repo_config, get_output_paths, get_rank_keys

router = APIRouter(prefix="/config", tags=["config"])

class ConfigOverride(BaseModel):
    platform: str | None = Field(default=None, description="qidian|fanqie")
    rank_key: str | None = None
    pages: int | None = None                 # only qidian
    qidian_pages: int | None = None          # legacy fallback
    chapter_count: int = 5
    newbook_chapter_count: int = 2
    no_detail: bool = False
    no_chapters: bool = False

@router.get("/schema")
def get_schema():
    repo_cfg = load_repo_config(settings.repo_root)
    rank_keys = get_rank_keys(repo_cfg)
    return {
        "defaults": ConfigOverride().model_dump(),
        "rank_keys": rank_keys,
        "notes": {
            "pages": "仅起点有效；番茄固定 1 页滚动",
            "rank_key": "必须与 config.WEBSITES[platform]['rank_urls'] key 完全一致",
        },
    }

@router.post("/runs")
def create_run(override: ConfigOverride):
    repo_cfg = load_repo_config(settings.repo_root)
    out = get_output_paths(repo_cfg)
    runs_dir = Path(out.get("reports", str(settings.repo_root / "outputs" / "reports"))).parent / "config_runs"
    runs_dir.mkdir(parents=True, exist_ok=True)

    run_id = f"cfg_{int(time.time()*1000)}"
    path = runs_dir / f"{run_id}.json"
    path.write_text(json.dumps(override.model_dump(), ensure_ascii=False, indent=2), encoding="utf-8")
    return {"run_id": run_id, "path": str(path)}
