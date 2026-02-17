from __future__ import annotations
import importlib.util
from pathlib import Path
from typing import Any, Dict, Tuple

def load_repo_config(repo_root: Path):
    cfg_path = repo_root / "config.py"
    if not cfg_path.exists():
        raise FileNotFoundError(f"config.py not found at: {cfg_path}")
    spec = importlib.util.spec_from_file_location("repo_config", str(cfg_path))
    module = importlib.util.module_from_spec(spec)  # type: ignore
    assert spec and spec.loader
    spec.loader.exec_module(module)  # type: ignore
    return module

def get_output_paths(repo_cfg) -> Dict[str, str]:
    # 你 config.py 定义了 OUTPUT_PATHS
    out = getattr(repo_cfg, "OUTPUT_PATHS", None) or {}
    return {k: str(v) for k, v in out.items()}

def get_db_path(repo_cfg, repo_root: Path) -> str:
    # 兼容你 config.py 的 DATABASE['path']（你现在就是这个）:contentReference[oaicite:1]{index=1}
    db = getattr(repo_cfg, "DATABASE", None) or {}
    p = db.get("path")
    if p:
        return str(Path(p))  # config.py 里已是绝对/拼好了 BASE_DIR
    # fallback
    out = get_output_paths(repo_cfg)
    data_dir = out.get("data", str(repo_root / "outputs" / "data"))
    return str(Path(data_dir) / "novels.db")

def get_rank_keys(repo_cfg) -> Dict[str, list[str]]:
    websites = getattr(repo_cfg, "WEBSITES", None) or {}
    res: Dict[str, list[str]] = {}
    for plat in ["qidian", "fanqie"]:
        cfg = websites.get(plat, {}) or {}
        rank_urls = cfg.get("rank_urls", {}) or {}
        res[plat] = list(rank_urls.keys())
    return res
