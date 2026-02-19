# ui/backend/app/runtime_paths.py
import os
import sys
from pathlib import Path

def bundle_dir() -> Path:
    # PyInstaller 解包目录
    if hasattr(sys, "_MEIPASS"):
        return Path(sys._MEIPASS)
    # 源码运行：ui/backend/app
    return Path(__file__).resolve().parent

def app_home() -> Path:
    base = os.environ.get("LOCALAPPDATA") or str(Path.home())
    home = Path(base) / "webnovel_trends"
    home.mkdir(parents=True, exist_ok=True)
    return home

def outputs_dir() -> Path:
    d = app_home() / "outputs"
    d.mkdir(parents=True, exist_ok=True)
    return d

def logs_dir() -> Path:
    d = outputs_dir() / "logs"
    d.mkdir(parents=True, exist_ok=True)
    return d

def data_dir() -> Path:
    d = outputs_dir() / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d

def reports_dir() -> Path:
    d = outputs_dir() / "reports"
    d.mkdir(parents=True, exist_ok=True)
    return d
