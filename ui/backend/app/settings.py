from __future__ import annotations
from pathlib import Path
from pydantic_settings import BaseSettings

class Settings(BaseSettings):
    # ui/backend/app/settings.py 所在目录：ui/backend/app
    # repo_root = 往上 3 层
    repo_root: Path = Path(__file__).resolve().parents[3]

    # 运行 python main.py 的解释器（默认用当前环境 python）
    python_bin: str = "python"

    # 允许访问的输出目录（安全白名单）
    allow_outputs_dirname: str = "outputs"

settings = Settings()
