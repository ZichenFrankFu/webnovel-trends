import threading
import time
import socket
import webview
import multiprocessing
import sys
from pathlib import Path
import uvicorn

import logging
from pathlib import Path

LOG_DIR = Path.home() / "AppData" / "Local" / "webnovel_trends"
LOG_DIR.mkdir(parents=True, exist_ok=True)
LOG_PATH = LOG_DIR / "launcher.log"

logging.basicConfig(
    filename=str(LOG_PATH),
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logging.info("Launcher starting...")



HOST = "127.0.0.1"
PORT = 8713


def wait_for_server(host, port, timeout=15):
    """等待端口可连接"""
    start = time.time()
    while time.time() - start < timeout:
        try:
            with socket.create_connection((host, port), timeout=1):
                return True
        except OSError:
            time.sleep(0.2)
    return False


def run_server():
    import sys
    from pathlib import Path
    import uvicorn

    # 确保 exe 运行时也能找到你的项目代码
    # 开发态：项目根目录
    root = Path(__file__).resolve().parent
    if str(root) not in sys.path:
        sys.path.insert(0, str(root))

    # 关键：直接 import app，而不是用字符串 "ui.backend.app.main:app"
    from ui.backend.app.main import app

    uvicorn.run(app, host=HOST, port=PORT, reload=False, log_level="info")



def main():
    multiprocessing.freeze_support()

    # 后台线程启动服务器
    server_thread = threading.Thread(target=run_server, daemon=True)
    server_thread.start()

    # 等待服务器启动
    if not wait_for_server(HOST, PORT):
        print("Server failed to start.")
        sys.exit(1)

    # 创建桌面窗口
    webview.create_window(
        "WebNovel Trends",
        f"http://{HOST}:{PORT}",
        width=1200,
        height=800,
    )

    # 启动 GUI 主循环
    webview.start(gui="edgechromium")


if __name__ == "__main__":
    main()
