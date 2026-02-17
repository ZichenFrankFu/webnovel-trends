from __future__ import annotations
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from .routers.config_api import router as config_router
from .routers.tasks_api import router as tasks_router
from .routers.reports_api import router as reports_router
from .routers.db_api import router as db_router

app = FastAPI(title="WebNovel Trends UI Backend", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:5173"],  # Vite dev
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

app.include_router(config_router)
app.include_router(tasks_router)
app.include_router(reports_router)
app.include_router(db_router)

@app.get("/health")
def health():
    return {"ok": True}
