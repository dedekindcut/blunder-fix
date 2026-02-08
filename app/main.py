from __future__ import annotations

from pathlib import Path

from fastapi import FastAPI, HTTPException
from fastapi.responses import FileResponse, RedirectResponse, Response
from fastapi.staticfiles import StaticFiles

app = FastAPI(title="Blunder Fix Static")


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/web/index.html")


@app.get("/import")
def import_page() -> RedirectResponse:
    return RedirectResponse(url="/web/import.html")


@app.get("/analyze")
def analyze_page() -> RedirectResponse:
    return RedirectResponse(url="/web/analyze.html")


@app.get("/review")
def review_page() -> RedirectResponse:
    return RedirectResponse(url="/web/review.html")


@app.get("/stats")
def stats_page() -> RedirectResponse:
    return RedirectResponse(url="/web/stats.html")


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"ok": "true"}


app.mount("/web", StaticFiles(directory="web"), name="web")


@app.get("/favicon.ico")
def favicon() -> Response:
    icon = Path("web/favicon.ico")
    if icon.exists():
        return FileResponse(icon)
    return Response(status_code=204)
