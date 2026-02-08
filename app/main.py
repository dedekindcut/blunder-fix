from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
import asyncio
import shutil
import sqlite3
import threading
import uuid

from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import FileResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from app.analyzer import analyze_username, evaluate_fen_cp, evaluate_fen_lines
from app.db import (
    anki_stats,
    count_unanalyzed_games,
    ensure_cards_for_threshold,
    fetch_due_card,
    fetch_unanalyzed_games,
    fetch_lines_for_position,
    fetch_practical_response,
    get_db_path,
    get_card,
    init_db,
    list_user_stats,
    replace_analysis_for_game,
    reset_analysis_for_user,
    stats,
    update_card_review,
)
from app.importers import (
    import_chesscom,
    import_chesscom_with_progress,
    import_lichess,
    import_lichess_with_progress,
)
from app.scheduler import next_review

app = FastAPI(title="Blunder Fix")
ANALYZE_JOBS: dict[str, dict] = {}
ANALYZE_LOCK = threading.Lock()
IMPORT_JOBS: dict[str, dict] = {}
IMPORT_LOCK = threading.Lock()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


class ImportRequest(BaseModel):
    username: str
    max_games: int = Field(default=100, ge=1, le=2000)


class ImportStartRequest(ImportRequest):
    source: str = Field(pattern="^(lichess|chesscom)$")


class AnalyzeRequest(BaseModel):
    username: str
    depth: int = Field(default=10, ge=4, le=24)
    multipv: int = Field(default=4, ge=1, le=12)
    cp_window: int = Field(default=50, ge=0, le=300)
    blunder_loss_cp: int = Field(default=200, ge=20, le=1000)
    objective_floor_cp: int = Field(default=-200, ge=-1000, le=1000)
    opening_user_moves_to_skip: int = Field(default=0, ge=0, le=40)
    max_games: int = Field(default=200, ge=1, le=2000)
    stockfish_path: str | None = None


class AnalyzeLineIn(BaseModel):
    pv_rank: int
    cp: int
    first_move_uci: str
    uci_line: str
    san_line: str
    is_acceptable: bool


class AnalyzePracticalIn(BaseModel):
    opponent_move_uci: str
    opponent_move_san: str
    cp_after: int | None = None


class AnalyzePositionIn(BaseModel):
    ply: int
    fen: str
    side_to_move: str = Field(pattern="^(white|black)$")
    played_uci: str
    played_san: str
    best_cp: int
    played_cp: int
    loss_cp: int
    is_blunder: bool
    candidate_lines: list[AnalyzeLineIn] = Field(default_factory=list)
    practical_response: AnalyzePracticalIn | None = None


class AnalyzeStoreGameRequest(BaseModel):
    game_id: int
    positions: list[AnalyzePositionIn] = Field(default_factory=list)


class ReviewGradeRequest(BaseModel):
    card_id: int
    rating: int = Field(ge=1, le=4)


class UserRequest(BaseModel):
    username: str


class EvalRequest(BaseModel):
    fen: str
    depth: int = Field(default=15, ge=4, le=30)
    pov_side: str | None = Field(default=None)
    stockfish_path: str | None = None


class ReplyLinesRequest(BaseModel):
    fen: str
    depth: int = Field(default=12, ge=4, le=24)
    multipv: int = Field(default=4, ge=1, le=12)
    cp_window: int = Field(default=30, ge=0, le=300)
    pov_side: str | None = Field(default=None)
    stockfish_path: str | None = None


def _to_sqlite_utc(dt: datetime) -> str:
    return dt.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")


@app.on_event("startup")
def _startup() -> None:
    init_db()


@app.get("/")
def root() -> RedirectResponse:
    return RedirectResponse(url="/import")


@app.get("/import")
def import_page() -> FileResponse:
    return FileResponse("web/import.html")


@app.get("/analyze")
def analyze_page() -> FileResponse:
    return FileResponse("web/analyze.html")


@app.get("/review")
def review_page() -> FileResponse:
    return FileResponse("web/review.html")


@app.get("/stats")
def stats_page() -> FileResponse:
    return FileResponse("web/stats.html")


@app.post("/api/import/lichess")
async def import_lichess_api(req: ImportRequest) -> dict:
    out = await import_lichess(req.username, max_games=req.max_games)
    return {"imported": out.imported, "skipped": out.skipped}


@app.post("/api/import/chesscom")
async def import_chesscom_api(req: ImportRequest) -> dict:
    out = await import_chesscom(req.username, max_games=req.max_games)
    return {"imported": out.imported, "skipped": out.skipped}


def _run_import_job(job_id: str, req: ImportStartRequest) -> None:
    def _progress(done: int, total: int, imported: int, skipped: int) -> None:
        with IMPORT_LOCK:
            job = IMPORT_JOBS.get(job_id)
            if not job:
                return
            job["done"] = int(done)
            job["total"] = int(total)
            job["imported"] = int(imported)
            job["skipped"] = int(skipped)

    try:
        if req.source == "lichess":
            out = asyncio.run(import_lichess_with_progress(req.username, max_games=req.max_games, progress_cb=_progress))
        else:
            out = asyncio.run(import_chesscom_with_progress(req.username, max_games=req.max_games, progress_cb=_progress))
        with IMPORT_LOCK:
            job = IMPORT_JOBS.get(job_id)
            if not job:
                return
            job["state"] = "done"
            job["imported"] = int(out.imported)
            job["skipped"] = int(out.skipped)
            job["done"] = int(out.imported + out.skipped)
            if not job.get("total"):
                job["total"] = int(out.imported + out.skipped)
    except Exception as exc:  # noqa: BLE001
        with IMPORT_LOCK:
            job = IMPORT_JOBS.get(job_id)
            if not job:
                return
            job["state"] = "error"
            job["error"] = str(exc)


@app.post("/api/import/start")
def import_start_api(req: ImportStartRequest) -> dict:
    job_id = uuid.uuid4().hex
    with IMPORT_LOCK:
        IMPORT_JOBS[job_id] = {
            "state": "running",
            "source": req.source,
            "username": req.username,
            "done": 0,
            "total": 0,
            "imported": 0,
            "skipped": 0,
            "error": None,
        }
    thread = threading.Thread(target=_run_import_job, args=(job_id, req), daemon=True)
    thread.start()
    return {"job_id": job_id}


@app.get("/api/import/progress/{job_id}")
def import_progress_api(job_id: str) -> dict:
    with IMPORT_LOCK:
        job = IMPORT_JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(job)


@app.post("/api/analyze")
def analyze_api(req: AnalyzeRequest) -> dict:
    out = analyze_username(
        req.username,
        depth=req.depth,
        multipv=req.multipv,
        cp_window=req.cp_window,
        blunder_loss_cp=req.blunder_loss_cp,
        objective_floor_cp=req.objective_floor_cp,
        opening_user_moves_to_skip=req.opening_user_moves_to_skip,
        max_games=req.max_games,
        stockfish_path=req.stockfish_path,
    )
    return {"games": out.games, "positions": out.positions, "blunders": out.blunders}


def _run_analyze_job(job_id: str, req: AnalyzeRequest) -> None:
    def _progress(done: int, total: int) -> None:
        with ANALYZE_LOCK:
            job = ANALYZE_JOBS.get(job_id)
            if not job:
                return
            job["games_done"] = int(done)
            job["total_games"] = int(total)

    try:
        out = analyze_username(
            req.username,
            depth=req.depth,
            multipv=req.multipv,
            cp_window=req.cp_window,
            blunder_loss_cp=req.blunder_loss_cp,
            objective_floor_cp=req.objective_floor_cp,
            opening_user_moves_to_skip=req.opening_user_moves_to_skip,
            max_games=req.max_games,
            stockfish_path=req.stockfish_path,
            progress_cb=_progress,
        )
        with ANALYZE_LOCK:
            job = ANALYZE_JOBS.get(job_id)
            if not job:
                return
            job["state"] = "done"
            job["result"] = {"games": out.games, "positions": out.positions, "blunders": out.blunders}
            job["games_done"] = out.games
    except Exception as exc:  # noqa: BLE001
        with ANALYZE_LOCK:
            job = ANALYZE_JOBS.get(job_id)
            if not job:
                return
            job["state"] = "error"
            job["error"] = str(exc)


@app.post("/api/analyze/start")
def analyze_start_api(req: AnalyzeRequest) -> dict:
    total = count_unanalyzed_games(req.username, req.max_games)
    job_id = uuid.uuid4().hex
    with ANALYZE_LOCK:
        ANALYZE_JOBS[job_id] = {
            "state": "running",
            "username": req.username,
            "games_done": 0,
            "total_games": total,
            "result": None,
            "error": None,
        }
    thread = threading.Thread(target=_run_analyze_job, args=(job_id, req), daemon=True)
    thread.start()
    return {"job_id": job_id, "total_games": total}


@app.get("/api/analyze/progress/{job_id}")
def analyze_progress_api(job_id: str) -> dict:
    with ANALYZE_LOCK:
        job = ANALYZE_JOBS.get(job_id)
        if not job:
            raise HTTPException(status_code=404, detail="Job not found")
        return dict(job)


@app.post("/api/analyze/reset")
def analyze_reset_api(req: UserRequest) -> dict:
    out = reset_analysis_for_user(req.username)
    return out


@app.get("/api/analyze/games/{username}")
def analyze_games_api(
    username: str,
    max_games: int = Query(default=200, ge=1, le=2000),
) -> dict:
    games = fetch_unanalyzed_games(username, max_games)
    return {
        "games": [
            {
                "id": int(g["id"]),
                "played_color": g["played_color"],
                "pgn": g["pgn"],
            }
            for g in games
        ],
        "total_games": len(games),
    }


@app.post("/api/analyze/store-game")
def analyze_store_game_api(req: AnalyzeStoreGameRequest) -> dict:
    out = replace_analysis_for_game(
        req.game_id,
        [p.model_dump() for p in req.positions],
    )
    return out


@app.get("/api/db/export")
def db_export_api() -> FileResponse:
    db_path = get_db_path()
    if not db_path.exists():
        raise HTTPException(status_code=404, detail="Database file not found")
    return FileResponse(
        path=str(db_path),
        filename=db_path.name,
        media_type="application/x-sqlite3",
    )


@app.post("/api/db/import")
async def db_import_api(file: UploadFile = File(...)) -> dict:
    db_path = get_db_path()
    suffix = Path(file.filename or "upload.db").suffix or ".db"
    tmp_path = db_path.with_suffix(f".import{suffix}")

    content = await file.read()
    if not content:
        raise HTTPException(status_code=400, detail="Uploaded file is empty")

    tmp_path.write_bytes(content)
    try:
        test_conn = sqlite3.connect(tmp_path)
        test_conn.execute("PRAGMA quick_check;").fetchone()
        test_conn.close()
    except Exception as exc:  # noqa: BLE001
        try:
            tmp_path.unlink(missing_ok=True)
        except Exception:
            pass
        raise HTTPException(status_code=400, detail=f"Invalid SQLite database: {exc}") from exc

    # Replace main DB and clear stale WAL/SHM sidecars.
    shutil.move(str(tmp_path), str(db_path))
    Path(str(db_path) + "-wal").unlink(missing_ok=True)
    Path(str(db_path) + "-shm").unlink(missing_ok=True)
    init_db()
    return {"ok": True, "db_path": str(db_path)}


@app.get("/api/stats/{username}")
def stats_api(
    username: str,
    blunder_threshold: int = Query(default=200, ge=20, le=1000),
    objective_floor_cp: int = Query(default=-200, ge=-1000, le=1000),
    winning_prune_cp: int = Query(default=300, ge=0, le=2000),
) -> dict:
    return stats(
        username,
        min_loss_cp=blunder_threshold,
        min_best_cp=objective_floor_cp,
        winning_prune_cp=winning_prune_cp,
    )


@app.get("/api/stats/anki/{username}")
def anki_stats_api(username: str, days: int = Query(default=60, ge=7, le=365)) -> dict:
    return anki_stats(username, days=days)


@app.get("/api/users")
def users_api(
    blunder_threshold: int = Query(default=200, ge=20, le=1000),
    objective_floor_cp: int = Query(default=-200, ge=-1000, le=1000),
    winning_prune_cp: int = Query(default=300, ge=0, le=2000),
) -> dict:
    return {
        "users": list_user_stats(
            min_loss_cp=blunder_threshold,
            min_best_cp=objective_floor_cp,
            winning_prune_cp=winning_prune_cp,
        )
    }


@app.get("/api/review/next/{username}")
def review_next_api(
    username: str,
    blunder_threshold: int = Query(default=200, ge=20, le=1000),
    objective_floor_cp: int = Query(default=-200, ge=-1000, le=1000),
    winning_prune_cp: int = Query(default=300, ge=0, le=2000),
) -> dict:
    ensure_cards_for_threshold(username, blunder_threshold, objective_floor_cp, winning_prune_cp)
    card = fetch_due_card(
        username,
        min_loss_cp=blunder_threshold,
        min_best_cp=objective_floor_cp,
        winning_prune_cp=winning_prune_cp,
    )
    if not card:
        return {"card": None}

    lines = fetch_lines_for_position(card["position_id"])
    practical = fetch_practical_response(card["position_id"])

    acceptable = [
        {
            "first_move_uci": r["first_move_uci"],
            "san_line": r["san_line"],
            "cp": r["cp"],
        }
        for r in lines
        if r["is_acceptable"]
    ]
    all_lines = [
        {
            "first_move_uci": r["first_move_uci"],
            "san_line": r["san_line"],
            "cp": r["cp"],
            "rank": r["pv_rank"],
            "is_acceptable": bool(r["is_acceptable"]),
        }
        for r in lines
    ]

    return {
        "card": {
            "card_id": card["card_id"],
            "fen": card["fen"],
            "side_to_move": card["side_to_move"],
            "loss_cp": card["loss_cp"],
            "played_uci": card["played_uci"],
            "played_san": card["played_san"],
            "source": card["source"],
            "source_game_id": card["source_game_id"],
            "best_cp": card["best_cp"],
            "played_cp": card["played_cp"],
            "acceptable_lines": acceptable,
            "all_lines": all_lines,
            "practical_response": (
                {
                    "opponent_move_uci": practical["opponent_move_uci"],
                    "opponent_move_san": practical["opponent_move_san"],
                    "cp_after": practical["cp_after"],
                }
                if practical
                else None
            ),
        }
    }


@app.post("/api/eval")
def eval_api(req: EvalRequest) -> dict:
    try:
        cp = evaluate_fen_cp(
            req.fen,
            depth=req.depth,
            pov_side=req.pov_side,
            stockfish_path=req.stockfish_path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"cp": cp}


@app.post("/api/reply-lines")
def reply_lines_api(req: ReplyLinesRequest) -> dict:
    try:
        lines = evaluate_fen_lines(
            req.fen,
            depth=req.depth,
            multipv=req.multipv,
            cp_window=req.cp_window,
            pov_side=req.pov_side,
            stockfish_path=req.stockfish_path,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    except RuntimeError as exc:
        raise HTTPException(status_code=503, detail=str(exc)) from exc
    return {"lines": lines}


@app.post("/api/review/grade")
def review_grade_api(req: ReviewGradeRequest) -> dict:
    card = get_card(req.card_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")

    rr = next_review(
        state=card["state"],
        step=card["step"],
        stability=card["stability"],
        difficulty=card["difficulty"],
        reps=card["reps"],
        lapses=card["lapses"],
        last_review_at=card["last_review_at"],
        due_at=card["due_at"],
        rating=req.rating,
    )

    reviewed_at = _to_sqlite_utc(datetime.now(timezone.utc))
    update_card_review(
        req.card_id,
        state=rr.state,
        step=rr.step,
        due_at=_to_sqlite_utc(rr.due_at),
        stability=rr.stability,
        difficulty=rr.difficulty,
        reps=rr.reps,
        lapses=rr.lapses,
        reviewed_at=reviewed_at,
        rating=req.rating,
        elapsed_days=rr.elapsed_days,
    )

    return {
        "next_due_at": _to_sqlite_utc(rr.due_at),
        "state": rr.state,
        "stability": rr.stability,
        "difficulty": rr.difficulty,
    }


@app.get("/api/review/preview/{card_id}")
def review_preview_api(card_id: int) -> dict:
    card = get_card(card_id)
    if not card:
        raise HTTPException(status_code=404, detail="Card not found")

    out: dict[str, str] = {}
    for rating in (1, 2, 3, 4):
        rr = next_review(
            state=card["state"],
            step=card["step"],
            stability=card["stability"],
            difficulty=card["difficulty"],
            reps=card["reps"],
            lapses=card["lapses"],
            last_review_at=card["last_review_at"],
            due_at=card["due_at"],
            rating=rating,
        )
        out[str(rating)] = _to_sqlite_utc(rr.due_at)
    return {"due_by_rating": out}


@app.get("/healthz")
def healthz() -> dict[str, str]:
    return {"ok": "true"}


app.mount("/web", StaticFiles(directory="web"), name="web")


@app.get("/favicon.ico")
def favicon() -> FileResponse:
    # Avoid 404 noise in logs.
    blank = Path("web/favicon.ico")
    if blank.exists():
        return FileResponse(blank)
    raise HTTPException(status_code=404, detail="Not found")
