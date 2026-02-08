from __future__ import annotations

import io
import shutil
from dataclasses import dataclass
from typing import Callable

import chess
import chess.engine
import chess.pgn

from app.db import (
    fetch_unanalyzed_games,
    insert_candidate_lines,
    insert_position,
    insert_practical_response,
    mark_game_analyzed,
    upsert_card_for_position,
)


@dataclass
class AnalyzeResult:
    games: int = 0
    positions: int = 0
    blunders: int = 0


def _score_cp(score: chess.engine.PovScore, pov: chess.Color) -> int:
    return score.pov(pov).score(mate_score=100000) or 0


def _resolve_stockfish_path(path_hint: str | None = None) -> str:
    if path_hint:
        return path_hint
    local = shutil.which("stockfish")
    if local:
        return local
    fallback = "../stockfish/stockfish-macos-m1-apple-silicon"
    return fallback


def _pv_to_san(board: chess.Board, pv: list[chess.Move], max_len: int = 10) -> str:
    b = board.copy()
    sans: list[str] = []
    for mv in pv[:max_len]:
        sans.append(b.san(mv))
        b.push(mv)
    return " ".join(sans)


def evaluate_fen_cp(fen: str, *, depth: int = 15, pov_side: str | None = None, stockfish_path: str | None = None) -> int:
    board = chess.Board(fen)
    if not board.is_valid():
        raise ValueError("Invalid FEN position")
    engine_path = _resolve_stockfish_path(stockfish_path)
    engine: chess.engine.SimpleEngine | None = None
    try:
        engine = chess.engine.SimpleEngine.popen_uci(engine_path)
        info = engine.analyse(board, chess.engine.Limit(depth=depth))
    except (OSError, chess.engine.EngineError) as exc:
        raise RuntimeError(f"Engine analyze failed: {exc}") from exc
    finally:
        if engine is not None:
            try:
                engine.quit()
            except chess.engine.EngineTerminatedError:
                pass

    if pov_side == "white":
        pov = chess.WHITE
    elif pov_side == "black":
        pov = chess.BLACK
    else:
        pov = board.turn
    return _score_cp(info["score"], pov)


def evaluate_fen_lines(
    fen: str,
    *,
    depth: int = 12,
    multipv: int = 4,
    cp_window: int = 30,
    pov_side: str | None = None,
    stockfish_path: str | None = None,
) -> list[dict]:
    board = chess.Board(fen)
    if not board.is_valid():
        raise ValueError("Invalid FEN position")
    if pov_side == "white":
        pov = chess.WHITE
    elif pov_side == "black":
        pov = chess.BLACK
    else:
        pov = board.turn

    engine_path = _resolve_stockfish_path(stockfish_path)
    engine: chess.engine.SimpleEngine | None = None
    try:
        engine = chess.engine.SimpleEngine.popen_uci(engine_path)
        infos = engine.analyse(board, chess.engine.Limit(depth=depth), multipv=multipv)
    except (OSError, chess.engine.EngineError) as exc:
        raise RuntimeError(f"Engine analyze failed: {exc}") from exc
    finally:
        if engine is not None:
            try:
                engine.quit()
            except chess.engine.EngineTerminatedError:
                pass

    if isinstance(infos, dict):
        infos = [infos]
    if not infos:
        return []

    best_cp: int | None = None
    out: list[dict] = []
    for idx, info in enumerate(infos, start=1):
        pv = info.get("pv", [])
        if not pv:
            continue
        cp = _score_cp(info["score"], pov)
        if best_cp is None:
            best_cp = cp
        first = pv[0]
        out.append(
            {
                "rank": idx,
                "cp": cp,
                "first_move_uci": first.uci(),
                "first_move_san": board.san(first),
                "san_line": _pv_to_san(board, pv, max_len=10),
            }
        )

    if best_cp is None:
        return []
    return [r for r in out if (best_cp - r["cp"]) <= cp_window]


def analyze_username(
    username: str,
    *,
    depth: int = 10,
    multipv: int = 4,
    cp_window: int = 30,
    blunder_loss_cp: int = 120,
    objective_floor_cp: int = -200,
    opening_user_moves_to_skip: int = 0,
    max_games: int = 200,
    stockfish_path: str | None = None,
    progress_cb: Callable[[int, int], None] | None = None,
) -> AnalyzeResult:
    games = fetch_unanalyzed_games(username, max_games)
    if not games:
        return AnalyzeResult()

    engine_path = _resolve_stockfish_path(stockfish_path)
    engine = chess.engine.SimpleEngine.popen_uci(engine_path)

    out = AnalyzeResult()

    try:
        total_games = len(games)
        if progress_cb:
            progress_cb(0, total_games)
        for game_row in games:
            game = chess.pgn.read_game(io.StringIO(game_row["pgn"]))
            if not game:
                mark_game_analyzed(game_row["id"])
                continue

            user_color = chess.WHITE if game_row["played_color"] == "white" else chess.BLACK
            board = game.board()
            mainline = list(game.mainline_moves())
            user_move_index = 0

            for ply, played_move in enumerate(mainline, start=1):
                if board.turn != user_color:
                    board.push(played_move)
                    continue
                user_move_index += 1
                if user_move_index <= opening_user_moves_to_skip:
                    board.push(played_move)
                    continue

                fen = board.fen()
                side_to_move = "white" if board.turn == chess.WHITE else "black"
                played_san = board.san(played_move)

                infos = engine.analyse(board, chess.engine.Limit(depth=depth), multipv=multipv)
                if isinstance(infos, dict):
                    infos = [infos]

                candidates: list[dict] = []
                best_cp = _score_cp(infos[0]["score"], board.turn)
                played_cp: int | None = None

                for idx, info in enumerate(infos, start=1):
                    pv = info.get("pv", [])
                    if not pv:
                        continue
                    cp = _score_cp(info["score"], board.turn)
                    first = pv[0].uci()
                    if first == played_move.uci():
                        played_cp = cp
                    candidates.append(
                        {
                            "pv_rank": idx,
                            "cp": cp,
                            "first_move_uci": first,
                            "uci_line": " ".join(m.uci() for m in pv[:10]),
                            "san_line": _pv_to_san(board, pv, max_len=10),
                            "is_acceptable": (best_cp - cp) <= cp_window,
                        }
                    )

                if played_cp is None:
                    b2 = board.copy()
                    b2.push(played_move)
                    played_info = engine.analyse(b2, chess.engine.Limit(depth=max(6, depth - 2)))
                    played_cp = -_score_cp(played_info["score"], b2.turn)

                loss_cp = best_cp - played_cp
                is_blunder = loss_cp >= blunder_loss_cp

                position_id = insert_position(
                    {
                        "game_id": game_row["id"],
                        "ply": ply,
                        "fen": fen,
                        "side_to_move": side_to_move,
                        "played_uci": played_move.uci(),
                        "played_san": played_san,
                        "best_cp": best_cp,
                        "played_cp": played_cp,
                        "loss_cp": loss_cp,
                        "is_blunder": is_blunder,
                    }
                )
                if candidates:
                    insert_candidate_lines(position_id, candidates)

                # Store practical response from the game (opponent's next move, if any).
                board.push(played_move)
                if ply < len(mainline):
                    opp_move = mainline[ply]
                    opp_san = board.san(opp_move)
                    b3 = board.copy()
                    b3.push(opp_move)
                    resp_info = engine.analyse(b3, chess.engine.Limit(depth=max(6, depth - 2)))
                    cp_after = _score_cp(resp_info["score"], user_color)
                    insert_practical_response(position_id, opp_move.uci(), opp_san, cp_after)

                if is_blunder and best_cp >= objective_floor_cp:
                    upsert_card_for_position(position_id)
                    out.blunders += 1

                out.positions += 1

            mark_game_analyzed(game_row["id"])
            out.games += 1
            if progress_cb:
                progress_cb(out.games, total_games)
    finally:
        engine.quit()

    return out
