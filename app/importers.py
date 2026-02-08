from __future__ import annotations

import asyncio
import io
import re
from dataclasses import dataclass
from typing import Callable, Iterable

import chess.pgn
import httpx

from app.db import insert_game


@dataclass
class ImportResult:
    imported: int = 0
    skipped: int = 0


def _split_pgn_games(blob: str) -> list[str]:
    chunks = re.split(r"\n\n(?=\[Event )", blob.strip())
    return [c.strip() for c in chunks if c.strip()]


def _game_headers(pgn_text: str) -> dict[str, str]:
    game = chess.pgn.read_game(io.StringIO(pgn_text))
    if not game:
        return {}
    return dict(game.headers)


def _result_for_user(headers: dict[str, str], username: str) -> tuple[str, str]:
    white = headers.get("White", "")
    black = headers.get("Black", "")
    result = headers.get("Result", "*")
    played_color = "white" if white.lower() == username.lower() else "black"

    if result == "1-0":
        return played_color, "win" if played_color == "white" else "loss"
    if result == "0-1":
        return played_color, "win" if played_color == "black" else "loss"
    if result == "1/2-1/2":
        return played_color, "draw"
    return played_color, "unknown"


async def import_lichess(username: str, max_games: int = 100) -> ImportResult:
    url = f"https://lichess.org/api/games/user/{username}"
    params = {
        "max": max_games,
        "moves": "true",
        "tags": "true",
        "clocks": "false",
        "evals": "false",
        "opening": "false",
        "pgnInJson": "false",
    }
    headers = {"Accept": "application/x-chess-pgn"}

    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(url, params=params, headers=headers)
        res.raise_for_status()
        pgn_blob = res.text

    games = _split_pgn_games(pgn_blob)
    result = ImportResult()
    for game_pgn in games:
        headers = _game_headers(game_pgn)
        game_id = headers.get("Site", "").rstrip("/").split("/")[-1]
        if not game_id:
            result.skipped += 1
            continue
        played_color, user_result = _result_for_user(headers, username)
        ok = insert_game("lichess", game_id, username, played_color, user_result, game_pgn)
        if ok:
            result.imported += 1
        else:
            result.skipped += 1
    return result


async def import_lichess_with_progress(
    username: str,
    *,
    max_games: int = 100,
    progress_cb: Callable[[int, int, int, int], None] | None = None,
) -> ImportResult:
    url = f"https://lichess.org/api/games/user/{username}"
    params = {
        "max": max_games,
        "moves": "true",
        "tags": "true",
        "clocks": "false",
        "evals": "false",
        "opening": "false",
        "pgnInJson": "false",
    }
    headers = {"Accept": "application/x-chess-pgn"}

    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(url, params=params, headers=headers)
        res.raise_for_status()
        pgn_blob = res.text

    games = _split_pgn_games(pgn_blob)
    total = len(games)
    out = ImportResult()
    if progress_cb:
        progress_cb(0, total, out.imported, out.skipped)

    done = 0
    for game_pgn in games:
        hdrs = _game_headers(game_pgn)
        game_id = hdrs.get("Site", "").rstrip("/").split("/")[-1]
        if not game_id:
            out.skipped += 1
        else:
            played_color, user_result = _result_for_user(hdrs, username)
            ok = insert_game("lichess", game_id, username, played_color, user_result, game_pgn)
            if ok:
                out.imported += 1
            else:
                out.skipped += 1
        done += 1
        if progress_cb:
            progress_cb(done, total, out.imported, out.skipped)
    return out


async def _fetch_chesscom_archives(username: str) -> Iterable[str]:
    base = f"https://api.chess.com/pub/player/{username}/games/archives"
    async with httpx.AsyncClient(timeout=60) as client:
        res = await client.get(base)
        res.raise_for_status()
        archives = res.json().get("archives", [])
        for archive_url in archives:
            pgn_res = await client.get(archive_url + "/pgn")
            if pgn_res.status_code == 200:
                yield pgn_res.text
            await asyncio.sleep(0.05)


async def import_chesscom(username: str, max_games: int = 200) -> ImportResult:
    out = ImportResult()
    collected: list[str] = []

    async for month_blob in _fetch_chesscom_archives(username):
        collected.extend(_split_pgn_games(month_blob))
        if len(collected) >= max_games:
            break

    for game_pgn in collected[-max_games:]:
        headers = _game_headers(game_pgn)
        game_id = headers.get("Link", "").rstrip("/").split("/")[-1]
        if not game_id:
            site = headers.get("Site", "")
            game_id = site.rstrip("/").split("/")[-1]
        if not game_id:
            out.skipped += 1
            continue

        played_color, user_result = _result_for_user(headers, username)
        ok = insert_game("chesscom", game_id, username, played_color, user_result, game_pgn)
        if ok:
            out.imported += 1
        else:
            out.skipped += 1

    return out


async def import_chesscom_with_progress(
    username: str,
    *,
    max_games: int = 200,
    progress_cb: Callable[[int, int, int, int], None] | None = None,
) -> ImportResult:
    out = ImportResult()
    collected: list[str] = []

    async for month_blob in _fetch_chesscom_archives(username):
        collected.extend(_split_pgn_games(month_blob))
        if len(collected) >= max_games:
            break

    selected = collected[-max_games:]
    total = len(selected)
    if progress_cb:
        progress_cb(0, total, out.imported, out.skipped)

    done = 0
    for game_pgn in selected:
        headers = _game_headers(game_pgn)
        game_id = headers.get("Link", "").rstrip("/").split("/")[-1]
        if not game_id:
            site = headers.get("Site", "")
            game_id = site.rstrip("/").split("/")[-1]

        if not game_id:
            out.skipped += 1
        else:
            played_color, user_result = _result_for_user(headers, username)
            ok = insert_game("chesscom", game_id, username, played_color, user_result, game_pgn)
            if ok:
                out.imported += 1
            else:
                out.skipped += 1
        done += 1
        if progress_cb:
            progress_cb(done, total, out.imported, out.skipped)

    return out
