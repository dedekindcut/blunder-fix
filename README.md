# Blunder Fix (MVP)

Simple web app to:

- import your games from Lichess and Chess.com
- analyze positions quickly with Stockfish at low depth
- use `MultiPV` to store multiple acceptable lines within a centipawn window
- store practical opponent responses from real games
- review blunders with a 4-button FSRS-style scheduler (`Again/Hard/Good/Easy`)

## Stack

- Backend: FastAPI + SQLite (`blunderfix.db`)
- Analysis: `python-chess` + UCI Stockfish
- Frontend: static HTML/JS + `chessground`

## About chessground

Yes, your assumption is correct: both `../maia-platform-frontend` and `../en-croissant` use `chessground`.

This MVP uses `chessground` directly in the browser via ESM CDN for minimal setup. If you want, we can switch this to a local package setup next.

## Quick start

1. Install/sync dependencies with `uv`:

```bash
uv sync
```

2. Run API + web app:

```bash
uv run uvicorn app.main:app --reload --port 8000
```

3. Open:

- http://127.0.0.1:8000/import
- http://127.0.0.1:8000/analyze
- http://127.0.0.1:8000/review

## Stockfish path

Analyzer tries these in order:

1. `stockfish` in `$PATH`
2. `../stockfish/stockfish-macos-m1-apple-silicon`

You can override in API request with `stockfish_path`.

## API summary

- `POST /api/import/lichess`
- `POST /api/import/chesscom`
- `POST /api/analyze`
- `GET /api/stats/{username}`
- `GET /api/review/next/{username}`
- `POST /api/review/grade`

## Current scheduler

`app/scheduler.py` implements an FSRS-style algorithm with:

- stability
- difficulty
- elapsed time
- 4-grade review updates

It is intentionally lightweight and compatible with Anki-like review flow, but not a strict byte-for-byte reimplementation of Anki FSRS parameters. If you want strict parity, next step is to swap this module with a full FSRS implementation and keep the same DB/API shape.

## Notes

- Analysis throughput is controlled by `depth`, `multipv`, and number of games.
- Blunders are currently positions where `best_cp - played_cp >= blunder_loss_cp`.
- Practical response currently stores the actual next opponent move from the game and evaluation after that move.
