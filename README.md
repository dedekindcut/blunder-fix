# Blunder Fix (Client-Side)

Browser-only chess blunder trainer:

- imports Lichess + Chess.com games directly in the browser
- analyzes with browser Stockfish (`web/vendor/stockfish/stockfish.js`)
- stores all state locally in IndexedDB (no server DB)
- exports/imports state as JSON from the Import page

## Run (pure static)

From the repo root:

```bash
cd web
python3 -m http.server 8000
```

Then open:

- http://127.0.0.1:8000/index.html
- http://127.0.0.1:8000/import.html
- http://127.0.0.1:8000/analyze.html
- http://127.0.0.1:8000/review.html
- http://127.0.0.1:8000/stats.html

## Storage model

- Runtime data is in IndexedDB (`blunderfix-local`)
- DB export/import uses JSON snapshots (Import page)
- No FastAPI/API backend is required for normal use

## Stockfish

- Local-first worker path: `web/vendor/stockfish/stockfish.js`
- Companion wasm: `web/vendor/stockfish/stockfish.wasm`

If local files are unavailable, CDN fallback is attempted.
