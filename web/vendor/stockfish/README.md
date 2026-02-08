# Stockfish Web Worker

Optional local override for browser Stockfish worker.

If present, this file is used first:
- `stockfish.js`
- `stockfish.wasm`

Otherwise, the app falls back to CDN:
- `stockfish@17.1.0/src/stockfish-17.1-lite-single-03e3232.js` (jsDelivr, then unpkg)
