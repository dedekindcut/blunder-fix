import { installLocalApi } from './local-api.js';

installLocalApi();

let ChessgroundCtor = null;
let ChessCtor = null;

const $ = (id) => document.getElementById(id);
const STORAGE_USER_KEY = 'bf:selectedUser';
const STORAGE_AUTO_GRADE_KEY = 'bf:autoGradeMode';
const STORAGE_OPP_RESPONSE_KEY = 'bf:opponentResponseMode';
const STORAGE_SHOW_POSITION_EVAL_KEY = 'bf:showPositionEval';
const STORAGE_HIDE_PLAYED_MOVE_KEY = 'bf:hidePlayedMove';
const STORAGE_EXCLUDE_LOST_KEY = 'bf:excludeLostPositions';
const STORAGE_BOARD_THEME_KEY = 'bf:boardTheme';
const STORAGE_PIECE_SET_KEY = 'bf:pieceSet';
const STORAGE_SEVERITY_FILTER_KEY = 'bf:severityFilter';

const logEl = $('log');
const infoEl = $('cardInfo');
const importStatusEl = $('importStatus');
const analyzeStatusEl = $('analyzeStatus');
const analyzeErrorsEl = $('analyzeErrors');
const reviewMoveStatusEl = $('reviewMoveStatus');
const engineStatusEl = $('engineStatus');
const boardOverlayEl = $('boardOverlay');
const boardDoneOverlayEl = $('boardDoneOverlay');
const userCardsEl = $('userCards');

const metricReviewedEl = $('metricReviewed');
const metricAccuracyEl = $('metricAccuracy');
const metricStreakEl = $('metricStreak');
const metricBestStreakEl = $('metricBestStreak');
const metricBlundersEl = $('metricBlunders');
const metricQueueDueEl = $('metricQueueDue');
const metricQueueWrongEl = $('metricQueueWrong');
const metricQueueNewEl = $('metricQueueNew');
const attemptCardIdEl = $('attemptCardId');
const attemptTimerEl = $('attemptTimer');
const attemptPlayedRowEl = $('attemptPlayedRow');
const attemptPlayedMoveEl = $('attemptPlayedMove');
const attemptListEl = $('attemptList');
const answerEvalMetricEl = $('answerEvalMetric');
const promotionPickerEl = $('promotionPicker');
const autoNextWrapEl = $('autoNextWrap');
const autoNextTextEl = $('autoNextText');
const autoNextCancelEl = $('autoNextCancel');

let cg = null;
let currentCard = null;
let positionChess = null;
let playedMoveUci = null;
let overlayTimer = null;
let wrongResetTimer = null;
let moveAttemptRecorded = false;
let usersCache = [];
let attempts = [];
let answerShown = false;
let answerRevealed = false;
let opponentReplyShapes = [];
let awaitingOpponentResponse = false;
let opponentResponseMoves = new Set();
let opponentPhaseFen = null;
let replyRequestSeq = 0;
let cardStartedAt = null;
let wrongAttemptsThisCard = 0;
let autoProceedTimer = null;
let cardTimerInterval = null;
let promotionRequestSeq = 0;
let autoNextTickTimer = null;
let sfWorker = null;
let sfInitPromise = null;
let sfQueue = Promise.resolve();
let sfInitState = 'idle';
let sfInitError = null;
let pieceSetApplySeq = 0;
const PIECE_SETS = ['cburnett', 'alpha', 'merida', 'pirouetti', 'cardinal', 'maestro'];
const pieceSetAvailableCache = new Map();
const SF_WORKER_CANDIDATES = [
  'vendor/stockfish/stockfish.js',
  'https://cdn.jsdelivr.net/npm/stockfish@17.1.0/src/stockfish-17.1-lite-single-03e3232.js',
  'https://unpkg.com/stockfish@17.1.0/src/stockfish-17.1-lite-single-03e3232.js',
];

const sessionMetrics = { reviewed: 0, attempts: 0, correct: 0, wrong: 0, streak: 0, bestStreak: 0 };
const gradeBaseLabels = { 1: 'Again', 2: 'Hard', 3: 'Good', 4: 'Easy' };
const DEFAULT_SEVERITY_FILTER = { inaccuracy: false, mistake: false, blunder: true };

function playedMoveSeverity(lossCp) {
  const cp = Number(lossCp || 0);
  if (cp > 200) return { suffix: '??', cls: 'played-severe' };
  if (cp >= 100) return { suffix: '?', cls: 'played-mistake' };
  return { suffix: '?!', cls: 'played-inaccuracy' };
}

function setPlayedMoveMetric(card) {
  if (!attemptPlayedMoveEl) return;
  const san = card?.played_san || '-';
  if (!card || !san || san === '-') {
    attemptPlayedMoveEl.textContent = '-';
    attemptPlayedMoveEl.classList.remove('played-severe', 'played-mistake', 'played-inaccuracy');
    return;
  }
  const sev = playedMoveSeverity(card.loss_cp);
  attemptPlayedMoveEl.textContent = `${san}${sev.suffix}`;
  attemptPlayedMoveEl.classList.remove('played-severe', 'played-mistake', 'played-inaccuracy');
  attemptPlayedMoveEl.classList.add(sev.cls);
}

function playedMoveArrowBrush(card) {
  const j = String(card?.judgement || '').toLowerCase();
  if (j === 'blunder') return 'red';
  if (j === 'mistake') return 'yellow';
  if (j === 'inaccuracy') return 'blue';
  const cp = Number(card?.loss_cp || 0);
  if (cp > 200) return 'red';
  if (cp >= 100) return 'yellow';
  return 'blue';
}

function playedMoveShape(card) {
  let u = String(card?.played_uci || '');
  if (u.length < 4 && ChessCtor && card?.fen && card?.played_san) {
    try {
      const b = new ChessCtor(card.fen);
      const target = String(card.played_san).replace(/[+#?!]+/g, '');
      const m = b
        .moves({ verbose: true })
        .find((x) => String(x.san || '').replace(/[+#?!]+/g, '') === target);
      if (m) u = `${m.from}${m.to}${m.promotion || ''}`;
    } catch {}
  }
  if (u.length < 4) return null;
  return {
    orig: u.slice(0, 2),
    dest: u.slice(2, 4),
    brush: playedMoveArrowBrush(card),
  };
}

function isHidePlayedMoveEnabled() {
  return Boolean($('hidePlayedMove')?.checked);
}

function syncPlayedMoveMetric() {
  const concealed = isHidePlayedMoveEnabled() && !answerShown;
  if (attemptPlayedRowEl) attemptPlayedRowEl.hidden = concealed;
  if (concealed) {
    setPlayedMoveMetric(null);
  } else {
    setPlayedMoveMetric(currentCard);
  }
  syncBoardArrows();
}

function setAnswerEvalMetric(cp) {
  if (!answerEvalMetricEl) return;
  answerEvalMetricEl.textContent = fmtEval(cp);
  answerEvalMetricEl.classList.remove('answer-eval-pos', 'answer-eval-neg', 'answer-eval-neutral');
  if (cp === null || cp === undefined) return;
  if (Number(cp) < 0) answerEvalMetricEl.classList.add('answer-eval-neg');
  else if (Number(cp) > 0) answerEvalMetricEl.classList.add('answer-eval-pos');
  else answerEvalMetricEl.classList.add('answer-eval-neutral');
}

function log(msg) {
  if (!logEl) return;
  logEl.textContent = `${new Date().toISOString()} ${msg}\n${logEl.textContent}`;
}

function fenTurn(fen) {
  const t = String(fen || '').split(' ')[1];
  return t === 'b' ? 'black' : 'white';
}

function parsePgnHeadersLocal(pgn) {
  const h = {};
  for (const line of String(pgn || '').split('\n')) {
    const m = line.match(/^\[([A-Za-z0-9_]+)\s+"(.*)"\]$/);
    if (!m) continue;
    h[m[1]] = m[2];
  }
  return h;
}

function isSupportedPgnVariant(pgn) {
  const h = parsePgnHeadersLocal(pgn);
  const variant = String(h.Variant || '').trim().toLowerCase();
  return !variant || variant === 'standard';
}

function moveSafe(board, moveObj) {
  try {
    return board.move(moveObj);
  } catch {
    return null;
  }
}

function scoreToCp(score) {
  if (!score) return null;
  if (score.kind === 'cp') return Number(score.value);
  if (score.kind === 'mate') return Number(score.value) > 0 ? 100000 : -100000;
  return null;
}

function scoreForPov(cpFromTurn, turnSide, povSide) {
  if (cpFromTurn === null || cpFromTurn === undefined) return null;
  if (!povSide || povSide === turnSide) return cpFromTurn;
  return -cpFromTurn;
}

function winningChancesFromCp(cp) {
  const MULTIPLIER = -0.00368208;
  const x = Number(cp || 0);
  const out = (2 / (1 + Math.exp(MULTIPLIER * x))) - 1;
  if (out < -1) return -1;
  if (out > 1) return 1;
  return out;
}

function classifyByWinningChanceDelta(bestCp, playedCp) {
  const b = Number(bestCp);
  const p = Number(playedCp);
  if (!Number.isFinite(b) || !Number.isFinite(p)) return { judgement: null, delta: 0 };
  const delta = winningChancesFromCp(b) - winningChancesFromCp(p);
  if (delta >= 0.3) return { judgement: 'blunder', delta };
  if (delta >= 0.2) return { judgement: 'mistake', delta };
  if (delta >= 0.1) return { judgement: 'inaccuracy', delta };
  return { judgement: null, delta };
}

function parseInfoLine(line) {
  if (!line.startsWith('info ')) return null;
  const pvMatch = line.match(/\spv\s+(.+)$/);
  if (!pvMatch) return null;
  const mpMatch = line.match(/\smultipv\s+(\d+)/);
  const cpMatch = line.match(/\sscore\s+cp\s+(-?\d+)/);
  const mateMatch = line.match(/\sscore\s+mate\s+(-?\d+)/);
  const multipv = Number(mpMatch?.[1] || 1);
  let score = null;
  if (cpMatch) score = { kind: 'cp', value: Number(cpMatch[1]) };
  else if (mateMatch) score = { kind: 'mate', value: Number(mateMatch[1]) };
  if (!score) return null;
  const pv = pvMatch[1].trim().split(/\s+/).filter(Boolean);
  if (!pv.length) return null;
  return { multipv, score, pv };
}

function bootStockfishWorker(url, workerType = 'module') {
  return new Promise((resolve, reject) => {
    let worker = null;
    try {
      worker = new Worker(url, { type: workerType });
    } catch (e) {
      reject(e);
      return;
    }

    let stage = 0;
    const timeout = setTimeout(() => {
      cleanup();
      worker?.terminate();
      reject(new Error(`worker init timeout (${workerType}): ${url}`));
    }, 15000);

    const onMsg = (ev) => {
      const line = String(ev.data || '');
      if (stage === 0 && line.includes('uciok')) {
        stage = 1;
        worker?.postMessage('isready');
        return;
      }
      if (stage === 1 && line.includes('readyok')) {
        cleanup();
        resolve(worker);
      }
    };

    const onErr = (ev) => {
      cleanup();
      worker?.terminate();
      reject(new Error(`worker init error (${workerType}): ${url} ${ev?.message || ''}`.trim()));
    };

    const cleanup = () => {
      clearTimeout(timeout);
      worker?.removeEventListener('message', onMsg);
      worker?.removeEventListener('error', onErr);
    };

    worker.addEventListener('message', onMsg);
    worker.addEventListener('error', onErr);
    worker.postMessage('uci');
  });
}

async function resolveStockfishCandidates() {
  return SF_WORKER_CANDIDATES;
}

function ensureStockfishWorker() {
  if (sfInitState === 'ready' && sfInitPromise) return sfInitPromise;
  if (sfInitState === 'failed') return Promise.reject(sfInitError || new Error('Stockfish init failed'));
  if (sfInitPromise) return sfInitPromise;
  sfInitState = 'loading';
  sfInitError = null;
  setEngineStatus('Stockfish: loading...', 'busy');
  sfInitPromise = (async () => {
    let lastErr = null;
    const candidates = await resolveStockfishCandidates();
    for (const url of candidates) {
      for (const workerType of ['module', 'classic']) {
        try {
          const worker = await bootStockfishWorker(url, workerType);
          sfWorker = worker;
          sfInitState = 'ready';
          setEngineStatus('Stockfish: ready.', 'ok');
          log(`Stockfish ready (${workerType}): ${url}`);
          return;
        } catch (e) {
          lastErr = e;
          log(`Stockfish candidate failed (${workerType}): ${url}`);
        }
      }
    }
    sfInitState = 'failed';
    sfInitError = lastErr || new Error('No Stockfish worker available');
    sfInitPromise = Promise.reject(sfInitError);
    sfInitPromise.catch(() => {});
    sfWorker = null;
    setEngineStatus('Stockfish: failed.', 'error');
    throw sfInitError;
  })();
  return sfInitPromise;
}

function stockfishAnalyze(fen, { depth = 12, multipv = 1 } = {}) {
  sfQueue = sfQueue.catch(() => {}).then(async () => {
    await ensureStockfishWorker();
    return await new Promise((resolve, reject) => {
      const infos = new Map();
      const timeout = setTimeout(() => {
        cleanup();
        try {
          sfWorker?.postMessage('stop');
        } catch {}
        reject(new Error('Stockfish timeout'));
      }, 20000);

      const onMsg = (ev) => {
        const line = String(ev.data || '');
        if (line.startsWith('bestmove')) {
          cleanup();
          const out = Array.from(infos.values()).sort((a, b) => a.multipv - b.multipv);
          resolve(out);
          return;
        }
        const p = parseInfoLine(line);
        if (!p) return;
        infos.set(p.multipv, p);
      };

      const cleanup = () => {
        clearTimeout(timeout);
        sfWorker?.removeEventListener('message', onMsg);
      };

      sfWorker?.addEventListener('message', onMsg);
      sfWorker?.postMessage('stop');
      sfWorker?.postMessage('ucinewgame');
      sfWorker?.postMessage('setoption name MultiPV value ' + Math.max(1, Number(multipv || 1)));
      sfWorker?.postMessage('position fen ' + fen);
      sfWorker?.postMessage('go depth ' + Math.max(4, Number(depth || 12)));
    });
  });
  return sfQueue;
}

async function evaluateFenCpWasm(fen, { depth = 15, povSide = null } = {}) {
  const lines = await stockfishAnalyze(fen, { depth, multipv: 1 });
  const first = lines[0];
  if (!first) return null;
  const cpFromTurn = scoreToCp(first.score);
  return scoreForPov(cpFromTurn, fenTurn(fen), povSide);
}

async function evaluateFenLinesWasm(fen, { depth = 12, multipv = 4, cpWindow = 30, povSide = null } = {}) {
  const infos = await stockfishAnalyze(fen, { depth, multipv });
  if (!infos.length) return [];
  const turn = fenTurn(fen);
  const lines = infos
    .map((x) => {
      const cpFromTurn = scoreToCp(x.score);
      const cp = scoreForPov(cpFromTurn, turn, povSide);
      return {
        rank: x.multipv,
        cp,
        first_move_uci: x.pv[0],
      };
    })
    .filter((x) => x.first_move_uci && x.cp !== null && x.cp !== undefined)
    .sort((a, b) => a.rank - b.rank);
  if (!lines.length) return [];
  const bestCp = Number(lines[0].cp);
  return lines.filter((l) => (bestCp - Number(l.cp)) <= Number(cpWindow || 30));
}

async function evaluateFenCp(fen, { depth = 15, povSide = null } = {}) {
  try {
    return await evaluateFenCpWasm(fen, { depth, povSide });
  } catch (e) {
    log(`WASM eval failed, fallback API: ${e.message}`);
    setEngineStatus('Stockfish: API fallback.', 'busy');
    try {
      const out = await postJson('/api/eval', { fen, depth, pov_side: povSide });
      setEngineStatus('Stockfish: API fallback.', 'ok');
      return Number(out?.cp);
    } catch (apiErr) {
      setEngineStatus('Stockfish: failed.', 'error');
      throw apiErr;
    }
  }
}

async function evaluateFenLines(fen, { depth = 12, multipv = 4, cpWindow = 30, povSide = null } = {}) {
  try {
    return await evaluateFenLinesWasm(fen, { depth, multipv, cpWindow, povSide });
  } catch (e) {
    log(`WASM lines failed, fallback API: ${e.message}`);
    setEngineStatus('Stockfish: API fallback.', 'busy');
    try {
      const out = await postJson('/api/reply-lines', {
        fen,
        depth,
        multipv,
        cp_window: cpWindow,
        pov_side: povSide,
      });
      setEngineStatus('Stockfish: API fallback.', 'ok');
      return Array.isArray(out?.lines) ? out.lines : [];
    } catch (apiErr) {
      setEngineStatus('Stockfish: failed.', 'error');
      throw apiErr;
    }
  }
}

function setStatus(el, msg, type = 'idle') {
  if (!el) return;
  const text = String(msg || '').trim();
  const hideWhenIdle = (el === importStatusEl || el === analyzeStatusEl || el === engineStatusEl) && type === 'idle';
  const hideAnalyzeErrors = el === analyzeErrorsEl && (type !== 'error' || !text);
  el.hidden = hideWhenIdle || hideAnalyzeErrors;
  el.textContent = text;
  el.dataset.type = type;
}

const setImportStatus = (msg, type = 'idle') => setStatus(importStatusEl, msg, type);
const setAnalyzeStatus = (msg, type = 'idle') => setStatus(analyzeStatusEl, msg, type);
const setAnalyzeErrors = (msg, type = 'idle') => setStatus(analyzeErrorsEl, msg, type);
const setReviewMoveStatus = (msg, type = 'idle') => setStatus(reviewMoveStatusEl, msg, type);
const setEngineStatus = (msg, type = 'idle') => setStatus(engineStatusEl, msg, type);

function setBtnBusy(btn, busy, textWhenBusy) {
  if (!btn) return;
  if (busy) {
    btn.dataset.originalText = btn.textContent;
    btn.textContent = textWhenBusy;
    btn.disabled = true;
  } else {
    btn.textContent = btn.dataset.originalText || btn.textContent;
    btn.disabled = false;
  }
}

function selectedUser() {
  return localStorage.getItem(STORAGE_USER_KEY) || '';
}

function setSelectedUser(username) {
  if (!username) return;
  localStorage.setItem(STORAGE_USER_KEY, username);
  for (const id of ['userSelectAnalyze', 'userSelectReview', 'userSelectStats']) {
    const s = $(id);
    if (s) s.value = username;
  }
}

function parseStoredSeverityFilter(raw) {
  let parsed = null;
  try {
    parsed = raw ? JSON.parse(raw) : null;
  } catch {
    parsed = null;
  }
  if (!parsed || typeof parsed !== 'object') return { ...DEFAULT_SEVERITY_FILTER };
  return {
    inaccuracy: Boolean(parsed.inaccuracy),
    mistake: Boolean(parsed.mistake),
    blunder: parsed.blunder === undefined ? true : Boolean(parsed.blunder),
  };
}

function getStoredSeverityFilter() {
  return parseStoredSeverityFilter(localStorage.getItem(STORAGE_SEVERITY_FILTER_KEY));
}

function saveSeverityFilter(filter) {
  localStorage.setItem(
    STORAGE_SEVERITY_FILTER_KEY,
    JSON.stringify({
      inaccuracy: Boolean(filter?.inaccuracy),
      mistake: Boolean(filter?.mistake),
      blunder: Boolean(filter?.blunder),
    })
  );
}

function severityFilterFromDom() {
  const stored = getStoredSeverityFilter();
  const map = [
    ['reviewShowInaccuracy', 'inaccuracy'],
    ['reviewShowMistake', 'mistake'],
    ['reviewShowBlunder', 'blunder'],
    ['analyzeShowInaccuracy', 'inaccuracy'],
    ['analyzeShowMistake', 'mistake'],
    ['analyzeShowBlunder', 'blunder'],
  ];
  const out = { ...stored };
  let found = false;
  for (const [id, key] of map) {
    const el = $(id);
    if (!el) continue;
    out[key] = Boolean(el.checked);
    found = true;
  }
  return found ? out : stored;
}

function applySeverityFilterToDom(filter) {
  const f = {
    inaccuracy: Boolean(filter?.inaccuracy),
    mistake: Boolean(filter?.mistake),
    blunder: Boolean(filter?.blunder),
  };
  const map = [
    ['reviewShowInaccuracy', f.inaccuracy],
    ['reviewShowMistake', f.mistake],
    ['reviewShowBlunder', f.blunder],
    ['analyzeShowInaccuracy', f.inaccuracy],
    ['analyzeShowMistake', f.mistake],
    ['analyzeShowBlunder', f.blunder],
  ];
  for (const [id, value] of map) {
    const el = $(id);
    if (el) el.checked = value;
  }
}

function severityFilterQuery() {
  const f = severityFilterFromDom();
  const excludeLost =
    $('reviewExcludeLost')?.checked ??
    $('analyzeExcludeLost')?.checked ??
    (localStorage.getItem(STORAGE_EXCLUDE_LOST_KEY) === '1');
  const q = new URLSearchParams();
  q.set('show_inaccuracy', f.inaccuracy ? '1' : '0');
  q.set('show_mistake', f.mistake ? '1' : '0');
  q.set('show_blunder', f.blunder ? '1' : '0');
  q.set('exclude_lost', excludeLost ? '1' : '0');
  return q.toString();
}

async function loadSessionMetricsForUser(username) {
  const zero = { reviewed: 0, attempts: 0, correct: 0, wrong: 0, streak: 0, bestStreak: 0 };
  if (!username) {
    Object.assign(sessionMetrics, zero);
    updateSessionMetricsUI();
    return;
  }
  try {
    const res = await fetch(`/api/stats/session/${encodeURIComponent(username)}?break_minutes=60`);
    if (!res.ok) throw new Error(`session stats ${res.status}`);
    const parsed = await res.json();
    Object.assign(sessionMetrics, {
      reviewed: Number(parsed?.reviewed || 0),
      attempts: Number(parsed?.attempts || 0),
      correct: Number(parsed?.correct || 0),
      wrong: Number(parsed?.wrong || 0),
      streak: Number(parsed?.streak || 0),
      bestStreak: Number(parsed?.bestStreak || 0),
    });
  } catch {
    Object.assign(sessionMetrics, zero);
  }
  updateSessionMetricsUI();
}

function isAutoGradeEnabled() {
  return Boolean($('autoGradeMode')?.checked);
}

function isOpponentResponseEnabled() {
  return Boolean($('opponentResponseMode')?.checked);
}

function isShowPositionEvalEnabled() {
  return Boolean($('showPositionEval')?.checked);
}

function storedBoardTheme() {
  const v = String(localStorage.getItem(STORAGE_BOARD_THEME_KEY) || 'brown').toLowerCase();
  return ['brown', 'blue', 'green', 'slate'].includes(v) ? v : 'brown';
}

function currentBoardTheme() {
  const v = String($('reviewBoardTheme')?.value || storedBoardTheme()).toLowerCase();
  return ['brown', 'blue', 'green', 'slate'].includes(v) ? v : 'brown';
}

function storedPieceSet() {
  const v = String(localStorage.getItem(STORAGE_PIECE_SET_KEY) || 'cburnett').toLowerCase();
  return PIECE_SETS.includes(v) ? v : 'cburnett';
}

function currentPieceSet() {
  const v = String($('reviewPieceSet')?.value || storedPieceSet()).toLowerCase();
  return PIECE_SETS.includes(v) ? v : 'cburnett';
}

function pieceSvgUrl(setName, pieceCode) {
  return `https://lichess1.org/assets/piece/${setName}/${pieceCode}.svg`;
}

function detectPieceSetAvailable(setName) {
  if (pieceSetAvailableCache.has(setName)) return pieceSetAvailableCache.get(setName);
  const p = new Promise((resolve) => {
    let done = false;
    const finish = (ok) => {
      if (done) return;
      done = true;
      resolve(ok);
    };
    const img = new Image();
    const timer = setTimeout(() => finish(false), 3500);
    img.onload = () => {
      clearTimeout(timer);
      finish(true);
    };
    img.onerror = () => {
      clearTimeout(timer);
      finish(false);
    };
    img.src = pieceSvgUrl(setName, 'wK');
  });
  pieceSetAvailableCache.set(setName, p);
  return p;
}

async function applyPieceSet() {
  const boardEl = $('board');
  if (!boardEl) return;
  const seq = ++pieceSetApplySeq;
  let setName = currentPieceSet();
  const ok = await detectPieceSetAvailable(setName);
  if (seq !== pieceSetApplySeq) return;
  if (!ok) {
    setName = 'cburnett';
    localStorage.setItem(STORAGE_PIECE_SET_KEY, setName);
    const sel = $('reviewPieceSet');
    if (sel) sel.value = setName;
  }
  boardEl.dataset.pieceSet = setName;
}

function applyBoardTheme() {
  const boardEl = $('board');
  if (!boardEl) return;
  boardEl.dataset.theme = currentBoardTheme();
}

function syncPositionEvalMetric() {
  if (!currentCard) {
    setAnswerEvalMetric(null);
    return;
  }
  if (isShowPositionEvalEnabled()) {
    setAnswerEvalMetric(currentCard.best_cp ?? null);
    return;
  }
  if (!answerShown) {
    setAnswerEvalMetric(null);
  }
}

function clearAutoProceedTimer() {
  if (autoProceedTimer) {
    clearTimeout(autoProceedTimer);
    autoProceedTimer = null;
  }
  if (autoNextTickTimer) {
    clearInterval(autoNextTickTimer);
    autoNextTickTimer = null;
  }
  if (autoNextWrapEl) autoNextWrapEl.hidden = true;
}

function fmtEval(cp) {
  if (cp === null || cp === undefined) return '-';
  if (Math.abs(cp) >= 90000) return cp > 0 ? 'M+' : 'M-';
  const v = cp / 100;
  return `${v >= 0 ? '+' : ''}${v.toFixed(2)}`;
}

function cardHash6(cardId) {
  const n = Number(cardId || 0) >>> 0;
  let x = (n * 1664525 + 1013904223) >>> 0;
  x ^= x >>> 15;
  return x.toString(36).toUpperCase().padStart(6, '0').slice(-6);
}

function findLineByUci(uci) {
  return (currentCard?.all_lines || []).find((l) => l.first_move_uci === uci);
}

function currentAcceptWindow() {
  const reviewW = $('reviewAcceptWindow');
  if (reviewW) return Number(reviewW.value || 30);
  return Number($('cpWindow')?.value || 30);
}

function acceptableLineSet() {
  if (!currentCard?.all_lines?.length) return new Set();
  const w = currentAcceptWindow();
  const bestCp = currentCard.best_cp ?? currentCard.all_lines[0].cp;
  const s = new Set();
  for (const l of currentCard.all_lines) {
    if ((bestCp - l.cp) <= w) s.add(l.first_move_uci);
  }
  return s;
}

function setShowAnswerButtonState() {
  const btn = $('showAnswer');
  if (!btn) return;
  btn.innerHTML = answerShown
    ? '<i class="btn-icon bi bi-arrow-repeat"></i><span>Reset</span>'
    : '<i class="btn-icon bi bi-eye"></i><span>Show Answer</span>';
}

function formatElapsed(seconds) {
  const s = Math.max(0, Math.floor(seconds));
  const m = Math.floor(s / 60);
  const rem = s % 60;
  return `${String(m).padStart(2, '0')}:${String(rem).padStart(2, '0')}`;
}

function stopCardTimer() {
  if (cardTimerInterval) {
    clearInterval(cardTimerInterval);
    cardTimerInterval = null;
  }
}

function tickCardTimer() {
  if (!attemptTimerEl) return;
  if (!cardStartedAt) {
    attemptTimerEl.textContent = '00:00';
    return;
  }
  const elapsed = (Date.now() - cardStartedAt) / 1000;
  attemptTimerEl.textContent = formatElapsed(elapsed);
}

function startCardTimer() {
  stopCardTimer();
  tickCardTimer();
  cardTimerInterval = setInterval(tickCardTimer, 1000);
}

function hidePromotionPicker() {
  promotionRequestSeq += 1;
  if (promotionPickerEl) promotionPickerEl.hidden = true;
}

function promotionChoices(orig, dest) {
  if (!positionChess) return [];
  const promos = new Set();
  for (const m of positionChess.moves({ verbose: true })) {
    if (m.from === orig && m.to === dest && m.promotion) promos.add(m.promotion);
  }
  return Array.from(promos);
}

function choosePromotionPiece(pieces) {
  if (!promotionPickerEl || !pieces.length) return Promise.resolve(null);
  const seq = ++promotionRequestSeq;
  promotionPickerEl.hidden = false;
  const buttons = Array.from(promotionPickerEl.querySelectorAll('button[data-piece]'));
  for (const b of buttons) b.hidden = !pieces.includes(b.dataset.piece);

  return new Promise((resolve) => {
    const onClick = (ev) => {
      if (seq !== promotionRequestSeq) return;
      const piece = ev.currentTarget?.dataset?.piece || null;
      cleanup();
      resolve(piece);
    };
    const cleanup = () => {
      for (const b of buttons) b.removeEventListener('click', onClick);
      if (seq === promotionRequestSeq) promotionPickerEl.hidden = true;
    };
    for (const b of buttons) b.addEventListener('click', onClick);
  });
}

function openReviewSettings() {
  const o = $('reviewSettingsOverlay');
  if (!o) return;
  o.hidden = false;
}

function closeReviewSettings() {
  const o = $('reviewSettingsOverlay');
  if (!o) return;
  o.hidden = true;
}

function openAnalyzeSettings() {
  const o = $('analyzeSettingsOverlay');
  if (!o) return;
  o.hidden = false;
}

function closeAnalyzeSettings() {
  const o = $('analyzeSettingsOverlay');
  if (!o) return;
  o.hidden = true;
}

function openAnalyzeClearConfirm() {
  const o = $('analyzeClearOverlay');
  if (!o) return;
  o.hidden = false;
}

function closeAnalyzeClearConfirm() {
  const o = $('analyzeClearOverlay');
  if (!o) return;
  o.hidden = true;
}

function openImportClearConfirm() {
  const o = $('importClearOverlay');
  if (!o) return;
  o.hidden = false;
}

function closeImportClearConfirm() {
  const o = $('importClearOverlay');
  if (!o) return;
  o.hidden = true;
}

function acceptableLines() {
  const allowed = acceptableLineSet();
  return (currentCard?.all_lines || []).filter((l) => allowed.has(l.first_move_uci));
}

function alternativeShapes() {
  const lines = acceptableLines();
  if (!lines.length) return [];
  const seen = new Set();
  const out = [];
  for (const l of lines) {
    const u = l.first_move_uci || '';
    if (u.length < 4 || seen.has(u) || (playedMoveUci && u === playedMoveUci)) continue;
    seen.add(u);
    out.push({
      orig: u.slice(0, 2),
      dest: u.slice(2, 4),
      brush: 'blue',
    });
  }
  return out;
}

function answerLineShapes() {
  const lines = acceptableLines();
  if (!lines.length) return [];
  const seen = new Set();
  const out = [];
  for (let i = 0; i < lines.length; i += 1) {
    const u = String(lines[i]?.first_move_uci || '');
    if (u.length < 4 || seen.has(u)) continue;
    seen.add(u);
    out.push({
      orig: u.slice(0, 2),
      dest: u.slice(2, 4),
      brush: i === 0 ? 'blue' : 'green',
    });
  }
  return out;
}

function computeBoardShapes() {
  const shapes = [];
  const showOpponentArrows = ((!isOpponentResponseEnabled() || answerRevealed) && opponentReplyShapes.length > 0);
  if (!showOpponentArrows && !answerRevealed && !isHidePlayedMoveEnabled() && currentCard) {
    const shape = playedMoveShape(currentCard);
    if (shape) shapes.push(shape);
  }
  if (answerRevealed) shapes.push(...answerLineShapes());
  if (showOpponentArrows) shapes.push(...opponentReplyShapes);
  return shapes;
}

function syncBoardArrows() {
  if (!cg) return;
  const shapes = computeBoardShapes();
  cg.set({ drawable: { autoShapes: shapes } });
}

function clearBoardDecorations() {
  replyRequestSeq += 1;
  opponentReplyShapes = [];
  awaitingOpponentResponse = false;
  opponentResponseMoves = new Set();
  opponentPhaseFen = null;
  hidePromotionPicker();
  clearAutoProceedTimer();
  if (!cg) return;
  cg.set({
    lastMove: null,
    drawable: { autoShapes: [] },
  });
}

function resetOpponentReplyArrows() {
  opponentReplyShapes = [];
  syncBoardArrows();
}

async function fetchOpponentReplyArrows(fen, cardId) {
  const seq = ++replyRequestSeq;
  try {
    const lines = await evaluateFenLines(fen, { depth: 12, multipv: 4, cpWindow: 30 });
    if (seq !== replyRequestSeq) return false;
    if (!currentCard || currentCard.card_id !== cardId) return false;
    opponentResponseMoves = new Set(lines.map((l) => String(l.first_move_uci || '')).filter((u) => u.length >= 4));
    opponentReplyShapes = lines
      .map((l, idx) => {
        const u = l.first_move_uci || '';
        if (u.length < 4) return null;
        return {
          orig: u.slice(0, 2),
          dest: u.slice(2, 4),
          brush: idx === 0 ? 'red' : 'yellow',
        };
      })
      .filter(Boolean);
    syncBoardArrows();
    return true;
  } catch (e) {
    log(`Replies failed: ${e.message}`);
    return false;
  }
}

function resetAttemptBox() {
  attempts = [];
  wrongAttemptsThisCard = 0;
  cardStartedAt = Date.now();
  startCardTimer();
  clearAutoProceedTimer();
  answerShown = false;
  answerRevealed = false;
  if (attemptCardIdEl) attemptCardIdEl.textContent = currentCard ? `#${cardHash6(currentCard.card_id)}` : '-';
  syncPlayedMoveMetric();
  syncPositionEvalMetric();
  if (attemptListEl) attemptListEl.innerHTML = '';
  awaitingOpponentResponse = false;
  opponentResponseMoves = new Set();
  opponentPhaseFen = null;
  opponentReplyShapes = [];
  setShowAnswerButtonState();
  if (cg) {
    cg.set({
      lastMove: null,
      drawable: { autoShapes: [] },
    });
  }
}

function renderAttempts() {
  if (!attemptListEl) return;
  if (!attempts.length) {
    attemptListEl.innerHTML = '<li><span class=\"attempt-move\">-</span><span class=\"attempt-eval\">-</span></li>';
    return;
  }
  attemptListEl.innerHTML = attempts
    .map((a, i) => {
      const cls =
        a.evalCp === null || a.evalCp === undefined
          ? 'attempt-eval'
          : a.evalCp < 0
            ? 'attempt-eval attempt-eval-neg'
            : a.evalCp > 0
              ? 'attempt-eval attempt-eval-pos'
              : 'attempt-eval attempt-eval-neutral';
      return `<li><span class=\"attempt-move\">${i + 1}. ${a.san}</span><span class=\"${cls}\">${fmtEval(a.evalCp)}</span></li>`;
    })
    .join('');
}

function activeUsername() {
  return (
    $('userSelectReview')?.value ||
    $('userSelectAnalyze')?.value ||
    $('reviewUser')?.value?.trim() ||
    $('analyzeUser')?.value?.trim() ||
    selectedUser()
  );
}

function percent(correct, total) {
  return total ? `${Math.round((correct / total) * 100)}%` : '0%';
}

function updateSessionMetricsUI() {
  if (metricReviewedEl) metricReviewedEl.textContent = String(sessionMetrics.reviewed);
  if (metricAccuracyEl) metricAccuracyEl.textContent = percent(sessionMetrics.correct, sessionMetrics.attempts);
  if (metricStreakEl) metricStreakEl.textContent = String(sessionMetrics.streak);
  if (metricBestStreakEl) metricBestStreakEl.textContent = String(sessionMetrics.bestStreak);
}

function formatDueDelta(targetTs) {
  const due = new Date(targetTs.replace(' ', 'T') + 'Z');
  const now = new Date();
  const sec = Math.max(0, Math.round((due.getTime() - now.getTime()) / 1000));
  if (sec < 60) return '<1m';
  const mins = Math.round(sec / 60);
  if (mins < 60) return `${mins}m`;
  const hours = Math.round(mins / 60);
  if (hours < 24) return `${hours}h`;
  const days = Math.round(hours / 24);
  if (days < 30) return `${days}d`;
  const months = Math.round(days / 30);
  if (months < 12) return `${months}mo`;
  const years = Math.round(months / 12);
  return `${years}y`;
}

function setGradeButtonsDefault() {
  for (const btn of document.querySelectorAll('.grade')) {
    const rating = Number(btn.dataset.grade || 0);
    btn.textContent = gradeBaseLabels[rating] || btn.textContent;
  }
}

function setReviewControlsEnabled(enabled) {
  const on = Boolean(enabled);
  const ids = ['showAnswer', 'openLichessAnalysis', 'viewSourceGame', 'autoNextCancel'];
  for (const id of ids) {
    const el = $(id);
    if (el) el.disabled = !on;
  }
  for (const btn of document.querySelectorAll('.grade')) {
    btn.disabled = !on;
  }
}

function buildSourceGameUrl(card) {
  if (!card) return '';
  const src = String(card.source || '').toLowerCase();
  if (src === 'pgn') return '';
  const id = String(card.source_game_id || '').trim();
  const sourceUrl = String(card.source_url || '').trim();
  const ply = Number(card.ply || 0);
  const targetPly = ply > 0 ? Math.max(1, ply - 2) : 0;
  const isAbsoluteHttpUrl = /^https?:\/\//i.test(sourceUrl);

  if (src === 'lichess') {
    if (isAbsoluteHttpUrl) return `${sourceUrl}${targetPly > 0 ? `#${targetPly}` : ''}`;
    if (!id) return '';
    return `https://lichess.org/${encodeURIComponent(id)}${targetPly > 0 ? `#${targetPly}` : ''}`;
  }

  if (src === 'chesscom') {
    const gameIdMatch =
      sourceUrl.match(/(?:^https?:\/\/(?:www\.)?chess\.com\/(?:analysis\/)?game\/live\/)(\d+)/i) ||
      id.match(/(?:^https?:\/\/(?:www\.)?chess\.com\/(?:analysis\/)?game\/live\/)(\d+)/i) ||
      id.match(/^(\d+)$/);
    if (gameIdMatch?.[1]) {
      const base = `https://www.chess.com/analysis/game/live/${gameIdMatch[1]}/analysis`;
      return targetPly > 0 ? `${base}?move=${targetPly}` : base;
    }
    if (!isAbsoluteHttpUrl) return '';
    if (targetPly > 0) {
      const sep = sourceUrl.includes('?') ? '&' : '?';
      return `${sourceUrl}${sep}move=${targetPly}`;
    }
    return sourceUrl;
  }

  return '';
}

async function refreshGradePreviewLabels() {
  if (!currentCard?.card_id) {
    setGradeButtonsDefault();
    return;
  }
  try {
    const res = await fetch(`/api/review/preview/${currentCard.card_id}`);
    if (!res.ok) throw new Error(`preview ${res.status}`);
    const data = await res.json();
    const byRating = data?.due_by_rating || {};
    for (const btn of document.querySelectorAll('.grade')) {
      const rating = Number(btn.dataset.grade || 0);
      const base = gradeBaseLabels[rating] || btn.textContent;
      const due = byRating[String(rating)];
      btn.textContent = due ? `${base} (${formatDueDelta(due)})` : base;
    }
  } catch (e) {
    log(`Preview failed: ${e.message}`);
    setGradeButtonsDefault();
  }
}

function renderBarChartRows(containerEl, rows, { okBars = false } = {}) {
  if (!containerEl) return;
  if (!rows.length) {
    containerEl.innerHTML = '<p class="status">No data yet.</p>';
    return;
  }
  const max = Math.max(...rows.map((r) => Number(r.value || 0)), 1);
  containerEl.innerHTML = rows
    .map((r) => {
      const pct = Math.max(4, Math.round((Number(r.value || 0) / max) * 100));
      return `
        <div class="chart-row">
          <span class="chart-label">${r.label}</span>
          <span class="chart-track"><span class="chart-bar${okBars ? ' ok' : ''}" style="width:${pct}%"></span></span>
          <span class="chart-value">${r.valueText ?? String(r.value ?? 0)}</span>
        </div>
      `;
    })
    .join('');
}

const statsChartInstances = {};

function upsertChart(canvasId, cfg) {
  const canvas = $(canvasId);
  if (!canvas) return;
  const ChartCtor = window.Chart;
  if (!ChartCtor) return;
  if (statsChartInstances[canvasId]) {
    statsChartInstances[canvasId].destroy();
    delete statsChartInstances[canvasId];
  }
  statsChartInstances[canvasId] = new ChartCtor(canvas, cfg);
}

async function loadStatsPage() {
  const sel = $('userSelectStats');
  const username = sel?.value || selectedUser();
  if (!username) return;
  const days = Number($('statsDays')?.value || 60);
  const res = await fetch(`/api/stats/anki/${encodeURIComponent(username)}?days=${encodeURIComponent(String(days))}`);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  const data = await res.json();
  const s = data.summary || {};

  const setTxt = (id, v) => {
    const el = $(id);
    if (el) el.textContent = String(v);
  };
  setTxt('statsTotalReviews', s.total_reviews ?? 0);
  setTxt('statsRetention', `${Number(s.retention_pct ?? 0).toFixed(1)}%`);
  setTxt('statsAvgInterval', `${Number(s.avg_interval_days ?? 0).toFixed(1)}d`);
  setTxt('statsAgain', s.again ?? 0);
  setTxt('statsHard', s.hard ?? 0);
  setTxt('statsGood', s.good ?? 0);
  setTxt('statsEasy', s.easy ?? 0);

  const byDay = data.by_day || [];
  const dayLabels = byDay.map((r) => r.day?.slice(5) || '-');
  const dayReviews = byDay.map((r) => Number(r.reviews || 0));
  const dayRetention = byDay.map((r) => Number(r.retention_pct || 0));

  const b = data.interval_buckets || {};
  const intervalLabels = ['<1d', '1-3d', '4-7d', '8-30d', '31d+'];
  const intervalValues = intervalLabels.map((k) => Number(b[k] || 0));

  upsertChart('statsChartReviews', {
    type: 'line',
    data: {
      labels: dayLabels,
      datasets: [{ label: 'Reviews', data: dayReviews, borderColor: '#68b0ff', backgroundColor: 'rgba(104,176,255,0.2)', tension: 0.25, fill: true }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { color: '#a9bdd6' }, grid: { color: 'rgba(169,189,214,0.14)' } }, y: { ticks: { color: '#a9bdd6' }, grid: { color: 'rgba(169,189,214,0.14)' } } },
    },
  });

  upsertChart('statsChartRetention', {
    type: 'line',
    data: {
      labels: dayLabels,
      datasets: [{ label: 'Retention %', data: dayRetention, borderColor: '#48c992', backgroundColor: 'rgba(72,201,146,0.2)', tension: 0.25, fill: true }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: {
        x: { ticks: { color: '#a9bdd6' }, grid: { color: 'rgba(169,189,214,0.14)' } },
        y: { min: 0, max: 100, ticks: { color: '#a9bdd6', callback: (v) => `${v}%` }, grid: { color: 'rgba(169,189,214,0.14)' } },
      },
    },
  });

  upsertChart('statsChartButtons', {
    type: 'doughnut',
    data: {
      labels: ['Again', 'Hard', 'Good', 'Easy'],
      datasets: [{ data: [Number(s.again || 0), Number(s.hard || 0), Number(s.good || 0), Number(s.easy || 0)], backgroundColor: ['#de5959', '#c89a4b', '#2ab980', '#3cb7c9'] }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { labels: { color: '#d8e7fa' } } },
    },
  });

  upsertChart('statsChartIntervals', {
    type: 'bar',
    data: {
      labels: intervalLabels,
      datasets: [{ label: 'Cards', data: intervalValues, backgroundColor: 'rgba(104,176,255,0.72)', borderColor: '#68b0ff', borderWidth: 1 }],
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: false } },
      scales: { x: { ticks: { color: '#a9bdd6' }, grid: { color: 'rgba(169,189,214,0.14)' } }, y: { ticks: { color: '#a9bdd6' }, grid: { color: 'rgba(169,189,214,0.14)' } } },
    },
  });
}

function renderUserCards(users) {
  if (!userCardsEl) return;
  const isImportPage = Boolean($('clearAllImportsBtn'));
  if (!users.length) {
    userCardsEl.innerHTML = '<p class="status">No imported users yet.</p>';
    return;
  }

  userCardsEl.innerHTML = users
    .map(
      (u) => `
      <article class="profile-card">
        <h3 class="profile-name">${u.username}:</h3>
        <div class="profile-stats">games ${u.games} • positions ${u.positions} • blunders ${u.blunders} • due ${u.due_cards}</div>
        ${isImportPage ? `<button class="profile-delete-btn danger-btn" data-delete-user="${u.username}" title="Delete profile ${u.username}" aria-label="Delete profile ${u.username}">×</button>` : ''}
      </article>
    `
    )
    .join('');
}

function populateSelect(select, users) {
  if (!select) return;
  const prev = select.value || selectedUser();
  select.innerHTML = '<option value="">Select user</option>';
  for (const u of users) {
    const opt = document.createElement('option');
    opt.value = u.username;
    opt.textContent = u.username;
    select.appendChild(opt);
  }
  if (prev && users.some((u) => u.username === prev)) {
    select.value = prev;
  } else if (users.length) {
    select.value = users[0].username;
  }
  if (select.value) setSelectedUser(select.value);
}

function applyUserStatsToReviewMetrics(username) {
  const u = usersCache.find((x) => x.username === username);
  if (!u) return;
  if (metricBlundersEl) metricBlundersEl.textContent = String(u.blunders);
}

function setQueueMetricsFromStats(s) {
  const newDue = Number(s?.new_due_cards || 0);
  const wrongDue = Number(s?.learn_due_cards ?? s?.wrong_due_cards ?? 0);
  const reviewDue = Number(s?.review_due_cards || 0);
  if (metricQueueNewEl) metricQueueNewEl.textContent = String(Math.max(0, newDue));
  if (metricQueueWrongEl) metricQueueWrongEl.textContent = String(Math.max(0, wrongDue));
  if (metricQueueDueEl) metricQueueDueEl.textContent = String(Math.max(0, reviewDue));
}

async function refreshReviewQueueMetrics(username) {
  if (!username) return;
  try {
    const severityQ = severityFilterQuery();
    const res = await fetch(`/api/stats/${encodeURIComponent(username)}?${severityQ}`);
    if (!res.ok) throw new Error(`stats ${res.status}`);
    const s = await res.json();
    setQueueMetricsFromStats(s);
  } catch (e) {
    log(`Queue metrics fetch failed: ${e.message}`);
  }
}

async function fetchUsers() {
  const severityQ = severityFilterQuery();
  const res = await fetch(`/api/users?${severityQ}`);
  if (!res.ok) throw new Error(`users fetch failed: ${res.status}`);
  const data = await res.json();
  usersCache = data.users || [];
  renderUserCards(usersCache);
  populateSelect($('userSelectAnalyze'), usersCache);
  populateSelect($('userSelectReview'), usersCache);
  populateSelect($('userSelectStats'), usersCache);
  applyUserStatsToReviewMetrics(activeUsername());
}

function clearBoardOverlay() {
  if (!boardOverlayEl) return;
  if (overlayTimer) clearTimeout(overlayTimer);
  boardOverlayEl.textContent = '';
  boardOverlayEl.classList.remove(
    'show',
    'success',
    'fail',
    'grade-again',
    'grade-hard',
    'grade-good',
    'grade-easy'
  );
}

function setBoardDoneOverlay(visible, subtitle = '', extra = '') {
  if (!boardDoneOverlayEl) return;
  const subEl = boardDoneOverlayEl.querySelector('span');
  if (subEl) subEl.textContent = subtitle || 'You have finished this deck for now.';
  const extraEl = $('boardDoneOverlayExtra');
  if (extraEl) {
    const txt = String(extra || '').trim();
    extraEl.textContent = txt;
    extraEl.hidden = !txt;
  }
  boardDoneOverlayEl.hidden = !visible;
}

function clearWrongResetTimer() {
  if (wrongResetTimer) {
    clearTimeout(wrongResetTimer);
    wrongResetTimer = null;
  }
}

function autoGradeRating() {
  const elapsedSec = cardStartedAt ? (Date.now() - cardStartedAt) / 1000 : 0;
  if (wrongAttemptsThisCard >= 2) return 1;
  if (wrongAttemptsThisCard === 1) return 2;
  if (elapsedSec > 10) return 3;
  return 4;
}

function scheduleAutoGrade(cardId) {
  if (!isAutoGradeEnabled()) return;
  const rating = autoGradeRating();
  const labels = { 1: 'Again', 2: 'Hard', 3: 'Good', 4: 'Easy' };
  setReviewMoveStatus(`Auto ${labels[rating]}...`, 'ok');
  scheduleNextCardCountdown(cardId, 1, rating);
}

function scheduleNextCardCountdown(cardId, seconds, rating) {
  clearAutoProceedTimer();
  let remaining = Math.max(1, Number(seconds || 1));
  if (autoNextWrapEl) autoNextWrapEl.hidden = false;
  if (autoNextTextEl) autoNextTextEl.textContent = `Next card in ${remaining}..`;
  autoNextTickTimer = setInterval(() => {
    remaining -= 1;
    if (remaining <= 0) {
      clearInterval(autoNextTickTimer);
      autoNextTickTimer = null;
      return;
    }
    if (autoNextTextEl) autoNextTextEl.textContent = `Next card in ${remaining}..`;
  }, 1000);

  autoProceedTimer = setTimeout(async () => {
    autoProceedTimer = null;
    if (autoNextTickTimer) {
      clearInterval(autoNextTickTimer);
      autoNextTickTimer = null;
    }
    if (autoNextWrapEl) autoNextWrapEl.hidden = true;
    if (!currentCard || currentCard.card_id !== cardId) return;
    try {
      await gradeCard(rating);
    } catch (e) {
      log(`Auto-grade failed: ${e.message}`);
      setReviewMoveStatus('Auto failed.', 'error');
    }
  }, remaining * 1000);
}

function showBoardOverlay(ok) {
  if (!boardOverlayEl) return;
  clearBoardOverlay();
  boardOverlayEl.textContent = ok ? '✓' : '✕';
  boardOverlayEl.classList.add('show', ok ? 'success' : 'fail');
  overlayTimer = setTimeout(clearBoardOverlay, 900);
}

function showGradeOverlay(rating) {
  if (!boardOverlayEl) return;
  clearBoardOverlay();
  const map = {
    1: { cls: 'grade-again', txt: 'Again' },
    2: { cls: 'grade-hard', txt: 'Hard' },
    3: { cls: 'grade-good', txt: 'Good' },
    4: { cls: 'grade-easy', txt: 'Easy' },
  };
  const meta = map[Number(rating)] || { cls: 'grade-good', txt: '' };
  boardOverlayEl.textContent = meta.txt;
  boardOverlayEl.classList.add('show', meta.cls);
  overlayTimer = setTimeout(clearBoardOverlay, 420);
}

function recordMoveAttempt(ok) {
  void ok;
}

async function postJson(url, body) {
  const res = await fetch(url, {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  return res.json();
}

const sleep = (ms) => new Promise((resolve) => setTimeout(resolve, ms));

async function ensureBoardDeps() {
  if (!ChessgroundCtor) ChessgroundCtor = (await import('https://esm.sh/@lichess-org/chessground@10.0.2')).Chessground;
  if (!ChessCtor) ChessCtor = (await import('https://esm.sh/chess.js@1.1.0')).Chess;
}

function setupBoard(card) {
  const boardEl = $('board');
  if (!boardEl) return;
  setBoardDoneOverlay(false);
  void applyPieceSet();
  applyBoardTheme();
  if (!cg) {
    cg = ChessgroundCtor(boardEl, {
      draggable: { enabled: true, showGhost: true },
      animation: { enabled: true, duration: 180 },
      highlight: { lastMove: true, check: true },
      drawable: { enabled: true, visible: true, autoShapes: [] },
      movable: { free: false, color: card.side_to_move, events: { after: onMove } },
    });
  }

  positionChess = new ChessCtor(card.fen);
  playedMoveUci = null;
  moveAttemptRecorded = false;
  clearBoardOverlay();
  hidePromotionPicker();

  const legalDests = new Map();
  for (const m of positionChess.moves({ verbose: true })) {
    if (!legalDests.has(m.from)) legalDests.set(m.from, []);
    legalDests.get(m.from).push(m.to);
  }

  cg.set({
    fen: card.fen.split(' ').slice(0, 4).join(' '),
    orientation: card.side_to_move,
    turnColor: card.side_to_move,
    lastMove: null,
    drawable: { autoShapes: computeBoardShapes() },
    movable: { color: card.side_to_move, dests: legalDests },
  });
}

function syncBoardFromPosition() {
  if (!cg || !positionChess || !currentCard) return;
  const turnColor = positionChess.turn() === 'w' ? 'white' : 'black';
  const legalDests = new Map();
  for (const m of positionChess.moves({ verbose: true })) {
    if (!legalDests.has(m.from)) legalDests.set(m.from, []);
    legalDests.get(m.from).push(m.to);
  }
  cg.set({
    fen: positionChess.fen().split(' ').slice(0, 4).join(' '),
    orientation: currentCard.side_to_move,
    turnColor,
    movable: { color: turnColor, dests: legalDests },
  });
  syncBoardArrows();
}

function resetToCardPosition() {
  if (!currentCard) return;
  clearWrongResetTimer();
  clearBoardDecorations();
  answerShown = false;
  answerRevealed = false;
  awaitingOpponentResponse = false;
  opponentResponseMoves = new Set();
  opponentPhaseFen = null;
  setShowAnswerButtonState();
  setupBoard(currentCard);
  // Extra hard reset pass after board re-render to avoid stale arrows/last-move markers.
  if (cg) {
    cg.set({
      lastMove: null,
      drawable: { autoShapes: [] },
    });
  }
  syncPlayedMoveMetric();
  syncPositionEvalMetric();
}

async function onMove(orig, dest) {
  if (!positionChess || !currentCard) return;
  let promotion = undefined;
  const promoChoices = promotionChoices(orig, dest);
  if (promoChoices.length) {
    promotion = await choosePromotionPiece(promoChoices);
    if (!promotion) return setupBoard(currentCard);
  }

  const move = positionChess.move({ from: orig, to: dest, promotion });
  if (!move) return setupBoard(currentCard);
  playedMoveUci = `${move.from}${move.to}${move.promotion || ''}`;
  const line = findLineByUci(playedMoveUci);
  attempts.push({ san: move.san || playedMoveUci, evalCp: line?.cp ?? null });
  const attemptIdx = attempts.length - 1;
  const fenAfter = positionChess.fen();
  const cardId = currentCard.card_id;
  renderAttempts();
  const inOppPhase = awaitingOpponentResponse;
  let ok = inOppPhase ? opponentResponseMoves.has(playedMoveUci) : acceptableLineSet().has(playedMoveUci);
  if (!inOppPhase && !ok) {
    try {
      const quickCp = await evaluateFenCp(fenAfter, { depth: 15, povSide: currentCard.side_to_move });
      if (!currentCard || currentCard.card_id !== cardId) return;
      if (attempts[attemptIdx]) {
        attempts[attemptIdx].evalCp = quickCp;
        renderAttempts();
      }
      const bestCp = Number(currentCard.best_cp ?? currentCard.all_lines?.[0]?.cp ?? 0);
      if (quickCp !== null && quickCp !== undefined) {
        ok = (bestCp - Number(quickCp)) <= currentAcceptWindow();
      }
    } catch (e) {
      log(`Eval failed: ${e.message}`);
    }
  }
  showBoardOverlay(ok);
  setReviewMoveStatus(ok ? 'Correct.' : 'Wrong.', ok ? 'ok' : 'error');
  clearWrongResetTimer();
  void (inOppPhase ? Promise.resolve(false) : fetchOpponentReplyArrows(fenAfter, cardId)).finally(() => {
    if (!currentCard || currentCard.card_id !== cardId) return;
    if (!ok) {
      setReviewMoveStatus('Wrong. Resetting...', 'error');
      wrongResetTimer = setTimeout(() => {
        if (!currentCard || currentCard.card_id !== cardId) return;
        if (awaitingOpponentResponse && opponentPhaseFen) {
          positionChess = new ChessCtor(opponentPhaseFen);
          syncBoardFromPosition();
          setReviewMoveStatus('Try opponent response again.', 'idle');
        } else {
          clearBoardDecorations();
          setupBoard(currentCard);
          setReviewMoveStatus('Try again.', 'idle');
        }
      }, 1000);
    } else {
      if (!inOppPhase && isOpponentResponseEnabled()) {
        if (!opponentResponseMoves.size) {
          setReviewMoveStatus('Correct. No opponent response found.', 'ok');
          scheduleAutoGrade(cardId);
          return;
        }
        awaitingOpponentResponse = true;
        opponentPhaseFen = fenAfter;
        positionChess = new ChessCtor(fenAfter);
        syncBoardFromPosition();
        setReviewMoveStatus('Correct. Play opponent response...', 'ok');
      } else {
        awaitingOpponentResponse = false;
        setReviewMoveStatus('Correct.', 'ok');
        scheduleAutoGrade(cardId);
      }
    }
  });

  if (ok && !inOppPhase) {
    answerShown = true;
    answerRevealed = false;
    syncPlayedMoveMetric();
    setShowAnswerButtonState();
    syncBoardArrows();
    setAnswerEvalMetric(line?.cp ?? null);
  } else {
    wrongAttemptsThisCard += 1;
  }
  if (!moveAttemptRecorded) {
    recordMoveAttempt(ok);
    moveAttemptRecorded = true;
  }
}

async function loadCard() {
  const username = $('userSelectReview')?.value || selectedUser();
  if (!username) return setReviewMoveStatus('Pick a user.', 'error');
  // Invalidate any in-flight reply-arrow requests from the previous card.
  replyRequestSeq += 1;
  opponentReplyShapes = [];
  const severityQ = severityFilterQuery();
  setSelectedUser(username);
  await refreshReviewQueueMetrics(username);
  const res = await fetch(`/api/review/next/${encodeURIComponent(username)}?${severityQ}`);
  if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
  const data = await res.json();
  if (!data.card) {
    currentCard = null;
    cardStartedAt = null;
    stopCardTimer();
    tickCardTimer();
    clearBoardDecorations();
    clearBoardOverlay();
    setPlayedMoveMetric(null);
    if (attemptPlayedRowEl) attemptPlayedRowEl.hidden = false;
    const viewSourceBtn = $('viewSourceGame');
    if (viewSourceBtn) viewSourceBtn.hidden = true;
    setGradeButtonsDefault();
    setReviewControlsEnabled(false);
    const excludeLost = $('reviewExcludeLost')?.checked || localStorage.getItem(STORAGE_EXCLUDE_LOST_KEY) === '1';
    let note = 'No due cards.';
    let doneSub = 'You have finished this deck for now.';
    let doneExtra = '';
    if (excludeLost) {
      try {
        const q = new URLSearchParams(severityQ);
        q.set('exclude_lost', '0');
        const sRes = await fetch(`/api/stats/${encodeURIComponent(username)}?${q.toString()}`);
        if (sRes.ok) {
          const s = await sRes.json();
          const hiddenDue = Number(s?.due_cards || 0);
          if (hiddenDue > 0) {
            note = 'No due cards with current filters.';
            doneExtra = `${hiddenDue} lost positions hidden.`;
          }
        }
      } catch {}
    }
    if (infoEl) infoEl.textContent = note;
    setBoardDoneOverlay(true, doneSub, doneExtra);
    setReviewMoveStatus(note, 'idle');
    return;
  }
  setBoardDoneOverlay(false);
  await ensureBoardDeps();
  clearWrongResetTimer();
  currentCard = data.card;
  {
    const viewSourceBtn = $('viewSourceGame');
    if (viewSourceBtn) {
      const url = buildSourceGameUrl(currentCard);
      viewSourceBtn.hidden = !url;
      const txt = viewSourceBtn.querySelector('span');
      if (txt) {
        const ply = Number(currentCard?.ply || 0);
        const targetPly = ply > 0 ? Math.max(1, ply - 2) : 0;
        txt.textContent = targetPly > 0 ? `View Game (move ${targetPly})` : 'View Game';
      }
    }
  }
  resetAttemptBox();
  setupBoard(currentCard);
  renderAttempts();
  setReviewControlsEnabled(true);
  setReviewMoveStatus('Your move.', 'idle');
  await refreshGradePreviewLabels();
  if (infoEl) infoEl.textContent = '';
}

async function gradeCard(rating) {
  if (!currentCard) return;
  const username = $('userSelectReview')?.value || selectedUser();
  clearWrongResetTimer();
  stopCardTimer();
  showGradeOverlay(rating);
  await sleep(220);
  clearBoardDecorations();
  const out = await postJson('/api/review/grade', { card_id: currentCard.card_id, rating });
  setReviewMoveStatus('Saved.', 'ok');
  await fetchUsers();
  if (username) {
    await refreshReviewQueueMetrics(username);
    await loadSessionMetricsForUser(username);
  }
  await loadCard();
}

function showAnswer() {
  if (!currentCard || !positionChess || !cg) return;
  if (answerShown) {
    resetToCardPosition();
    setReviewMoveStatus('Reset.', 'idle');
    return;
  }
  resetToCardPosition();

  const acceptable = (currentCard.all_lines || []).filter((l) => acceptableLineSet().has(l.first_move_uci));
  const best = (acceptable && acceptable[0]) || (currentCard.all_lines && currentCard.all_lines[0]);
  if (!best?.first_move_uci) {
    setReviewMoveStatus('No answer.', 'error');
    return;
  }

  const uci = best.first_move_uci;
  const answerLine = findLineByUci(uci);
  const from = uci.slice(0, 2);
  const to = uci.slice(2, 4);
  const promotion = uci.length > 4 ? uci[4] : undefined;
  const mv = positionChess.move({ from, to, promotion });
  if (!mv) {
    setReviewMoveStatus('No answer.', 'error');
    return;
  }

  cg.set({
    fen: positionChess.fen().split(' ').slice(0, 4).join(' '),
    lastMove: [from, to],
    drawable: { autoShapes: [] },
  });
  playedMoveUci = uci;
  setReviewMoveStatus(`Answer: ${mv.san || uci}`, 'ok');
  setAnswerEvalMetric(answerLine?.cp ?? best?.cp ?? null);
  resetOpponentReplyArrows();
  answerShown = true;
  answerRevealed = true;
  syncPlayedMoveMetric();
  setShowAnswerButtonState();
  syncBoardArrows();
  const cardId = currentCard.card_id;
  const fenAfter = positionChess.fen();
  void fetchOpponentReplyArrows(fenAfter, cardId);
  scheduleNextCardCountdown(cardId, 5, 1);
}

function wireCommon() {
  window.addEventListener('error', (e) => log(`JS error: ${e.message}`));
  window.addEventListener('unhandledrejection', (e) => log(`Promise error: ${e.reason?.message || String(e.reason)}`));

  const selAnalyze = $('userSelectAnalyze');
  if (selAnalyze) {
    selAnalyze.addEventListener('change', () => setSelectedUser(selAnalyze.value));
  }
  const selReview = $('userSelectReview');
  if (selReview) {
    selReview.addEventListener('change', async () => {
      setSelectedUser(selReview.value);
      await loadSessionMetricsForUser(selReview.value);
      applyUserStatsToReviewMetrics(selReview.value);
      void refreshReviewQueueMetrics(selReview.value).catch((e) => log(`Queue refresh failed: ${e.message}`));
      void loadCard().catch((e) => log(`Auto-load user change failed: ${e.message}`));
    });
  }

  applySeverityFilterToDom(getStoredSeverityFilter());
  const severityIds = [
    'analyzeShowInaccuracy',
    'analyzeShowMistake',
    'analyzeShowBlunder',
    'reviewShowInaccuracy',
    'reviewShowMistake',
    'reviewShowBlunder',
  ];
  for (const id of severityIds) {
    const el = $(id);
    if (!el) continue;
    el.addEventListener('change', async () => {
      const filter = severityFilterFromDom();
      applySeverityFilterToDom(filter);
      saveSeverityFilter(filter);
      try {
        await fetchUsers();
        const ru = $('userSelectReview')?.value || selectedUser();
        if (ru) {
          await refreshReviewQueueMetrics(ru);
          if ($('userSelectReview')) await loadCard();
        }
      } catch (e) {
        log(`Class filter update failed: ${e.message}`);
      }
    });
  }
  const excludeLostIds = ['reviewExcludeLost', 'analyzeExcludeLost'];
  const excludeLostStored = localStorage.getItem(STORAGE_EXCLUDE_LOST_KEY) === '1';
  for (const id of excludeLostIds) {
    const el = $(id);
    if (el) el.checked = excludeLostStored;
  }
  for (const id of excludeLostIds) {
    const el = $(id);
    if (!el) continue;
    el.addEventListener('change', async () => {
      const enabled = Boolean(el.checked);
      localStorage.setItem(STORAGE_EXCLUDE_LOST_KEY, enabled ? '1' : '0');
      for (const peerId of excludeLostIds) {
        const peer = $(peerId);
        if (peer) peer.checked = enabled;
      }
      try {
        await fetchUsers();
        const ru = $('userSelectReview')?.value || selectedUser();
        if (ru) {
          await refreshReviewQueueMetrics(ru);
          if ($('userSelectReview')) await loadCard();
        }
      } catch (e) {
        log(`Lost-position filter update failed: ${e.message}`);
      }
    });
  }

  const sliderMap = [
    ['depth', 'depthValue'],
    ['multipv', 'multipvValue'],
    ['openingSkip', 'openingSkipValue'],
    ['cpWindow', 'cpWindowValue'],
    ['reviewAcceptWindow', 'reviewAcceptWindowValue'],
    ['statsDays', 'statsDaysValue'],
  ];
  for (const [inputId, outId] of sliderMap) {
    const inp = $(inputId);
    const out = $(outId);
    if (!inp || !out) continue;
    const sync = () => {
      out.textContent = inp.value;
    };
    inp.addEventListener('input', sync);
    sync();
  }

  const reviewAcceptWindow = $('reviewAcceptWindow');
  if (reviewAcceptWindow) {
    reviewAcceptWindow.addEventListener('change', () => {
      if (!currentCard) return;
      syncBoardArrows();
      setReviewMoveStatus('Window updated.', 'idle');
    });
  }
  const reviewSettingsBtn = $('reviewSettingsBtn');
  if (reviewSettingsBtn) {
    reviewSettingsBtn.addEventListener('click', () => openReviewSettings());
  }
  const reviewSettingsClose = $('reviewSettingsClose');
  if (reviewSettingsClose) {
    reviewSettingsClose.addEventListener('click', () => closeReviewSettings());
  }
  const reviewSettingsBackdrop = $('reviewSettingsBackdrop');
  if (reviewSettingsBackdrop) {
    reviewSettingsBackdrop.addEventListener('click', () => closeReviewSettings());
  }
  const analyzeSettingsBtn = $('analyzeSettingsBtn');
  if (analyzeSettingsBtn) {
    analyzeSettingsBtn.addEventListener('click', () => openAnalyzeSettings());
  }
  const analyzeSettingsClose = $('analyzeSettingsClose');
  if (analyzeSettingsClose) {
    analyzeSettingsClose.addEventListener('click', () => closeAnalyzeSettings());
  }
  const analyzeSettingsBackdrop = $('analyzeSettingsBackdrop');
  if (analyzeSettingsBackdrop) {
    analyzeSettingsBackdrop.addEventListener('click', () => closeAnalyzeSettings());
  }
  const analyzeClearBackdrop = $('analyzeClearBackdrop');
  if (analyzeClearBackdrop) {
    analyzeClearBackdrop.addEventListener('click', () => closeAnalyzeClearConfirm());
  }
  const analyzeClearCancel = $('analyzeClearCancel');
  if (analyzeClearCancel) {
    analyzeClearCancel.addEventListener('click', () => closeAnalyzeClearConfirm());
  }
  const importClearBackdrop = $('importClearBackdrop');
  if (importClearBackdrop) {
    importClearBackdrop.addEventListener('click', () => closeImportClearConfirm());
  }
  const importClearCancel = $('importClearCancel');
  if (importClearCancel) {
    importClearCancel.addEventListener('click', () => closeImportClearConfirm());
  }

  window.addEventListener('keydown', (e) => {
    if (e.key === 'Escape') {
      closeReviewSettings();
      closeAnalyzeSettings();
      closeAnalyzeClearConfirm();
      closeImportClearConfirm();
    }
  });

  const autoGradeMode = $('autoGradeMode');
  if (autoGradeMode) {
    const stored = localStorage.getItem(STORAGE_AUTO_GRADE_KEY);
    autoGradeMode.checked = stored === null ? true : stored === '1';
    autoGradeMode.addEventListener('change', () => {
      localStorage.setItem(STORAGE_AUTO_GRADE_KEY, autoGradeMode.checked ? '1' : '0');
      if (!autoGradeMode.checked) clearAutoProceedTimer();
    });
  }
  const opponentResponseMode = $('opponentResponseMode');
  if (opponentResponseMode) {
    opponentResponseMode.checked = localStorage.getItem(STORAGE_OPP_RESPONSE_KEY) === '1';
    opponentResponseMode.addEventListener('change', () => {
      localStorage.setItem(STORAGE_OPP_RESPONSE_KEY, opponentResponseMode.checked ? '1' : '0');
      if (opponentResponseMode.checked) {
        opponentReplyShapes = [];
        syncBoardArrows();
      }
    });
  }
  const showPositionEval = $('showPositionEval');
  if (showPositionEval) {
    showPositionEval.checked = localStorage.getItem(STORAGE_SHOW_POSITION_EVAL_KEY) === '1';
    showPositionEval.addEventListener('change', () => {
      localStorage.setItem(STORAGE_SHOW_POSITION_EVAL_KEY, showPositionEval.checked ? '1' : '0');
      syncPositionEvalMetric();
    });
  }
  const hidePlayedMove = $('hidePlayedMove');
  if (hidePlayedMove) {
    hidePlayedMove.checked = localStorage.getItem(STORAGE_HIDE_PLAYED_MOVE_KEY) === '1';
    hidePlayedMove.addEventListener('change', () => {
      localStorage.setItem(STORAGE_HIDE_PLAYED_MOVE_KEY, hidePlayedMove.checked ? '1' : '0');
      syncPlayedMoveMetric();
    });
  }
  const reviewBoardTheme = $('reviewBoardTheme');
  if (reviewBoardTheme) {
    reviewBoardTheme.value = storedBoardTheme();
    reviewBoardTheme.addEventListener('change', () => {
      const theme = currentBoardTheme();
      localStorage.setItem(STORAGE_BOARD_THEME_KEY, theme);
      applyBoardTheme();
    });
  }
  const reviewPieceSet = $('reviewPieceSet');
  if (reviewPieceSet) {
    reviewPieceSet.value = storedPieceSet();
    reviewPieceSet.addEventListener('change', () => {
      const setName = currentPieceSet();
      localStorage.setItem(STORAGE_PIECE_SET_KEY, setName);
      void applyPieceSet();
    });
  }

  setImportStatus('Idle');
  setAnalyzeStatus('Idle');
  setAnalyzeErrors('', 'idle');
  setReviewMoveStatus('Ready.');
  setEngineStatus('Stockfish: idle.');
  setShowAnswerButtonState();
  setReviewControlsEnabled(false);
  syncPositionEvalMetric();
  updateSessionMetricsUI();
}

function wireImportPage() {
  const renderImportProgress = (p) => {
    const msg = String(p?.message || '').trim();
    if (msg) {
      setImportStatus(msg, 'busy');
      return;
    }
    const done = Number(p?.done || 0);
    const total = Number(p?.total || 0);
    if (total > 0) setImportStatus(`Importing... ${done}/${total} games`, 'busy');
    else setImportStatus(`Importing... ${done} games`, 'busy');
  };

  const btnL = $('importLichess');
  if (btnL) {
    btnL.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget;
      const username = $('lichessUser')?.value.trim();
      if (!username) return setImportStatus('Enter username.', 'error');
      setBtnBusy(btn, true, 'Importing...');
      setImportStatus('Importing... 0 games', 'busy');
      try {
        const start = await postJson('/api/import/start', {
          source: 'lichess',
          username,
          max_games: Number($('lichessMax')?.value || 100),
        });
        const jobId = start.job_id;
        let finalOut = null;
        while (true) {
          const res = await fetch(`/api/import/progress/${encodeURIComponent(jobId)}`);
          if (!res.ok) throw new Error(`progress ${res.status}`);
          const p = await res.json();
          renderImportProgress(p);
          if (p.state === 'done') {
            finalOut = { imported: Number(p.imported || 0), skipped: Number(p.skipped || 0) };
            break;
          }
          if (p.state === 'error') throw new Error(p.error || 'import failed');
          await sleep(600);
        }
        const out = finalOut || { imported: 0, skipped: 0 };
        setSelectedUser(username);
        setImportStatus(`Imported ${out.imported}, skipped ${out.skipped}.`, 'ok');
        await fetchUsers();
      } catch (e) {
        setImportStatus('Import failed.', 'error');
      } finally {
        setBtnBusy(btn, false, 'Importing...');
      }
    });
  }

  const btnC = $('importChesscom');
  if (btnC) {
    btnC.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget;
      const username = $('chesscomUser')?.value.trim();
      if (!username) return setImportStatus('Enter username.', 'error');
      setBtnBusy(btn, true, 'Importing...');
      setImportStatus('Importing... 0 games', 'busy');
      try {
        const start = await postJson('/api/import/start', {
          source: 'chesscom',
          username,
          max_games: Number($('chesscomMax')?.value || 100),
        });
        const jobId = start.job_id;
        let finalOut = null;
        while (true) {
          const res = await fetch(`/api/import/progress/${encodeURIComponent(jobId)}`);
          if (!res.ok) throw new Error(`progress ${res.status}`);
          const p = await res.json();
          renderImportProgress(p);
          if (p.state === 'done') {
            finalOut = { imported: Number(p.imported || 0), skipped: Number(p.skipped || 0) };
            break;
          }
          if (p.state === 'error') throw new Error(p.error || 'import failed');
          await sleep(600);
        }
        const out = finalOut || { imported: 0, skipped: 0 };
        setSelectedUser(username);
        setImportStatus(`Imported ${out.imported}, skipped ${out.skipped}.`, 'ok');
        await fetchUsers();
      } catch (e) {
        setImportStatus('Import failed.', 'error');
      } finally {
        setBtnBusy(btn, false, 'Importing...');
      }
    });
  }

  const btnPgn = $('importPgn');
  if (btnPgn) {
    btnPgn.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget;
      const fileInput = $('pgnFile');
      const file = fileInput?.files?.[0];
      if (!file) return setImportStatus('Pick PGN file.', 'error');
      setBtnBusy(btn, true, 'Importing...');
      setImportStatus('Importing PGN...', 'busy');
      try {
        const form = new FormData();
        form.append('file', file);
        const res = await fetch('/api/import/pgn', { method: 'POST', body: form });
        if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
        const out = await res.json();
        if (out?.username) setSelectedUser(out.username);
        setImportStatus(`Imported ${Number(out?.imported || 0)}, skipped ${Number(out?.skipped || 0)}.`, 'ok');
        await fetchUsers();
      } catch (e) {
        setImportStatus('PGN import failed.', 'error');
        log(`PGN import failed: ${e.message}`);
      } finally {
        setBtnBusy(btn, false, 'Importing...');
      }
    });
  }

  const exportDbBtn = $('exportDbBtn');
  if (exportDbBtn) {
    exportDbBtn.addEventListener('click', async () => {
      try {
        const res = await fetch('/api/db/export');
        if (!res.ok) throw new Error(`export ${res.status}`);
        const blob = await res.blob();
        const href = URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.href = href;
        a.download = `blunderfix-local-${Date.now()}.json`;
        document.body.appendChild(a);
        a.click();
        a.remove();
        URL.revokeObjectURL(href);
      } catch (e) {
        setImportStatus('DB export failed.', 'error');
        log(`DB export failed: ${e.message}`);
      }
    });
  }
  const importDbBtn = $('importDbBtn');
  if (importDbBtn) {
    importDbBtn.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget;
      const fileInput = $('importDbFile');
      const file = fileInput?.files?.[0];
      if (!file) return setImportStatus('Pick DB file.', 'error');
      if (!window.confirm('Importing DB will replace current local data. Continue?')) return;
      setBtnBusy(btn, true, 'Importing DB...');
      setImportStatus('Importing DB...', 'busy');
      try {
        const form = new FormData();
        form.append('file', file);
        const res = await fetch('/api/db/import', { method: 'POST', body: form });
        if (!res.ok) throw new Error(`${res.status}: ${await res.text()}`);
        setImportStatus('DB imported.', 'ok');
        await fetchUsers();
      } catch (e) {
        setImportStatus('DB import failed.', 'error');
        log(`DB import failed: ${e.message}`);
      } finally {
        setBtnBusy(btn, false, 'Importing DB...');
      }
    });
  }

  const clearAllImportsBtn = $('clearAllImportsBtn');
  if (clearAllImportsBtn) {
    clearAllImportsBtn.addEventListener('click', () => openImportClearConfirm());
  }
  if (userCardsEl) {
    userCardsEl.addEventListener('click', async (ev) => {
      const targetEl = ev.target instanceof Element ? ev.target : ev.target?.parentElement;
      const btn = targetEl?.closest?.('button[data-delete-user]');
      if (!btn) return;
      const username = String(btn.dataset.deleteUser || '').trim();
      if (!username) return;
      if (!window.confirm(`Delete profile "${username}" and all its games, analysis, cards, and reviews?`)) return;
      setBtnBusy(btn, true, '×');
      setImportStatus(`Deleting ${username}...`, 'busy');
      try {
        const out = await postJson('/api/import/clear-user', { username });
        setImportStatus(`Deleted ${username}: ${Number(out.games_deleted || 0)} games.`, 'ok');
        if (selectedUser() === username) localStorage.removeItem(STORAGE_USER_KEY);
        await fetchUsers();
      } catch (e) {
        setImportStatus('Delete profile failed.', 'error');
        log(`Delete profile failed: ${e.message}`);
      } finally {
        setBtnBusy(btn, false, '×');
      }
    });
  }
  const importClearConfirm = $('importClearConfirm');
  if (importClearConfirm) {
    importClearConfirm.addEventListener('click', async () => {
      closeImportClearConfirm();
      setImportStatus('Clearing all imports...', 'busy');
      setBtnBusy(clearAllImportsBtn, true, 'Clearing...');
      try {
        const out = await postJson('/api/import/clear-all', {});
        setImportStatus(`Cleared ${out.games_deleted} games.`, 'ok');
        await fetchUsers();
      } catch (e) {
        setImportStatus('Clear imports failed.', 'error');
        log(`Clear imports failed: ${e.message}`);
      } finally {
        setBtnBusy(clearAllImportsBtn, false, 'Clearing...');
      }
    });
  }
}

function moveObjToUci(m) {
  return `${m.from}${m.to}${m.promotion || ''}`;
}

function uciToMoveObj(uci) {
  const out = { from: uci.slice(0, 2), to: uci.slice(2, 4) };
  if (uci.length > 4) out.promotion = uci[4];
  return out;
}

function uciLineToSan(fen, pv) {
  if (!ChessCtor) return '';
  const b = new ChessCtor(fen);
  const sans = [];
  for (const u of (pv || []).slice(0, 10)) {
    if (!u || u.length < 4) break;
    const out = moveSafe(b, uciToMoveObj(u));
    if (!out) break;
    sans.push(out.san);
  }
  return sans.join(' ');
}

async function analyzeSinglePositionWasm(fen, playedUci, sideToMove, cfg) {
  const infos = await stockfishAnalyze(fen, { depth: cfg.depth, multipv: cfg.multipv });
  const candidates = [];
  let bestCp = null;
  let playedCp = null;
  for (const info of infos) {
    const cpFromTurn = scoreToCp(info.score);
    if (cpFromTurn === null || cpFromTurn === undefined) continue;
    const cp = scoreForPov(cpFromTurn, fenTurn(fen), sideToMove);
    if (bestCp === null) bestCp = cp;
    const first = info.pv?.[0];
    if (!first) continue;
    if (first === playedUci) playedCp = cp;
    candidates.push({
      pv_rank: Number(info.multipv || 1),
      cp: Number(cp),
      first_move_uci: first,
      uci_line: (info.pv || []).slice(0, 10).join(' '),
      san_line: uciLineToSan(fen, info.pv || []),
      is_acceptable: false,
    });
  }
  candidates.sort((a, b) => a.pv_rank - b.pv_rank);
  if (bestCp === null) bestCp = 0;
  for (const c of candidates) {
    c.is_acceptable = (Number(bestCp) - Number(c.cp)) <= cfg.cpWindow;
  }

  if (playedCp === null || playedCp === undefined) {
    const b2 = new ChessCtor(fen);
    const applied = moveSafe(b2, uciToMoveObj(playedUci));
    if (!applied) playedCp = Number(bestCp);
    else {
      const replyInfos = await stockfishAnalyze(b2.fen(), { depth: Math.max(6, cfg.depth - 2), multipv: 1 });
      const r0 = replyInfos[0];
      const cpFromTurn = r0 ? scoreToCp(r0.score) : null;
      playedCp = cpFromTurn === null || cpFromTurn === undefined ? Number(bestCp) : -Number(cpFromTurn);
    }
  }
  const lossCp = Number(bestCp) - Number(playedCp);
  const cls = classifyByWinningChanceDelta(bestCp, playedCp);
  return {
    bestCp: Number(bestCp),
    playedCp: Number(playedCp),
    lossCp: Number(lossCp),
    judgement: cls.judgement,
    winningChanceDelta: Number(cls.delta || 0),
    candidates,
  };
}

async function analyzeGameInBrowser(game, cfg) {
  if (!ChessCtor) await ensureBoardDeps();
  if (!isSupportedPgnVariant(game.pgn)) {
    return { positions: [], blunders: 0, errors: 0, skipped: 'unsupported variant' };
  }
  const pgnChess = new ChessCtor();
  try {
    pgnChess.loadPgn(game.pgn, { strict: false });
  } catch {
    return { positions: [], blunders: 0 };
  }
  const moves = pgnChess.history({ verbose: true });
  const board = new ChessCtor();
  const userSide = game.played_color === 'white' ? 'white' : 'black';
  let userMoveIndex = 0;
  let blunders = 0;
  const positions = [];
  let errors = 0;

  for (let i = 0; i < moves.length; i += 1) {
    const mv = moves[i];
    const sideToMove = board.turn() === 'w' ? 'white' : 'black';
    const moveObj = { from: mv.from, to: mv.to, promotion: mv.promotion };
    if (sideToMove !== userSide) {
      if (!moveSafe(board, moveObj)) {
        errors += 1;
        break;
      }
      continue;
    }
    userMoveIndex += 1;
    if (userMoveIndex <= cfg.openingSkip) {
      if (!moveSafe(board, moveObj)) {
        errors += 1;
        break;
      }
      continue;
    }

    const fen = board.fen();
    const playedUci = moveObjToUci(mv);
    let analysis = null;
    try {
      analysis = await analyzeSinglePositionWasm(fen, playedUci, sideToMove, cfg);
    } catch {
      errors += 1;
      board.move(moveObj);
      continue;
    }
    if (!analysis.judgement) {
      board.move(moveObj);
      continue;
    }
    const isBlunder = analysis.judgement === 'blunder';
    if (isBlunder) blunders += 1;

    const practical = (() => {
      const bAfter = new ChessCtor(fen);
      const userApplied = moveSafe(bAfter, moveObj);
      if (!userApplied) return null;
      const opp = moves[i + 1];
      if (!opp) return null;
      const bResp = new ChessCtor(bAfter.fen());
      const oppMoveObj = { from: opp.from, to: opp.to, promotion: opp.promotion };
      const oppApplied = moveSafe(bResp, oppMoveObj);
      if (!oppApplied) return null;
      return {
        opponent_move_uci: moveObjToUci(opp),
        opponent_move_san: opp.san,
        fen_after: bResp.fen(),
      };
    })();

    let cpAfter = null;
    if (practical) {
      try {
        const infos = await stockfishAnalyze(practical.fen_after, { depth: Math.max(6, cfg.depth - 2), multipv: 1 });
        const cpFromTurn = infos[0] ? scoreToCp(infos[0].score) : null;
        cpAfter = scoreForPov(cpFromTurn, fenTurn(practical.fen_after), userSide);
      } catch {
        errors += 1;
        cpAfter = null;
      }
    }

    positions.push({
      ply: i + 1,
      fen,
      side_to_move: sideToMove,
      played_uci: playedUci,
      played_san: mv.san || playedUci,
      best_cp: analysis.bestCp,
      played_cp: analysis.playedCp,
      loss_cp: analysis.lossCp,
      judgement: analysis.judgement,
      winning_chance_delta: analysis.winningChanceDelta,
      candidate_lines: analysis.candidates,
      practical_response: practical
        ? {
            opponent_move_uci: practical.opponent_move_uci,
            opponent_move_san: practical.opponent_move_san,
            cp_after: cpAfter,
          }
        : null,
    });

    if (!moveSafe(board, moveObj)) {
      errors += 1;
      break;
    }
  }

  return { positions, blunders, errors };
}

function wireAnalyzePage() {
  const analyzeBtn = $('analyzeBtn');
  if (analyzeBtn) {
    analyzeBtn.addEventListener('click', async (ev) => {
      const btn = ev.currentTarget;
      const username = $('userSelectAnalyze')?.value || selectedUser();
      if (!username) return setAnalyzeStatus('Pick a user.', 'error');
      setSelectedUser(username);
      setBtnBusy(btn, true, 'Analyzing...');
      setAnalyzeStatus('Analyzing... 0 games', 'busy');
      setAnalyzeErrors('', 'idle');
      try {
        const cfg = {
          depth: Number($('depth')?.value || 10),
          multipv: Number($('multipv')?.value || 4),
          openingSkip: Number($('openingSkip')?.value || 0),
          cpWindow: Number($('cpWindow')?.value || 30),
        };

        await ensureBoardDeps();
        await ensureStockfishWorker();
        const gamesRes = await fetch(`/api/analyze/games/${encodeURIComponent(username)}?max_games=200`);
        if (!gamesRes.ok) throw new Error(`games ${gamesRes.status}`);
        const gamesData = await gamesRes.json();
        const games = Array.isArray(gamesData.games) ? gamesData.games : [];
        const total = games.length;
        if (!total) {
          setAnalyzeStatus('No unanalyzed games.', 'idle');
          return;
        }

        let done = 0;
        let totalPositions = 0;
        let totalBlunders = 0;
        let totalErrors = 0;
        let failedGames = 0;
        const failedDetails = [];
        for (const g of games) {
          setAnalyzeStatus(`Analyzing... ${done}/${total} games • errors ${totalErrors}`, 'busy');
          try {
            const out = await analyzeGameInBrowser(g, cfg);
            await postJson('/api/analyze/store-game', {
              game_id: g.id,
              positions: out.positions,
            });
            totalPositions += Number(out.positions.length || 0);
            totalBlunders += Number(out.blunders || 0);
            totalErrors += Number(out.errors || 0);
          } catch (e) {
            failedGames += 1;
            totalErrors += 1;
            failedDetails.push(`#${g.id}: ${String(e?.message || 'error')}`);
            log(`Analyze game failed id=${g.id}: ${e.message}`);
          }
          done += 1;
          setAnalyzeStatus(`Analyzing... ${done}/${total} games • errors ${totalErrors}`, 'busy');
        }
        const out = { games: done, positions: totalPositions, blunders: totalBlunders, errors: totalErrors, failed_games: failedGames };
        setAnalyzeStatus(`Done: ${out.games}g ${out.positions}p ${out.blunders}b • errors ${out.errors} • failed ${out.failed_games}`, out.errors ? 'error' : 'ok');
        if (failedDetails.length) {
          setAnalyzeErrors(`Failed games: ${failedDetails.slice(0, 8).join(' | ')}`, 'error');
        } else {
          setAnalyzeErrors('', 'idle');
        }
        await fetchUsers();
      } catch (e) {
        log(`Analyze failed: ${e.message}`);
        setAnalyzeStatus('Analyze failed.', 'error');
        setAnalyzeErrors(String(e?.message || 'Analyze failed.'), 'error');
      } finally {
        setBtnBusy(btn, false, 'Analyzing...');
      }
    });
  }

  const flushBtn = $('resetAnalyzeBtn');
  const clearConfirmBtn = $('analyzeClearConfirm');

  async function clearAnalyzeDataForSelectedUser() {
    const username = $('userSelectAnalyze')?.value || selectedUser();
    if (!username) return setAnalyzeStatus('Pick a user.', 'error');
    setBtnBusy(flushBtn, true, 'Clearing...');
    setAnalyzeStatus('Clearing...', 'busy');
    closeAnalyzeClearConfirm();
    try {
      const out = await postJson('/api/analyze/reset', { username });
      setAnalyzeStatus(`Cleared: ${out.positions_deleted} positions.`, 'ok');
      await fetchUsers();
    } catch (e) {
      setAnalyzeStatus('Clear failed.', 'error');
    } finally {
      setBtnBusy(flushBtn, false, 'Clearing...');
    }
  }

  if (flushBtn) {
    flushBtn.addEventListener('click', () => {
      const username = $('userSelectAnalyze')?.value || selectedUser();
      if (!username) return setAnalyzeStatus('Pick a user.', 'error');
      openAnalyzeClearConfirm();
    });
  }
  if (clearConfirmBtn) {
    clearConfirmBtn.addEventListener('click', () => {
      void clearAnalyzeDataForSelectedUser();
    });
  }
}

function wireReviewPage() {
  const showBtn = $('showAnswer');
  if (showBtn) showBtn.addEventListener('click', showAnswer);
  if (autoNextCancelEl) {
    autoNextCancelEl.addEventListener('click', () => {
      clearAutoProceedTimer();
      setReviewMoveStatus('Auto-next canceled.', 'idle');
    });
  }

  const lichessBtn = $('openLichessAnalysis');
  if (lichessBtn) {
    lichessBtn.addEventListener('click', () => {
      if (!currentCard?.fen) return;
      const fenPath = currentCard.fen.replaceAll(' ', '_');
      window.open(`https://lichess.org/analysis/${fenPath}`, '_blank', 'noopener,noreferrer');
    });
  }
  const viewSourceBtn = $('viewSourceGame');
  if (viewSourceBtn) {
    viewSourceBtn.addEventListener('click', () => {
      const url = buildSourceGameUrl(currentCard);
      if (!url) return;
      window.open(url, '_blank', 'noopener,noreferrer');
    });
  }
  for (const btn of document.querySelectorAll('.grade')) {
    btn.addEventListener('click', async () => {
      try {
        await gradeCard(Number(btn.dataset.grade));
      } catch (e) {
        log(`Grade error: ${e.message}`);
      }
    });
  }

  window.addEventListener('keydown', async (e) => {
    if (!$('userSelectReview')) return; // review page only
    if (e.metaKey || e.ctrlKey || e.altKey) return;
    const tgt = e.target;
    const tag = tgt && tgt.tagName ? tgt.tagName.toLowerCase() : '';
    if (tag === 'input' || tag === 'textarea' || tag === 'select' || (tgt && tgt.isContentEditable)) return;
    if (!currentCard) return;

    // Requested mapping: 1=Again, 2=Good, 3=Hard, 4=Easy
    const map = {
      Digit1: 1,
      Digit2: 3,
      Digit3: 2,
      Digit4: 4,
      Numpad1: 1,
      Numpad2: 3,
      Numpad3: 2,
      Numpad4: 4,
      '1': 1,
      '2': 3,
      '3': 2,
      '4': 4,
    };
    const rating = map[e.code] || map[e.key];
    if (!rating) return;
    e.preventDefault();
    try {
      await gradeCard(rating);
    } catch (err) {
      log(`Shortcut grade error: ${err.message}`);
    }
  });
}

function wireStatsPage() {
  const sel = $('userSelectStats');
  if (sel) {
    sel.addEventListener('change', () => {
      setSelectedUser(sel.value);
      void loadStatsPage().catch((e) => log(`Stats load failed: ${e.message}`));
    });
  }
  const days = $('statsDays');
  if (days) {
    days.addEventListener('change', () => {
      void loadStatsPage().catch((e) => log(`Stats load failed: ${e.message}`));
    });
  }
  const refresh = $('statsRefresh');
  if (refresh) {
    refresh.addEventListener('click', () => {
      void loadStatsPage().catch((e) => log(`Stats load failed: ${e.message}`));
    });
  }
}

async function init() {
  wireCommon();
  wireImportPage();
  wireAnalyzePage();
  wireReviewPage();
  wireStatsPage();

  try {
    await fetchUsers();
  } catch (e) {
    log(`Initial users load failed: ${e.message}`);
  }

  const su = selectedUser();
  if (su) {
    if ($('userSelectAnalyze')) $('userSelectAnalyze').value = su;
    if ($('userSelectReview')) {
      $('userSelectReview').value = su;
      await loadSessionMetricsForUser(su);
      applyUserStatsToReviewMetrics(su);
    }
    if ($('userSelectStats')) $('userSelectStats').value = su;
  }

  if ($('userSelectReview') && !su) {
    await loadSessionMetricsForUser($('userSelectReview').value || '');
  }

  if ($('userSelectStats')) {
    try {
      await loadStatsPage();
    } catch (e) {
      log(`Stats load failed: ${e.message}`);
    }
  }

  if ($('userSelectReview')) {
    try {
      await loadCard();
    } catch (e) {
      setReviewMoveStatus('Load failed.', 'error');
      log(`Auto-load failed: ${e.message}`);
    }
  }

  log('Ready.');
}

init();
