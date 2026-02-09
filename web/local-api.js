const DB_NAME = 'blunderfix-local';
const DB_VERSION = 1;
const STATE_KEY = 'state';

const DEFAULT_STATE = {
  version: 1,
  nextIds: { game: 1, position: 1, line: 1, practical: 1, card: 1, review: 1 },
  games: [],
  positions: [],
  candidate_lines: [],
  practical_responses: [],
  cards: [],
  reviews: [],
};

const importJobs = new Map();

let dbPromise = null;
let stateCache = null;

function nowIsoUtc() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

function sqlTsToDate(ts) {
  return new Date(String(ts).replace(' ', 'T') + 'Z');
}

function dateToSqlTs(d) {
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getUTCFullYear()}-${pad(d.getUTCMonth() + 1)}-${pad(d.getUTCDate())} ${pad(d.getUTCHours())}:${pad(d.getUTCMinutes())}:${pad(d.getUTCSeconds())}`;
}

function parseIntQ(url, key, fallback) {
  const u = new URL(url, window.location.origin);
  const v = Number(u.searchParams.get(key));
  return Number.isFinite(v) ? v : fallback;
}

function parseBoolQ(url, key, fallback) {
  const u = new URL(url, window.location.origin);
  if (!u.searchParams.has(key)) return fallback;
  const v = String(u.searchParams.get(key) || '').toLowerCase();
  if (v === '1' || v === 'true' || v === 'yes' || v === 'on') return true;
  if (v === '0' || v === 'false' || v === 'no' || v === 'off') return false;
  return fallback;
}

function parseSeverityFilter(url) {
  return {
    inaccuracy: parseBoolQ(url, 'show_inaccuracy', false),
    mistake: parseBoolQ(url, 'show_mistake', false),
    blunder: parseBoolQ(url, 'show_blunder', true),
    exclude_lost: parseBoolQ(url, 'exclude_lost', false),
  };
}

function inferJudgementLegacy(p) {
  const existing = String(p?.judgement || '').toLowerCase();
  if (existing === 'blunder' || existing === 'mistake' || existing === 'inaccuracy') return existing;
  if (Number(p?.is_blunder || 0) > 0) return 'blunder';
  const d = Number(p?.winning_chance_delta);
  if (Number.isFinite(d)) {
    if (d >= 0.3) return 'blunder';
    if (d >= 0.2) return 'mistake';
    if (d >= 0.1) return 'inaccuracy';
  }
  const cp = Number(p?.loss_cp);
  if (Number.isFinite(cp)) {
    if (cp > 200) return 'blunder';
    if (cp >= 100) return 'mistake';
    if (cp > 0) return 'inaccuracy';
  }
  return '';
}

function lower(s) {
  return String(s || '').trim().toLowerCase();
}

function openDb() {
  if (dbPromise) return dbPromise;
  dbPromise = new Promise((resolve, reject) => {
    const req = indexedDB.open(DB_NAME, DB_VERSION);
    req.onupgradeneeded = () => {
      const db = req.result;
      if (!db.objectStoreNames.contains('kv')) db.createObjectStore('kv');
    };
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error('indexeddb open failed'));
  });
  return dbPromise;
}

async function idbGet(key) {
  const db = await openDb();
  return await new Promise((resolve, reject) => {
    const tx = db.transaction('kv', 'readonly');
    const st = tx.objectStore('kv');
    const req = st.get(key);
    req.onsuccess = () => resolve(req.result);
    req.onerror = () => reject(req.error || new Error('indexeddb get failed'));
  });
}

async function idbSet(key, value) {
  const db = await openDb();
  return await new Promise((resolve, reject) => {
    const tx = db.transaction('kv', 'readwrite');
    const st = tx.objectStore('kv');
    st.put(value, key);
    tx.oncomplete = () => resolve();
    tx.onerror = () => reject(tx.error || new Error('indexeddb put failed'));
  });
}

async function loadState() {
  if (stateCache) return stateCache;
  const v = await idbGet(STATE_KEY);
  stateCache = v && typeof v === 'object' ? v : structuredClone(DEFAULT_STATE);
  if (!stateCache.nextIds) stateCache.nextIds = structuredClone(DEFAULT_STATE.nextIds);
  if (Array.isArray(stateCache.positions)) {
    for (const p of stateCache.positions) {
      if (!p || typeof p !== 'object') continue;
      if (!p.judgement) {
        const inferred = inferJudgementLegacy(p);
        if (inferred) p.judgement = inferred;
      }
      if (p.winning_chance_delta === undefined || p.winning_chance_delta === null) p.winning_chance_delta = 0;
      if (Object.prototype.hasOwnProperty.call(p, 'is_blunder')) delete p.is_blunder;
    }
  }
  return stateCache;
}

async function saveState() {
  if (!stateCache) return;
  await idbSet(STATE_KEY, stateCache);
}

function nextId(kind) {
  const id = Number(stateCache.nextIds[kind] || 1);
  stateCache.nextIds[kind] = id + 1;
  return id;
}

function parsePgnHeaders(pgn) {
  const h = {};
  for (const line of String(pgn || '').split('\n')) {
    const m = line.match(/^\[([A-Za-z0-9_]+)\s+"(.*)"\]$/);
    if (!m) continue;
    h[m[1]] = m[2];
  }
  return h;
}

function userColorFromPgn(pgn, username) {
  const h = parsePgnHeaders(pgn);
  const u = lower(username);
  if (lower(h.White) === u) return 'white';
  if (lower(h.Black) === u) return 'black';
  return 'white';
}

function resultFromPgn(pgn, playedColor) {
  const h = parsePgnHeaders(pgn);
  const r = h.Result || '*';
  if (r === '1-0') return playedColor === 'white' ? 'win' : 'loss';
  if (r === '0-1') return playedColor === 'black' ? 'win' : 'loss';
  if (r === '1/2-1/2') return 'draw';
  return 'unknown';
}

function profileFromFilename(name) {
  const raw = String(name || '').replace(/\.[^.]+$/, '').trim();
  const cleaned = raw.replace(/[^A-Za-z0-9._-]+/g, '-').replace(/-+/g, '-').replace(/^-|-$/g, '');
  return lower(cleaned || 'pgn-import');
}

function splitPgnGames(text) {
  const src = String(text || '').replace(/\r\n/g, '\n').trim();
  if (!src) return [];
  return src
    .split(/\n{2,}(?=\[(?:Event|Site|Round|White|Black|Result)\s+")/g)
    .map((x) => x.trim())
    .filter(Boolean);
}

function hashString32(text) {
  let h = 2166136261 >>> 0;
  for (let i = 0; i < text.length; i += 1) {
    h ^= text.charCodeAt(i);
    h = Math.imul(h, 16777619) >>> 0;
  }
  return h.toString(36);
}

function primaryPlayerFromPgnGames(games) {
  const counts = new Map();
  for (const pgn of games) {
    const h = parsePgnHeaders(pgn);
    const white = lower(h.White || '');
    const black = lower(h.Black || '');
    if (white) counts.set(white, (counts.get(white) || 0) + 1);
    if (black) counts.set(black, (counts.get(black) || 0) + 1);
  }
  let best = '';
  let bestCount = -1;
  for (const [name, count] of counts) {
    if (count > bestCount) {
      best = name;
      bestCount = count;
    }
  }
  return best;
}

function playedColorFromHeadersWithPrimary(pgn, primaryPlayer) {
  if (!primaryPlayer) return 'white';
  const h = parsePgnHeaders(pgn);
  if (lower(h.White || '') === primaryPlayer) return 'white';
  if (lower(h.Black || '') === primaryPlayer) return 'black';
  return 'white';
}

function addDaysTs(baseTs, days) {
  const d = sqlTsToDate(baseTs || nowIsoUtc());
  d.setUTCSeconds(d.getUTCSeconds() + Math.max(0, Math.round(days * 86400)));
  return dateToSqlTs(d);
}

function nextReview(card, rating) {
  const now = nowIsoUtc();
  const reps = Number(card.reps || 0);
  const lapses = Number(card.lapses || 0);
  const difficulty = Number(card.difficulty || 5.0);
  const stability = Number(card.stability || 0.4);

  let dueAt = now;
  let outState = card.state || 'learning';
  let outStep = Number(card.step || 0);
  let outStability = stability;
  let outDifficulty = difficulty;
  let outLapses = lapses;

  if (reps === 0) {
    if (rating === 1) {
      dueAt = addDaysTs(now, 10 / 1440);
      outState = 'learning';
      outStep = 1;
      outStability = 0.2;
    } else if (rating === 2) {
      dueAt = addDaysTs(now, 1);
      outState = 'learning';
      outStep = 2;
      outStability = 1.0;
    } else if (rating === 3) {
      dueAt = addDaysTs(now, 3);
      outState = 'review';
      outStep = 0;
      outStability = 3.0;
    } else {
      dueAt = addDaysTs(now, 7);
      outState = 'review';
      outStep = 0;
      outStability = 7.0;
    }
  } else {
    const base = Math.max(0.2, stability);
    if (rating === 1) {
      dueAt = addDaysTs(now, 10 / 1440);
      outState = 'relearning';
      outStep = 0;
      outStability = Math.max(0.2, base * 0.5);
      outDifficulty = Math.min(10, difficulty + 0.5);
      outLapses += 1;
    } else if (rating === 2) {
      dueAt = addDaysTs(now, base * 1.2);
      outState = 'review';
      outStep = 0;
      outStability = base * 1.2;
      outDifficulty = Math.min(10, difficulty + 0.1);
    } else if (rating === 3) {
      dueAt = addDaysTs(now, base * 2.0);
      outState = 'review';
      outStep = 0;
      outStability = base * 2.0;
      outDifficulty = Math.max(1, difficulty - 0.05);
    } else {
      dueAt = addDaysTs(now, base * 3.2);
      outState = 'review';
      outStep = 0;
      outStability = base * 3.2;
      outDifficulty = Math.max(1, difficulty - 0.15);
    }
  }

  return {
    state: outState,
    step: outStep,
    due_at: dueAt,
    stability: outStability,
    difficulty: outDifficulty,
    reps: reps + 1,
    lapses: outLapses,
    last_review_at: now,
    reviewed_at: now,
  };
}

function toJsonResponse(body, status = 200) {
  return new Response(JSON.stringify(body), {
    status,
    headers: { 'content-type': 'application/json' },
  });
}

function getGameIdsByUser(usernameNorm) {
  return stateCache.games.filter((g) => lower(g.username) === usernameNorm).map((g) => g.id);
}

function getPositionsByGameIds(gameIds) {
  const s = new Set(gameIds);
  return stateCache.positions.filter((p) => s.has(p.game_id));
}

function positionJudgement(p) {
  return inferJudgementLegacy(p);
}

function matchesPositionFilter(p, severity) {
  const j = positionJudgement(p);
  if (!j) return false;
  if (severity.exclude_lost && Number(p?.best_cp ?? 0) <= -200) return false;
  const okClass = (j === 'blunder' && severity.blunder) || (j === 'mistake' && severity.mistake) || (j === 'inaccuracy' && severity.inaccuracy);
  if (!okClass) return false;
  return true;
}

function ensureCardsForFilter(username, severity) {
  const u = lower(username);
  const gameIds = getGameIdsByUser(u);
  const pset = getPositionsByGameIds(gameIds);
  const cardByPosition = new Set(stateCache.cards.map((c) => c.position_id));
  let created = 0;
  for (const p of pset) {
    if (cardByPosition.has(p.id)) continue;
    if (!matchesPositionFilter(p, severity)) continue;
    stateCache.cards.push({
      id: nextId('card'),
      position_id: p.id,
      state: 'learning',
      step: 0,
      due_at: nowIsoUtc(),
      stability: 0.4,
      difficulty: 5.0,
      reps: 0,
      lapses: 0,
      last_review_at: null,
    });
    created += 1;
  }
  return created;
}

function filterDueCardsForUser(username, severity) {
  const u = lower(username);
  const gameIds = new Set(getGameIdsByUser(u));
  const positions = new Map(stateCache.positions.filter((p) => gameIds.has(p.game_id)).map((p) => [p.id, p]));
  const now = sqlTsToDate(nowIsoUtc()).getTime();
  return stateCache.cards.filter((c) => {
    const p = positions.get(c.position_id);
    if (!p) return false;
    if (sqlTsToDate(c.due_at).getTime() > now) return false;
    if (!matchesPositionFilter(p, severity)) return false;
    return true;
  }).map((c) => ({ card: c, position: positions.get(c.position_id) }));
}

async function importLichess(username, maxGames, job) {
  const u = lower(username);
  const url = `https://lichess.org/api/games/user/${encodeURIComponent(username)}?max=${encodeURIComponent(String(maxGames))}&pgnInJson=true`;
  job.phase = 'fetching';
  job.message = 'Fetching games from Lichess... 0';
  const res = await fetch(url, { headers: { Accept: 'application/x-ndjson' } });
  if (!res.ok) throw new Error(`lichess ${res.status}`);
  const lines = [];
  if (res.body && typeof res.body.getReader === 'function') {
    const reader = res.body.getReader();
    const decoder = new TextDecoder();
    let buf = '';
    while (true) {
      const { done, value } = await reader.read();
      if (done) break;
      buf += decoder.decode(value, { stream: true });
      const chunks = buf.split('\n');
      buf = chunks.pop() || '';
      for (const raw of chunks) {
        const line = raw.trim();
        if (!line) continue;
        lines.push(line);
      }
      job.message = `Fetching games from Lichess... ${lines.length}`;
    }
    buf += decoder.decode();
    const tail = buf.trim();
    if (tail) lines.push(tail);
  } else {
    const txt = await res.text();
    for (const raw of txt.split('\n')) {
      const line = raw.trim();
      if (!line) continue;
      lines.push(line);
      job.message = `Fetching games from Lichess... ${lines.length}`;
    }
  }
  job.total = lines.length;
  job.phase = 'importing';
  job.message = `Importing... 0/${job.total} games`;
  for (let i = 0; i < lines.length; i += 1) {
    const g = JSON.parse(lines[i]);
    const sourceGameId = String(g.id || g.gameId || `lichess-${i}`);
    const pgn = String(g.pgn || '');
    const playedColor = userColorFromPgn(pgn, u);
    const exists = stateCache.games.some((x) => x.source === 'lichess' && lower(x.username) === u && x.source_game_id === sourceGameId);
    if (!exists) {
      stateCache.games.push({
        id: nextId('game'),
        source: 'lichess',
        source_game_id: sourceGameId,
        username: u,
        played_color: playedColor,
        result: resultFromPgn(pgn, playedColor),
        pgn,
        analyzed: 0,
        created_at: nowIsoUtc(),
      });
      job.imported += 1;
    } else {
      job.skipped += 1;
    }
    job.done = i + 1;
    job.message = `Importing... ${job.done}/${job.total} games`;
  }
}

async function importChessCom(username, maxGames, job) {
  const u = lower(username);
  job.phase = 'fetching_archives';
  job.message = 'Loading Chess.com archive list...';
  const archivesRes = await fetch(`https://api.chess.com/pub/player/${encodeURIComponent(username)}/games/archives`);
  if (!archivesRes.ok) throw new Error(`chesscom archives ${archivesRes.status}`);
  const archivesJson = await archivesRes.json();
  const archives = Array.isArray(archivesJson.archives) ? archivesJson.archives.slice().reverse() : [];
  job.archives_total = archives.length;
  job.archives_done = 0;

  const games = [];
  for (const a of archives) {
    if (games.length >= maxGames) break;
    job.phase = 'fetching_games';
    job.message = `Fetching games... ${games.length}/${maxGames} collected`;
    const res = await fetch(a);
    job.archives_done += 1;
    if (!res.ok) continue;
    const j = await res.json();
    const arr = Array.isArray(j.games) ? j.games : [];
    for (const g of arr.reverse()) {
      if (games.length >= maxGames) break;
      games.push(g);
    }
    job.message = `Fetching games... ${games.length}/${maxGames} collected`;
  }

  job.total = games.length;
  job.phase = 'importing';
  job.message = `Importing... 0/${job.total} games`;
  for (let i = 0; i < games.length; i += 1) {
    const g = games[i];
    const sourceGameId = String(g.url || g.uuid || `chesscom-${i}`);
    const pgn = String(g.pgn || '');
    const playedColor = userColorFromPgn(pgn, u);
    const exists = stateCache.games.some((x) => x.source === 'chesscom' && lower(x.username) === u && x.source_game_id === sourceGameId);
    if (!exists) {
      stateCache.games.push({
        id: nextId('game'),
        source: 'chesscom',
        source_game_id: sourceGameId,
        username: u,
        played_color: playedColor,
        result: resultFromPgn(pgn, playedColor),
        pgn,
        analyzed: 0,
        created_at: nowIsoUtc(),
      });
      job.imported += 1;
    } else {
      job.skipped += 1;
    }
    job.done = i + 1;
    job.message = `Importing... ${job.done}/${job.total} games`;
  }
}

async function handleImportStart(body) {
  const jobId = crypto.randomUUID().replaceAll('-', '');
  const job = {
    state: 'running',
    source: body.source,
    username: lower(body.username),
    phase: 'queued',
    message: 'Queued...',
    done: 0,
    total: 0,
    imported: 0,
    skipped: 0,
    error: null,
  };
  importJobs.set(jobId, job);

  queueMicrotask(async () => {
    try {
      await loadState();
      if (job.source === 'lichess') await importLichess(body.username, Number(body.max_games || 100), job);
      else await importChessCom(body.username, Number(body.max_games || 200), job);
      job.state = 'done';
      await saveState();
    } catch (e) {
      job.state = 'error';
      job.error = e?.message || String(e);
    }
  });

  return toJsonResponse({ job_id: jobId });
}

async function handleImportProgress(jobId) {
  const job = importJobs.get(jobId);
  if (!job) return toJsonResponse({ detail: 'Job not found' }, 404);
  return toJsonResponse(job);
}

async function handleImportClearAll() {
  const gamesDeleted = stateCache.games.length;
  const positionsDeleted = stateCache.positions.length;
  const cardsDeleted = stateCache.cards.length;
  const reviewsDeleted = stateCache.reviews.length;
  stateCache.games = [];
  stateCache.positions = [];
  stateCache.candidate_lines = [];
  stateCache.practical_responses = [];
  stateCache.cards = [];
  stateCache.reviews = [];
  stateCache.nextIds = structuredClone(DEFAULT_STATE.nextIds);
  importJobs.clear();
  await saveState();
  return toJsonResponse({
    games_deleted: gamesDeleted,
    positions_deleted: positionsDeleted,
    cards_deleted: cardsDeleted,
    reviews_deleted: reviewsDeleted,
  });
}

async function handleImportClearUser(body) {
  const username = lower(body?.username || '');
  if (!username) return toJsonResponse({ detail: 'Missing username' }, 400);

  const gameIds = new Set(stateCache.games.filter((g) => lower(g.username) === username).map((g) => g.id));
  const positionIds = new Set(stateCache.positions.filter((p) => gameIds.has(p.game_id)).map((p) => p.id));
  const cardIds = new Set(stateCache.cards.filter((c) => positionIds.has(c.position_id)).map((c) => c.id));

  const gamesDeleted = gameIds.size;
  const positionsDeleted = positionIds.size;
  const cardsDeleted = cardIds.size;
  const reviewsDeleted = stateCache.reviews.filter((r) => cardIds.has(Number(r.card_id))).length;

  stateCache.games = stateCache.games.filter((g) => !gameIds.has(g.id));
  stateCache.positions = stateCache.positions.filter((p) => !positionIds.has(p.id));
  stateCache.candidate_lines = stateCache.candidate_lines.filter((l) => !positionIds.has(l.position_id));
  stateCache.practical_responses = stateCache.practical_responses.filter((r) => !positionIds.has(r.position_id));
  stateCache.cards = stateCache.cards.filter((c) => !cardIds.has(c.id));
  stateCache.reviews = stateCache.reviews.filter((r) => !cardIds.has(Number(r.card_id)));

  await saveState();
  return toJsonResponse({
    username,
    games_deleted: gamesDeleted,
    positions_deleted: positionsDeleted,
    cards_deleted: cardsDeleted,
    reviews_deleted: reviewsDeleted,
  });
}

async function handleImportPgn(options) {
  const form = options?.body instanceof FormData ? options.body : null;
  const file = form ? form.get('file') : null;
  if (!file) return toJsonResponse({ detail: 'Missing file' }, 400);
  const text = await file.text();
  const games = splitPgnGames(text);
  if (!games.length) return toJsonResponse({ detail: 'No PGN games found' }, 400);

  const username = profileFromFilename(file.name || 'pgn-import');
  const primaryPlayer = primaryPlayerFromPgnGames(games);
  let imported = 0;
  let skipped = 0;

  for (let i = 0; i < games.length; i += 1) {
    const pgn = games[i];
    const headers = parsePgnHeaders(pgn);
    const pgnHash = hashString32(pgn);
    const sourceGameId = `pgn-${pgnHash}`;
    const exists = stateCache.games.some((x) => {
      if (!(x.source === 'pgn' && lower(x.username) === username)) return false;
      const existingHash = String(x.pgn_hash || '').trim();
      if (existingHash) return existingHash === pgnHash;
      if (String(x.source_game_id || '') === sourceGameId) return true;
      return String(x.pgn || '') === pgn;
    });
    if (exists) {
      skipped += 1;
      continue;
    }
    const playedColor = playedColorFromHeadersWithPrimary(pgn, primaryPlayer);
    stateCache.games.push({
      id: nextId('game'),
      source: 'pgn',
      source_game_id: sourceGameId,
      pgn_hash: pgnHash,
      username,
      played_color: playedColor,
      result: resultFromPgn(pgn, playedColor),
      pgn,
      analyzed: 0,
      created_at: nowIsoUtc(),
    });
    imported += 1;
  }

  await saveState();
  return toJsonResponse({ username, imported, skipped, total: games.length });
}

function computeUserStats(username, severity) {
  const u = lower(username);
  const gameIds = getGameIdsByUser(u);
  const positions = getPositionsByGameIds(gameIds);
  const posById = new Map(positions.map((p) => [p.id, p]));
  const now = sqlTsToDate(nowIsoUtc()).getTime();
  const cards = stateCache.cards.filter((c) => posById.has(c.position_id));

  const matches = (p) => matchesPositionFilter(p, severity);
  const blunders = positions.filter(matches).length;
  const dueCardsNow = cards.filter((c) => {
    const p = posById.get(c.position_id);
    return p && matches(p) && sqlTsToDate(c.due_at).getTime() <= now;
  });
  const wrongDue = cards.filter((c) => {
    const p = posById.get(c.position_id);
    if (!p || !matches(p)) return false;
    if (!(Number(c.reps || 0) > 0 && ['learning', 'relearning'].includes(String(c.state)))) return false;
    return sqlTsToDate(c.due_at).getTime() <= now;
  }).length;
  const reviewDue = dueCardsNow.filter((c) => Number(c.reps || 0) > 0 && String(c.state) === 'review').length;
  const newDue = dueCardsNow.filter((c) => Number(c.reps || 0) === 0).length;

  return {
    username: u,
    games: gameIds.length,
    positions: positions.length,
    blunders,
    due_cards: newDue + wrongDue + reviewDue,
    wrong_due_cards: wrongDue,
    learn_due_cards: wrongDue,
    review_due_cards: reviewDue,
    new_due_cards: newDue,
  };
}

async function handleUsers(url) {
  const severity = parseSeverityFilter(url);
  const users = [...new Set(stateCache.games.map((g) => lower(g.username)))].sort();
  let created = 0;
  for (const u of users) created += ensureCardsForFilter(u, severity);
  if (created > 0) await saveState();
  return toJsonResponse({ users: users.map((u) => computeUserStats(u, severity)) });
}

async function handleStats(url, username) {
  const severity = parseSeverityFilter(url);
  const created = ensureCardsForFilter(username, severity);
  if (created > 0) await saveState();
  return toJsonResponse(computeUserStats(username, severity));
}

function handleAnkiStats(username, url) {
  const days = parseIntQ(url, 'days', 60);
  const u = lower(username);
  const gameIds = new Set(getGameIdsByUser(u));
  const positions = new Map(stateCache.positions.filter((p) => gameIds.has(p.game_id)).map((p) => [p.id, p]));
  const cards = new Map(stateCache.cards.filter((c) => positions.has(c.position_id)).map((c) => [c.id, c]));
  const reviews = stateCache.reviews.filter((r) => cards.has(r.card_id));

  const total = reviews.length;
  const again = reviews.filter((r) => Number(r.rating) === 1).length;
  const hard = reviews.filter((r) => Number(r.rating) === 2).length;
  const good = reviews.filter((r) => Number(r.rating) === 3).length;
  const easy = reviews.filter((r) => Number(r.rating) === 4).length;
  const correct = hard + good + easy;

  const byDayMap = new Map();
  const minTs = Date.now() - days * 86400 * 1000;
  for (const r of reviews) {
    const ts = sqlTsToDate(r.reviewed_at).getTime();
    if (ts < minTs) continue;
    const d = dateToSqlTs(new Date(ts)).slice(0, 10);
    const cur = byDayMap.get(d) || { day: d, reviews: 0, correct: 0 };
    cur.reviews += 1;
    if (Number(r.rating) > 1) cur.correct += 1;
    byDayMap.set(d, cur);
  }
  const byDay = [...byDayMap.values()].sort((a, b) => String(a.day).localeCompare(String(b.day))).map((x) => ({
    ...x,
    retention_pct: x.reviews ? Number(((x.correct * 100) / x.reviews).toFixed(1)) : 0,
  }));

  return toJsonResponse({
    summary: {
      total_reviews: total,
      again,
      hard,
      good,
      easy,
      retention_pct: total ? Number(((correct * 100) / total).toFixed(1)) : 0,
      avg_interval_days: reviews.length ? Number((reviews.reduce((a, r) => a + Number(r.elapsed_days || 0), 0) / reviews.length).toFixed(2)) : 0,
    },
    by_day: byDay,
    interval_buckets: {},
  });
}

function handleSessionStats(url, username) {
  const breakMinutes = parseIntQ(url, 'break_minutes', 60);
  const breakMs = Math.max(1, Number(breakMinutes)) * 60 * 1000;
  const u = lower(username);
  const gameIds = new Set(getGameIdsByUser(u));
  const positions = new Map(stateCache.positions.filter((p) => gameIds.has(p.game_id)).map((p) => [p.id, p]));
  const cards = new Map(stateCache.cards.filter((c) => positions.has(c.position_id)).map((c) => [c.id, c]));
  const reviews = stateCache.reviews
    .filter((r) => cards.has(Number(r.card_id)))
    .sort((a, b) => sqlTsToDate(String(b.reviewed_at || '')).getTime() - sqlTsToDate(String(a.reviewed_at || '')).getTime());

  if (!reviews.length) {
    return toJsonResponse({ reviewed: 0, attempts: 0, correct: 0, wrong: 0, streak: 0, bestStreak: 0 });
  }

  const session = [reviews[0]];
  let prevTs = sqlTsToDate(String(reviews[0].reviewed_at || '')).getTime();
  for (let i = 1; i < reviews.length; i += 1) {
    const cur = reviews[i];
    const ts = sqlTsToDate(String(cur.reviewed_at || '')).getTime();
    if (!Number.isFinite(ts) || !Number.isFinite(prevTs)) break;
    if ((prevTs - ts) > breakMs) break;
    session.push(cur);
    prevTs = ts;
  }

  const ordered = session.slice().reverse();
  let correct = 0;
  let wrong = 0;
  let streak = 0;
  let bestStreak = 0;
  for (const r of ordered) {
    const ok = Number(r.rating || 0) > 1;
    if (ok) {
      correct += 1;
      streak += 1;
      if (streak > bestStreak) bestStreak = streak;
    } else {
      wrong += 1;
      streak = 0;
    }
  }
  return toJsonResponse({
    reviewed: ordered.length,
    attempts: ordered.length,
    correct,
    wrong,
    streak,
    bestStreak,
  });
}

function handleReviewNext(url, username) {
  const severity = parseSeverityFilter(url);
  ensureCardsForFilter(username, severity);
  const rows = filterDueCardsForUser(username, severity);
  rows.sort((a, b) => {
    const ar = Number(a.card.reps || 0) > 0 ? 0 : 1;
    const br = Number(b.card.reps || 0) > 0 ? 0 : 1;
    if (ar !== br) return ar - br;
    return sqlTsToDate(a.card.due_at).getTime() - sqlTsToDate(b.card.due_at).getTime();
  });
  const first = rows[0];
  if (!first) return toJsonResponse({ card: null });
  const game = stateCache.games.find((g) => g.id === first.position.game_id);
  const gameHeaders = parsePgnHeaders(game?.pgn || '');
  const sourceUrl = String(gameHeaders.Site || gameHeaders.Link || '').trim();
  const lines = stateCache.candidate_lines
    .filter((l) => l.position_id === first.position.id)
    .sort((a, b) => Number(a.pv_rank) - Number(b.pv_rank))
    .map((r) => ({
      first_move_uci: r.first_move_uci,
      san_line: r.san_line,
      cp: Number(r.cp),
      rank: Number(r.pv_rank),
      is_acceptable: Boolean(r.is_acceptable),
    }));
  const practical = stateCache.practical_responses.find((r) => r.position_id === first.position.id);
  return toJsonResponse({
    card: {
      card_id: first.card.id,
      ply: Number(first.position.ply || 0),
      fen: first.position.fen,
      side_to_move: first.position.side_to_move,
      loss_cp: first.position.loss_cp,
      played_uci: first.position.played_uci,
      played_san: first.position.played_san,
      source: game?.source || '',
      source_game_id: game?.source_game_id || '',
      source_url: sourceUrl,
      best_cp: first.position.best_cp,
      played_cp: first.position.played_cp,
      judgement: first.position.judgement || '',
      winning_chance_delta: Number(first.position.winning_chance_delta || 0),
      acceptable_lines: lines.filter((l) => l.is_acceptable),
      all_lines: lines,
      practical_response: practical
        ? {
            opponent_move_uci: practical.opponent_move_uci,
            opponent_move_san: practical.opponent_move_san,
            cp_after: practical.cp_after ?? null,
          }
        : null,
    },
  });
}

function handleReviewPreview(cardId) {
  const card = stateCache.cards.find((c) => c.id === Number(cardId));
  if (!card) return toJsonResponse({ detail: 'Card not found' }, 404);
  const dueByRating = {};
  for (const r of [1, 2, 3, 4]) {
    dueByRating[String(r)] = nextReview(card, r).due_at;
  }
  return toJsonResponse({ due_by_rating: dueByRating });
}

async function handleReviewGrade(body) {
  const card = stateCache.cards.find((c) => c.id === Number(body.card_id));
  if (!card) return toJsonResponse({ detail: 'Card not found' }, 404);
  const rr = nextReview(card, Number(body.rating));
  const last = card.last_review_at ? sqlTsToDate(card.last_review_at) : sqlTsToDate(nowIsoUtc());
  const elapsed = Math.max(0, (sqlTsToDate(rr.reviewed_at).getTime() - last.getTime()) / 86400000);

  Object.assign(card, {
    state: rr.state,
    step: rr.step,
    due_at: rr.due_at,
    stability: rr.stability,
    difficulty: rr.difficulty,
    reps: rr.reps,
    lapses: rr.lapses,
    last_review_at: rr.last_review_at,
  });

  stateCache.reviews.push({
    id: nextId('review'),
    card_id: Number(card.id),
    rating: Number(body.rating),
    reviewed_at: rr.reviewed_at,
    next_due_at: rr.due_at,
    elapsed_days: elapsed,
  });
  await saveState();
  return toJsonResponse({
    card_id: Number(card.id),
    next_due_at: rr.due_at,
    state: rr.state,
    step: rr.step,
    stability: rr.stability,
    difficulty: rr.difficulty,
    reps: rr.reps,
    lapses: rr.lapses,
  });
}

function handleAnalyzeGames(username, url) {
  const max = parseIntQ(url, 'max_games', 200);
  const u = lower(username);
  const games = stateCache.games.filter((g) => lower(g.username) === u && !g.analyzed).slice(0, max).map((g) => ({
    id: g.id,
    played_color: g.played_color,
    pgn: g.pgn,
  }));
  return toJsonResponse({ games, total_games: games.length });
}

async function handleAnalyzeStoreGame(body) {
  const gameId = Number(body.game_id);
  const positions = Array.isArray(body.positions) ? body.positions : [];
  const posIds = new Set(stateCache.positions.filter((p) => p.game_id === gameId).map((p) => p.id));
  stateCache.positions = stateCache.positions.filter((p) => p.game_id !== gameId);
  stateCache.candidate_lines = stateCache.candidate_lines.filter((l) => !posIds.has(l.position_id));
  stateCache.practical_responses = stateCache.practical_responses.filter((r) => !posIds.has(r.position_id));
  stateCache.cards = stateCache.cards.filter((c) => !posIds.has(c.position_id));
  stateCache.reviews = stateCache.reviews.filter((r) => {
    const card = stateCache.cards.find((c) => c.id === r.card_id);
    return Boolean(card);
  });

  let blunders = 0;
  for (const p of positions) {
    const posId = nextId('position');
    stateCache.positions.push({
      id: posId,
      game_id: gameId,
      ply: Number(p.ply),
      fen: p.fen,
      side_to_move: p.side_to_move,
      played_uci: p.played_uci,
      played_san: p.played_san,
      best_cp: Number(p.best_cp),
      played_cp: Number(p.played_cp),
      loss_cp: Number(p.loss_cp),
      judgement: String(p.judgement || ''),
      winning_chance_delta: Number(p.winning_chance_delta || 0),
      created_at: nowIsoUtc(),
    });
    if (String(p.judgement || '').toLowerCase() === 'blunder') blunders += 1;

    for (const c of (p.candidate_lines || [])) {
      stateCache.candidate_lines.push({
        id: nextId('line'),
        position_id: posId,
        pv_rank: Number(c.pv_rank),
        cp: Number(c.cp),
        first_move_uci: c.first_move_uci,
        uci_line: c.uci_line,
        san_line: c.san_line,
        is_acceptable: c.is_acceptable ? 1 : 0,
      });
    }
    if (p.practical_response) {
      stateCache.practical_responses.push({
        id: nextId('practical'),
        position_id: posId,
        opponent_move_uci: p.practical_response.opponent_move_uci,
        opponent_move_san: p.practical_response.opponent_move_san,
        cp_after: p.practical_response.cp_after ?? null,
      });
    }
  }
  const g = stateCache.games.find((x) => x.id === gameId);
  if (g) g.analyzed = 1;
  await saveState();
  return toJsonResponse({ positions: positions.length, blunders });
}

async function handleAnalyzeReset(body) {
  const u = lower(body.username);
  const gameIds = getGameIdsByUser(u);
  const s = new Set(gameIds);
  const posIds = new Set(stateCache.positions.filter((p) => s.has(p.game_id)).map((p) => p.id));
  const positionsDeleted = posIds.size;
  stateCache.positions = stateCache.positions.filter((p) => !s.has(p.game_id));
  stateCache.candidate_lines = stateCache.candidate_lines.filter((l) => !posIds.has(l.position_id));
  stateCache.practical_responses = stateCache.practical_responses.filter((r) => !posIds.has(r.position_id));
  stateCache.cards = stateCache.cards.filter((c) => !posIds.has(c.position_id));
  const cardIds = new Set(stateCache.cards.map((c) => c.id));
  stateCache.reviews = stateCache.reviews.filter((r) => cardIds.has(r.card_id));
  for (const g of stateCache.games) {
    if (s.has(g.id)) g.analyzed = 0;
  }
  await saveState();
  return toJsonResponse({ games_reset: gameIds.length, positions_deleted: positionsDeleted });
}

async function handleDbExport() {
  const state = await loadState();
  return new Response(JSON.stringify(state), {
    status: 200,
    headers: {
      'content-type': 'application/json',
      'content-disposition': `attachment; filename="blunderfix-local-${Date.now()}.json"`,
    },
  });
}

async function handleDbImport(options) {
  const form = options?.body instanceof FormData ? options.body : null;
  const file = form ? form.get('file') : null;
  if (!file) return toJsonResponse({ detail: 'Missing file' }, 400);
  const txt = await file.text();
  let obj = null;
  try {
    obj = JSON.parse(txt);
  } catch {
    return toJsonResponse({ detail: 'Invalid JSON' }, 400);
  }
  if (!obj || typeof obj !== 'object' || !Array.isArray(obj.games) || !Array.isArray(obj.positions)) {
    return toJsonResponse({ detail: 'Invalid DB payload' }, 400);
  }
  stateCache = obj;
  if (!stateCache.nextIds) stateCache.nextIds = structuredClone(DEFAULT_STATE.nextIds);
  await saveState();
  return toJsonResponse({ ok: true });
}

async function routeApi(url, options = {}) {
  await loadState();
  const method = String(options.method || 'GET').toUpperCase();
  const path = new URL(url, window.location.origin).pathname;
  const body = options.body && !(options.body instanceof FormData) ? JSON.parse(options.body) : null;

  if (path === '/api/import/start' && method === 'POST') return handleImportStart(body || {});
  if (path === '/api/import/pgn' && method === 'POST') return handleImportPgn(options);
  if (path.startsWith('/api/import/progress/') && method === 'GET') return handleImportProgress(path.split('/').pop() || '');
  if (path === '/api/import/clear-all' && method === 'POST') return handleImportClearAll();
  if (path === '/api/import/clear-user' && method === 'POST') return handleImportClearUser(body || {});

  if (path.startsWith('/api/analyze/games/') && method === 'GET') {
    return handleAnalyzeGames(decodeURIComponent(path.split('/').pop() || ''), url);
  }
  if (path === '/api/analyze/store-game' && method === 'POST') return handleAnalyzeStoreGame(body || {});
  if (path === '/api/analyze/reset' && method === 'POST') return handleAnalyzeReset(body || {});

  if (path === '/api/users' && method === 'GET') return handleUsers(url);
  if (path.startsWith('/api/stats/anki/') && method === 'GET') {
    return handleAnkiStats(decodeURIComponent(path.split('/').pop() || ''), url);
  }
  if (path.startsWith('/api/stats/session/') && method === 'GET') {
    return handleSessionStats(url, decodeURIComponent(path.split('/').pop() || ''));
  }
  if (path.startsWith('/api/stats/') && method === 'GET') {
    return handleStats(url, decodeURIComponent(path.split('/').pop() || ''));
  }

  if (path.startsWith('/api/review/next/') && method === 'GET') {
    return handleReviewNext(url, decodeURIComponent(path.split('/').pop() || ''));
  }
  if (path.startsWith('/api/review/preview/') && method === 'GET') return handleReviewPreview(path.split('/').pop() || '');
  if (path === '/api/review/grade' && method === 'POST') return handleReviewGrade(body || {});

  if (path === '/api/db/export' && method === 'GET') return handleDbExport();
  if (path === '/api/db/import' && method === 'POST') return handleDbImport(options);

  if ((path === '/api/eval' || path === '/api/reply-lines') && method === 'POST') {
    return toJsonResponse({ detail: 'Not available in local mode' }, 503);
  }

  return toJsonResponse({ detail: `Local API route not implemented: ${method} ${path}` }, 404);
}

export function installLocalApi() {
  const realFetch = window.fetch.bind(window);
  window.fetch = async (input, init = undefined) => {
    const url = typeof input === 'string' ? input : (input?.url || '');
    if (url.startsWith('/api/')) {
      return await routeApi(url, init || {});
    }
    return await realFetch(input, init);
  };
}
