from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import Any

DB_PATH = Path(os.getenv("BLUNDERFIX_DB_PATH", "blunderfix.db")).expanduser()


def _norm_username(username: str) -> str:
    return username.strip().lower()


def get_db_path() -> Path:
    return DB_PATH


def get_conn() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db() -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.executescript(
        """
        CREATE TABLE IF NOT EXISTS games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_game_id TEXT NOT NULL,
            username TEXT NOT NULL,
            played_color TEXT NOT NULL,
            result TEXT,
            pgn TEXT NOT NULL,
            analyzed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, source_game_id)
        );

        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL REFERENCES games(id) ON DELETE CASCADE,
            ply INTEGER NOT NULL,
            fen TEXT NOT NULL,
            side_to_move TEXT NOT NULL,
            played_uci TEXT NOT NULL,
            played_san TEXT NOT NULL,
            best_cp INTEGER NOT NULL,
            played_cp INTEGER NOT NULL,
            loss_cp INTEGER NOT NULL,
            is_blunder INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS candidate_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
            pv_rank INTEGER NOT NULL,
            cp INTEGER NOT NULL,
            first_move_uci TEXT NOT NULL,
            uci_line TEXT NOT NULL,
            san_line TEXT NOT NULL,
            is_acceptable INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS practical_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
            opponent_move_uci TEXT NOT NULL,
            opponent_move_san TEXT NOT NULL,
            cp_after INTEGER
        );

        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL UNIQUE REFERENCES positions(id) ON DELETE CASCADE,
            state TEXT NOT NULL DEFAULT 'learning',
            step INTEGER NOT NULL DEFAULT 0,
            due_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            stability REAL NOT NULL DEFAULT 0.4,
            difficulty REAL NOT NULL DEFAULT 5.0,
            reps INTEGER NOT NULL DEFAULT 0,
            lapses INTEGER NOT NULL DEFAULT 0,
            last_review_at TEXT
        );

        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
            rating INTEGER NOT NULL,
            reviewed_at TEXT NOT NULL,
            next_due_at TEXT NOT NULL,
            elapsed_days REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_games_username ON games(username);
        CREATE INDEX IF NOT EXISTS idx_positions_game ON positions(game_id);
        CREATE INDEX IF NOT EXISTS idx_cards_due ON cards(due_at);
        """
    )
    _migrate_games_unique_constraint(conn)
    _normalize_username_storage(conn)
    _repair_analysis_foreign_keys(conn)
    _migrate_cards_step_column(conn)
    conn.commit()
    conn.close()


def _migrate_games_unique_constraint(conn: sqlite3.Connection) -> None:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name='games'"
    ).fetchone()
    if not row or not row["sql"]:
        return

    create_sql = row["sql"].replace(" ", "")
    if "UNIQUE(source,username,source_game_id)".replace(" ", "") in create_sql:
        return
    if "UNIQUE(source,source_game_id)".replace(" ", "") not in create_sql:
        return

    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute(
        """
        CREATE TABLE games_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_game_id TEXT NOT NULL,
            username TEXT NOT NULL,
            played_color TEXT NOT NULL,
            result TEXT,
            pgn TEXT NOT NULL,
            analyzed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, username, source_game_id)
        );
        """
    )
    conn.execute(
        """
        INSERT INTO games_new (id, source, source_game_id, username, played_color, result, pgn, analyzed, created_at)
        SELECT id, source, source_game_id, username, played_color, result, pgn, analyzed, created_at
        FROM games
        ORDER BY id ASC;
        """
    )
    conn.execute("DROP TABLE games;")
    conn.execute("ALTER TABLE games_new RENAME TO games;")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_games_username ON games(username);")
    conn.execute("PRAGMA foreign_keys=ON;")


def _repair_analysis_foreign_keys(conn: sqlite3.Connection) -> None:
    fk_rows = conn.execute("PRAGMA foreign_key_list('positions')").fetchall()
    if not fk_rows:
        return
    if fk_rows[0]["table"] == "games":
        return

    conn.execute("PRAGMA foreign_keys=OFF;")
    _rebuild_analysis_tables(conn)
    conn.execute("PRAGMA foreign_keys=ON;")


def _rebuild_analysis_tables(conn: sqlite3.Connection) -> None:
    conn.execute("DROP TABLE IF EXISTS reviews;")
    conn.execute("DROP TABLE IF EXISTS cards;")
    conn.execute("DROP TABLE IF EXISTS practical_responses;")
    conn.execute("DROP TABLE IF EXISTS candidate_lines;")
    conn.execute("DROP TABLE IF EXISTS positions;")
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS positions (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            game_id INTEGER NOT NULL REFERENCES games(id) ON DELETE CASCADE,
            ply INTEGER NOT NULL,
            fen TEXT NOT NULL,
            side_to_move TEXT NOT NULL,
            played_uci TEXT NOT NULL,
            played_san TEXT NOT NULL,
            best_cp INTEGER NOT NULL,
            played_cp INTEGER NOT NULL,
            loss_cp INTEGER NOT NULL,
            is_blunder INTEGER NOT NULL,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP
        );

        CREATE TABLE IF NOT EXISTS candidate_lines (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
            pv_rank INTEGER NOT NULL,
            cp INTEGER NOT NULL,
            first_move_uci TEXT NOT NULL,
            uci_line TEXT NOT NULL,
            san_line TEXT NOT NULL,
            is_acceptable INTEGER NOT NULL
        );

        CREATE TABLE IF NOT EXISTS practical_responses (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL REFERENCES positions(id) ON DELETE CASCADE,
            opponent_move_uci TEXT NOT NULL,
            opponent_move_san TEXT NOT NULL,
            cp_after INTEGER
        );

        CREATE TABLE IF NOT EXISTS cards (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            position_id INTEGER NOT NULL UNIQUE REFERENCES positions(id) ON DELETE CASCADE,
            state TEXT NOT NULL DEFAULT 'learning',
            step INTEGER NOT NULL DEFAULT 0,
            due_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            stability REAL NOT NULL DEFAULT 0.4,
            difficulty REAL NOT NULL DEFAULT 5.0,
            reps INTEGER NOT NULL DEFAULT 0,
            lapses INTEGER NOT NULL DEFAULT 0,
            last_review_at TEXT
        );

        CREATE TABLE IF NOT EXISTS reviews (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            card_id INTEGER NOT NULL REFERENCES cards(id) ON DELETE CASCADE,
            rating INTEGER NOT NULL,
            reviewed_at TEXT NOT NULL,
            next_due_at TEXT NOT NULL,
            elapsed_days REAL NOT NULL
        );

        CREATE INDEX IF NOT EXISTS idx_positions_game ON positions(game_id);
        CREATE INDEX IF NOT EXISTS idx_cards_due ON cards(due_at);
        """
    )


def _migrate_cards_step_column(conn: sqlite3.Connection) -> None:
    cols = conn.execute("PRAGMA table_info('cards')").fetchall()
    if not any(c["name"] == "step" for c in cols):
        conn.execute("ALTER TABLE cards ADD COLUMN step INTEGER NOT NULL DEFAULT 0")


def _normalize_username_storage(conn: sqlite3.Connection) -> None:
    has_mixed_case = conn.execute(
        "SELECT 1 FROM games WHERE username != lower(username) LIMIT 1"
    ).fetchone()
    has_case_dupes = conn.execute(
        """
        SELECT 1
        FROM games
        GROUP BY source, lower(username), source_game_id
        HAVING COUNT(*) > 1
        LIMIT 1
        """
    ).fetchone()
    if not has_mixed_case and not has_case_dupes:
        return

    conn.execute("PRAGMA foreign_keys=OFF;")
    conn.execute("ALTER TABLE games RENAME TO games_old_case;")
    conn.execute(
        """
        CREATE TABLE games (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            source TEXT NOT NULL,
            source_game_id TEXT NOT NULL,
            username TEXT NOT NULL,
            played_color TEXT NOT NULL,
            result TEXT,
            pgn TEXT NOT NULL,
            analyzed INTEGER NOT NULL DEFAULT 0,
            created_at TEXT NOT NULL DEFAULT CURRENT_TIMESTAMP,
            UNIQUE(source, username, source_game_id)
        );
        """
    )
    conn.execute(
        """
        INSERT OR IGNORE INTO games (source, source_game_id, username, played_color, result, pgn, analyzed, created_at)
        SELECT source, source_game_id, lower(username), played_color, result, pgn, 0, created_at
        FROM games_old_case
        ORDER BY id ASC;
        """
    )
    conn.execute("DROP TABLE games_old_case;")
    conn.execute("CREATE INDEX IF NOT EXISTS idx_games_username ON games(username);")
    # Game IDs may change after merge, so reset derived analysis tables.
    _rebuild_analysis_tables(conn)
    conn.execute("PRAGMA foreign_keys=ON;")


def insert_game(source: str, source_game_id: str, username: str, played_color: str, result: str, pgn: str) -> bool:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()
    try:
        cur.execute(
            """
            INSERT INTO games (source, source_game_id, username, played_color, result, pgn)
            VALUES (?, ?, ?, ?, ?, ?)
            """,
            (source, source_game_id, username, played_color, result, pgn),
        )
        conn.commit()
        return True
    except sqlite3.IntegrityError:
        return False
    finally:
        conn.close()


def fetch_unanalyzed_games(username: str, limit: int) -> list[sqlite3.Row]:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT * FROM games
        WHERE username = ? AND analyzed = 0
        ORDER BY id ASC
        LIMIT ?
        """,
        (username, limit),
    ).fetchall()
    conn.close()
    return rows


def count_unanalyzed_games(username: str, limit: int) -> int:
    username = _norm_username(username)
    conn = get_conn()
    row = conn.execute(
        """
        SELECT COUNT(*) AS c
        FROM (
            SELECT id
            FROM games
            WHERE username = ? AND analyzed = 0
            ORDER BY id ASC
            LIMIT ?
        ) t
        """,
        (username, limit),
    ).fetchone()
    conn.close()
    return int(row["c"] if row else 0)


def mark_game_analyzed(game_id: int) -> None:
    conn = get_conn()
    conn.execute("UPDATE games SET analyzed = 1 WHERE id = ?", (game_id,))
    conn.commit()
    conn.close()


def insert_position(data: dict[str, Any]) -> int:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        INSERT INTO positions (
            game_id, ply, fen, side_to_move, played_uci, played_san,
            best_cp, played_cp, loss_cp, is_blunder
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            data["game_id"],
            data["ply"],
            data["fen"],
            data["side_to_move"],
            data["played_uci"],
            data["played_san"],
            data["best_cp"],
            data["played_cp"],
            data["loss_cp"],
            1 if data["is_blunder"] else 0,
        ),
    )
    position_id = cur.lastrowid
    conn.commit()
    conn.close()
    return int(position_id)


def insert_candidate_lines(position_id: int, rows: list[dict[str, Any]]) -> None:
    conn = get_conn()
    conn.executemany(
        """
        INSERT INTO candidate_lines (
            position_id, pv_rank, cp, first_move_uci, uci_line, san_line, is_acceptable
        ) VALUES (?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                position_id,
                row["pv_rank"],
                row["cp"],
                row["first_move_uci"],
                row["uci_line"],
                row["san_line"],
                1 if row["is_acceptable"] else 0,
            )
            for row in rows
        ],
    )
    conn.commit()
    conn.close()


def insert_practical_response(position_id: int, opponent_uci: str, opponent_san: str, cp_after: int | None) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO practical_responses (position_id, opponent_move_uci, opponent_move_san, cp_after)
        VALUES (?, ?, ?, ?)
        """,
        (position_id, opponent_uci, opponent_san, cp_after),
    )
    conn.commit()
    conn.close()


def upsert_card_for_position(position_id: int) -> None:
    conn = get_conn()
    conn.execute(
        """
        INSERT INTO cards (position_id, due_at)
        VALUES (?, CURRENT_TIMESTAMP)
        ON CONFLICT(position_id) DO NOTHING
        """,
        (position_id,),
    )
    conn.commit()
    conn.close()


def ensure_cards_for_threshold(
    username: str,
    min_loss_cp: int,
    min_best_cp: int = -200,
    winning_prune_cp: int = 300,
) -> int:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()
    rows = cur.execute(
        """
        SELECT p.id
        FROM positions p
        JOIN games g ON g.id = p.game_id
        LEFT JOIN cards c ON c.position_id = p.id
        WHERE lower(g.username) = ?
          AND p.loss_cp >= ?
          AND p.best_cp >= ?
          AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
          AND c.id IS NULL
        """,
        (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
    ).fetchall()
    if rows:
        cur.executemany(
            "INSERT INTO cards (position_id, due_at) VALUES (?, CURRENT_TIMESTAMP)",
            [(r["id"],) for r in rows],
        )
    conn.commit()
    conn.close()
    return len(rows)


def fetch_due_card(
    username: str,
    min_loss_cp: int = 200,
    min_best_cp: int = -200,
    winning_prune_cp: int = 300,
) -> sqlite3.Row | None:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()
    row = cur.execute(
        """
        SELECT
            c.id AS card_id,
            c.position_id,
            c.state,
            c.due_at,
            c.stability,
            c.difficulty,
            c.reps,
            c.lapses,
            p.fen,
            p.side_to_move,
            p.played_uci,
            p.played_san,
            p.loss_cp,
            p.best_cp,
            p.played_cp,
            g.source,
            g.source_game_id
        FROM cards c
        JOIN positions p ON p.id = c.position_id
        JOIN games g ON g.id = p.game_id
        WHERE lower(g.username) = ?
          AND c.due_at <= CURRENT_TIMESTAMP
          AND p.loss_cp >= ?
          AND p.best_cp >= ?
          AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
        ORDER BY CASE WHEN c.reps > 0 THEN 0 ELSE 1 END ASC, c.due_at ASC
        LIMIT 1
        """,
        (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
    ).fetchone()
    conn.close()
    return row


def fetch_lines_for_position(position_id: int) -> list[sqlite3.Row]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT * FROM candidate_lines
        WHERE position_id = ?
        ORDER BY pv_rank ASC
        """,
        (position_id,),
    ).fetchall()
    conn.close()
    return rows


def fetch_practical_response(position_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute(
        """
        SELECT * FROM practical_responses
        WHERE position_id = ?
        LIMIT 1
        """,
        (position_id,),
    ).fetchone()
    conn.close()
    return row


def get_card(card_id: int) -> sqlite3.Row | None:
    conn = get_conn()
    row = conn.execute("SELECT * FROM cards WHERE id = ?", (card_id,)).fetchone()
    conn.close()
    return row


def update_card_review(
    card_id: int,
    *,
    state: str,
    step: int,
    due_at: str,
    stability: float,
    difficulty: float,
    reps: int,
    lapses: int,
    reviewed_at: str,
    rating: int,
    elapsed_days: float,
) -> None:
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        UPDATE cards
        SET state = ?, step = ?, due_at = ?, stability = ?, difficulty = ?, reps = ?, lapses = ?, last_review_at = ?
        WHERE id = ?
        """,
        (state, step, due_at, stability, difficulty, reps, lapses, reviewed_at, card_id),
    )
    cur.execute(
        """
        INSERT INTO reviews (card_id, rating, reviewed_at, next_due_at, elapsed_days)
        VALUES (?, ?, ?, ?, ?)
        """,
        (card_id, rating, reviewed_at, due_at, elapsed_days),
    )
    conn.commit()
    conn.close()


def stats(
    username: str,
    min_loss_cp: int = 200,
    min_best_cp: int = -200,
    winning_prune_cp: int = 300,
) -> dict[str, int]:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()
    out = {
        "games": cur.execute("SELECT COUNT(*) AS c FROM games WHERE lower(username) = ?", (username,)).fetchone()["c"],
        "positions": cur.execute(
            "SELECT COUNT(*) AS c FROM positions p JOIN games g ON g.id = p.game_id WHERE lower(g.username) = ?",
            (username,),
        ).fetchone()["c"],
        "blunders": cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM positions p
            JOIN games g ON g.id = p.game_id
            WHERE lower(g.username) = ?
              AND p.loss_cp >= ?
              AND p.best_cp >= ?
              AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            """,
            (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
        ).fetchone()["c"],
        "due_cards": cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM cards c
            JOIN positions p ON p.id = c.position_id
            JOIN games g ON g.id = p.game_id
            WHERE lower(g.username) = ?
              AND c.due_at <= CURRENT_TIMESTAMP
              AND p.loss_cp >= ?
              AND p.best_cp >= ?
              AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            """,
            (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
        ).fetchone()["c"],
        "wrong_due_cards": cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM cards c
            JOIN positions p ON p.id = c.position_id
            JOIN games g ON g.id = p.game_id
            WHERE lower(g.username) = ?
              AND c.due_at <= CURRENT_TIMESTAMP
              AND c.reps > 0
              AND c.state IN ('learning', 'relearning')
              AND p.loss_cp >= ?
              AND p.best_cp >= ?
              AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            """,
            (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
        ).fetchone()["c"],
        "review_due_cards": cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM cards c
            JOIN positions p ON p.id = c.position_id
            JOIN games g ON g.id = p.game_id
            WHERE lower(g.username) = ?
              AND c.due_at <= CURRENT_TIMESTAMP
              AND c.reps > 0
              AND c.state = 'review'
              AND p.loss_cp >= ?
              AND p.best_cp >= ?
              AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            """,
            (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
        ).fetchone()["c"],
        "new_due_cards": cur.execute(
            """
            SELECT COUNT(*) AS c
            FROM cards c
            JOIN positions p ON p.id = c.position_id
            JOIN games g ON g.id = p.game_id
            WHERE lower(g.username) = ?
              AND c.due_at <= CURRENT_TIMESTAMP
              AND c.reps = 0
              AND p.loss_cp >= ?
              AND p.best_cp >= ?
              AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            """,
            (username, min_loss_cp, min_best_cp, winning_prune_cp, winning_prune_cp),
        ).fetchone()["c"],
    }
    out["learn_due_cards"] = out["wrong_due_cards"]
    conn.close()
    return out


def reset_analysis_for_user(username: str) -> dict[str, int]:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()

    games = cur.execute("SELECT id FROM games WHERE lower(username) = ?", (username,)).fetchall()
    game_ids = [row["id"] for row in games]
    if not game_ids:
        conn.close()
        return {"games_reset": 0, "positions_deleted": 0}

    placeholders = ",".join("?" for _ in game_ids)
    positions_deleted = cur.execute(
        f"SELECT COUNT(*) AS c FROM positions WHERE game_id IN ({placeholders})",
        game_ids,
    ).fetchone()["c"]

    # Cascades remove candidate_lines/practical_responses/cards/reviews.
    cur.execute(f"DELETE FROM positions WHERE game_id IN ({placeholders})", game_ids)
    cur.execute("UPDATE games SET analyzed = 0 WHERE lower(username) = ?", (username,))
    conn.commit()
    conn.close()
    return {"games_reset": len(game_ids), "positions_deleted": positions_deleted}


def replace_analysis_for_game(game_id: int, positions: list[dict[str, Any]]) -> dict[str, int]:
    conn = get_conn()
    cur = conn.cursor()

    cur.execute("DELETE FROM positions WHERE game_id = ?", (game_id,))

    positions_written = 0
    blunders = 0
    for p in positions:
        cur.execute(
            """
            INSERT INTO positions (
                game_id, ply, fen, side_to_move, played_uci, played_san,
                best_cp, played_cp, loss_cp, is_blunder
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                int(game_id),
                int(p["ply"]),
                p["fen"],
                p["side_to_move"],
                p["played_uci"],
                p["played_san"],
                int(p["best_cp"]),
                int(p["played_cp"]),
                int(p["loss_cp"]),
                1 if bool(p.get("is_blunder")) else 0,
            ),
        )
        position_id = int(cur.lastrowid)
        positions_written += 1
        if bool(p.get("is_blunder")):
            blunders += 1

        candidates = p.get("candidate_lines") or []
        if candidates:
            cur.executemany(
                """
                INSERT INTO candidate_lines (
                    position_id, pv_rank, cp, first_move_uci, uci_line, san_line, is_acceptable
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        position_id,
                        int(c["pv_rank"]),
                        int(c["cp"]),
                        c["first_move_uci"],
                        c["uci_line"],
                        c["san_line"],
                        1 if bool(c.get("is_acceptable")) else 0,
                    )
                    for c in candidates
                ],
            )

        practical = p.get("practical_response")
        if practical:
            cur.execute(
                """
                INSERT INTO practical_responses (position_id, opponent_move_uci, opponent_move_san, cp_after)
                VALUES (?, ?, ?, ?)
                """,
                (
                    position_id,
                    practical["opponent_move_uci"],
                    practical["opponent_move_san"],
                    (None if practical.get("cp_after") is None else int(practical["cp_after"])),
                ),
            )

    cur.execute("UPDATE games SET analyzed = 1 WHERE id = ?", (int(game_id),))
    conn.commit()
    conn.close()
    return {"positions": positions_written, "blunders": blunders}


def list_user_stats(
    min_loss_cp: int = 200,
    min_best_cp: int = -200,
    winning_prune_cp: int = 300,
) -> list[dict[str, int | str]]:
    conn = get_conn()
    rows = conn.execute(
        """
        SELECT
            u.username AS username,
            (SELECT COUNT(*) FROM games g WHERE lower(g.username) = u.username) AS games,
            (
                SELECT COUNT(*)
                FROM positions p
                JOIN games g2 ON g2.id = p.game_id
                WHERE lower(g2.username) = u.username
            ) AS positions,
            (
                SELECT COUNT(*)
                FROM positions p
                JOIN games g2 ON g2.id = p.game_id
                WHERE lower(g2.username) = u.username
                  AND p.loss_cp >= ?
                  AND p.best_cp >= ?
                  AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            ) AS blunders,
            (
                SELECT COUNT(*)
                FROM cards c
                JOIN positions p ON p.id = c.position_id
                JOIN games g3 ON g3.id = p.game_id
                WHERE lower(g3.username) = u.username
                  AND c.due_at <= CURRENT_TIMESTAMP
                  AND p.loss_cp >= ?
                  AND p.best_cp >= ?
                  AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            ) AS due_cards
            ,
            (
                SELECT COUNT(*)
                FROM cards c
                JOIN positions p ON p.id = c.position_id
                JOIN games g4 ON g4.id = p.game_id
                WHERE lower(g4.username) = u.username
                  AND c.due_at <= CURRENT_TIMESTAMP
                  AND c.reps > 0
                  AND c.state IN ('learning', 'relearning')
                  AND p.loss_cp >= ?
                  AND p.best_cp >= ?
                  AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            ) AS wrong_due_cards
            ,
            (
                SELECT COUNT(*)
                FROM cards c
                JOIN positions p ON p.id = c.position_id
                JOIN games g5 ON g5.id = p.game_id
                WHERE lower(g5.username) = u.username
                  AND c.due_at <= CURRENT_TIMESTAMP
                  AND c.reps > 0
                  AND c.state = 'review'
                  AND p.loss_cp >= ?
                  AND p.best_cp >= ?
                  AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            ) AS review_due_cards
            ,
            (
                SELECT COUNT(*)
                FROM cards c
                JOIN positions p ON p.id = c.position_id
                JOIN games g6 ON g6.id = p.game_id
                WHERE lower(g6.username) = u.username
                  AND c.due_at <= CURRENT_TIMESTAMP
                  AND c.reps = 0
                  AND p.loss_cp >= ?
                  AND p.best_cp >= ?
                  AND NOT (p.best_cp >= ? AND p.played_cp >= ?)
            ) AS new_due_cards
        FROM (
            SELECT DISTINCT lower(username) AS username
            FROM games
            ORDER BY lower(username) COLLATE NOCASE ASC
        ) u
        """,
        (
            min_loss_cp,
            min_best_cp,
            winning_prune_cp,
            winning_prune_cp,
            min_loss_cp,
            min_best_cp,
            winning_prune_cp,
            winning_prune_cp,
            min_loss_cp,
            min_best_cp,
            winning_prune_cp,
            winning_prune_cp,
            min_loss_cp,
            min_best_cp,
            winning_prune_cp,
            winning_prune_cp,
            min_loss_cp,
            min_best_cp,
            winning_prune_cp,
            winning_prune_cp,
        ),
    ).fetchall()
    conn.close()
    out_rows = [dict(r) for r in rows]
    for r in out_rows:
        if "learn_due_cards" not in r and "wrong_due_cards" in r:
            r["learn_due_cards"] = r["wrong_due_cards"]
    return out_rows


def anki_stats(username: str, days: int = 60) -> dict[str, object]:
    username = _norm_username(username)
    conn = get_conn()
    cur = conn.cursor()

    total_reviews = cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM reviews r
        JOIN cards c ON c.id = r.card_id
        JOIN positions p ON p.id = c.position_id
        JOIN games g ON g.id = p.game_id
        WHERE lower(g.username) = ?
        """,
        (username,),
    ).fetchone()["c"]
    rating_rows = cur.execute(
        """
        SELECT r.rating AS rating, COUNT(*) AS c
        FROM reviews r
        JOIN cards c ON c.id = r.card_id
        JOIN positions p ON p.id = c.position_id
        JOIN games g ON g.id = p.game_id
        WHERE lower(g.username) = ?
        GROUP BY r.rating
        """,
        (username,),
    ).fetchall()
    rating_counts = {int(r["rating"]): int(r["c"]) for r in rating_rows}
    again = int(rating_counts.get(1, 0))
    hard = int(rating_counts.get(2, 0))
    good = int(rating_counts.get(3, 0))
    easy = int(rating_counts.get(4, 0))
    correct = hard + good + easy
    retention_pct = round((correct * 100.0 / total_reviews), 1) if total_reviews else 0.0

    avg_interval_days = cur.execute(
        """
        SELECT AVG(MAX(julianday(r.next_due_at) - julianday(r.reviewed_at), 0)) AS d
        FROM reviews r
        JOIN cards c ON c.id = r.card_id
        JOIN positions p ON p.id = c.position_id
        JOIN games g ON g.id = p.game_id
        WHERE lower(g.username) = ?
        """,
        (username,),
    ).fetchone()["d"]

    by_day_rows = cur.execute(
        """
        SELECT
          date(r.reviewed_at) AS day,
          COUNT(*) AS reviews,
          SUM(CASE WHEN r.rating > 1 THEN 1 ELSE 0 END) AS correct
        FROM reviews r
        JOIN cards c ON c.id = r.card_id
        JOIN positions p ON p.id = c.position_id
        JOIN games g ON g.id = p.game_id
        WHERE lower(g.username) = ?
          AND date(r.reviewed_at) >= date('now', ?)
        GROUP BY date(r.reviewed_at)
        ORDER BY day ASC
        """,
        (username, f"-{int(days)} days"),
    ).fetchall()
    by_day = [
        {
            "day": r["day"],
            "reviews": int(r["reviews"]),
            "correct": int(r["correct"] or 0),
            "retention_pct": round((int(r["correct"] or 0) * 100.0 / int(r["reviews"])), 1) if int(r["reviews"]) else 0.0,
        }
        for r in by_day_rows
    ]

    bucket_rows = cur.execute(
        """
        SELECT
          CASE
            WHEN d < 1 THEN '<1d'
            WHEN d < 4 THEN '1-3d'
            WHEN d < 8 THEN '4-7d'
            WHEN d < 31 THEN '8-30d'
            ELSE '31d+'
          END AS bucket,
          COUNT(*) AS c
        FROM (
          SELECT MAX(julianday(r.next_due_at) - julianday(r.reviewed_at), 0) AS d
          FROM reviews r
          JOIN cards c ON c.id = r.card_id
          JOIN positions p ON p.id = c.position_id
          JOIN games g ON g.id = p.game_id
          WHERE lower(g.username) = ?
        ) t
        GROUP BY bucket
        """,
        (username,),
    ).fetchall()
    interval_buckets = {r["bucket"]: int(r["c"]) for r in bucket_rows}

    conn.close()
    return {
        "summary": {
            "total_reviews": int(total_reviews),
            "again": again,
            "hard": hard,
            "good": good,
            "easy": easy,
            "retention_pct": retention_pct,
            "avg_interval_days": round(float(avg_interval_days), 2) if avg_interval_days is not None else 0.0,
        },
        "by_day": by_day,
        "interval_buckets": interval_buckets,
    }
