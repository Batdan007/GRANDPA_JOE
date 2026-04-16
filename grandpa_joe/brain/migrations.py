"""
Schema migrations for the Racing Brain.

Run at brain init after CREATE TABLE IF NOT EXISTS. SQLite doesn't add
UNIQUE constraints to existing tables, so when schema.py grows new
constraints we have to detect + rebuild the table here.
"""

import logging
import sqlite3

logger = logging.getLogger(__name__)


def run_migrations(conn: sqlite3.Connection) -> None:
    """Apply pending migrations. Idempotent. FKs off during rebuild."""
    conn.execute("PRAGMA foreign_keys=OFF")
    try:
        _migrate_entries_unique(conn)
        _migrate_results_unique(conn)
        _migrate_past_performances_unique(conn)
    finally:
        conn.execute("PRAGMA foreign_keys=ON")


def _has_unique(conn: sqlite3.Connection, table: str, needle: str) -> bool:
    row = conn.execute(
        "SELECT sql FROM sqlite_master WHERE type='table' AND name=?",
        (table,)
    ).fetchone()
    if not row or not row[0]:
        return False
    sql = row[0].upper()
    return "UNIQUE" in sql and needle.upper() in sql


def _migrate_entries_unique(conn: sqlite3.Connection) -> None:
    if _has_unique(conn, "entries", "race_id, horse_id"):
        return
    logger.info("Migrating entries table: adding UNIQUE(race_id, horse_id) and deduping")

    conn.execute("""
        DELETE FROM entries WHERE id NOT IN (
            SELECT MIN(id) FROM entries GROUP BY race_id, horse_id
        )
    """)

    conn.executescript("""
        CREATE TABLE entries_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            race_id INTEGER NOT NULL,
            horse_id INTEGER NOT NULL,
            jockey_id INTEGER,
            trainer_id INTEGER,
            post_position INTEGER,
            morning_line_odds REAL,
            weight_lbs REAL,
            medication TEXT,
            equipment_changes TEXT,
            scratched INTEGER DEFAULT 0,
            scratch_reason TEXT,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (race_id) REFERENCES races(id),
            FOREIGN KEY (horse_id) REFERENCES horses(id),
            FOREIGN KEY (jockey_id) REFERENCES jockeys(id),
            FOREIGN KEY (trainer_id) REFERENCES trainers(id),
            UNIQUE(race_id, horse_id)
        );
        INSERT INTO entries_new SELECT * FROM entries;
        DROP TABLE entries;
        ALTER TABLE entries_new RENAME TO entries;
        CREATE INDEX IF NOT EXISTS idx_entries_race ON entries(race_id);
        CREATE INDEX IF NOT EXISTS idx_entries_horse ON entries(horse_id);
    """)
    conn.commit()


def _migrate_results_unique(conn: sqlite3.Connection) -> None:
    if _has_unique(conn, "results", "entry_id"):
        return
    logger.info("Migrating results table: adding UNIQUE(entry_id) and deduping")

    conn.execute("""
        DELETE FROM results WHERE id NOT IN (
            SELECT MIN(id) FROM results GROUP BY entry_id
        )
    """)

    conn.executescript("""
        CREATE TABLE results_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            entry_id INTEGER NOT NULL,
            finish_position INTEGER NOT NULL,
            beaten_lengths REAL,
            final_odds REAL,
            speed_figure INTEGER,
            final_time_seconds REAL,
            fractional_times TEXT,
            running_position TEXT,
            comment TEXT,
            payout_win REAL,
            payout_place REAL,
            payout_show REAL,
            created_at TEXT NOT NULL DEFAULT (datetime('now')),
            FOREIGN KEY (entry_id) REFERENCES entries(id),
            UNIQUE(entry_id)
        );
        INSERT INTO results_new SELECT * FROM results;
        DROP TABLE results;
        ALTER TABLE results_new RENAME TO results;
        CREATE INDEX IF NOT EXISTS idx_results_entry ON results(entry_id);
    """)
    conn.commit()


def _migrate_past_performances_unique(conn: sqlite3.Connection) -> None:
    if _has_unique(conn, "past_performances", "horse_id, race_date, track_code"):
        return
    logger.info("Migrating past_performances: adding UNIQUE and deduping")

    conn.execute("""
        DELETE FROM past_performances WHERE id NOT IN (
            SELECT MIN(id) FROM past_performances
            GROUP BY horse_id, race_date, track_code, distance_furlongs, surface
        )
    """)

    conn.executescript("""
        CREATE TABLE past_performances_new (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            horse_id INTEGER NOT NULL,
            race_date TEXT NOT NULL,
            track_code TEXT NOT NULL,
            surface TEXT,
            distance_furlongs REAL,
            track_condition TEXT,
            class_level INTEGER,
            finish_position INTEGER,
            field_size INTEGER,
            speed_figure INTEGER,
            beaten_lengths REAL,
            final_time_seconds REAL,
            weight_lbs REAL,
            jockey_name TEXT,
            trainer_name TEXT,
            days_since_prev_race INTEGER,
            comment TEXT,
            FOREIGN KEY (horse_id) REFERENCES horses(id),
            UNIQUE(horse_id, race_date, track_code, distance_furlongs, surface)
        );
        INSERT INTO past_performances_new SELECT * FROM past_performances;
        DROP TABLE past_performances;
        ALTER TABLE past_performances_new RENAME TO past_performances;
        CREATE INDEX IF NOT EXISTS idx_pp_horse_date ON past_performances(horse_id, race_date DESC);
        CREATE INDEX IF NOT EXISTS idx_pp_track ON past_performances(track_code);
    """)
    conn.commit()
