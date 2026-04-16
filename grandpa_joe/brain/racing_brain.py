"""
Racing Brain - SQLite persistent memory for GRANDPA_JOE.
Follows ALFRED's AlfredBrain pattern: SQLite + WAL + TF-IDF search + caching.
No patent-pending technology (CORTEX/ULTRATHUNK/Guardian).
"""

import json
import logging
import sqlite3
from collections import defaultdict
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

from grandpa_joe.brain.migrations import run_migrations
from grandpa_joe.brain.schema import SCHEMA_SQL
from grandpa_joe.path_manager import PathManager

logger = logging.getLogger(__name__)


class RacingBrain:
    """
    Persistent racing memory with SQLite backend.
    12 tables covering tracks, horses, jockeys, trainers, races, entries,
    results, past performances, predictions, bets, patterns, and session logs.
    """

    def __init__(self, data_dir: Optional[str] = None):
        self.data_dir = Path(data_dir) if data_dir else PathManager.DATA_DIR
        self.db_path = self.data_dir / "racing_brain.db"
        self.data_dir.mkdir(parents=True, exist_ok=True)

        # In-memory caches
        self.horse_cache: Dict[str, int] = {}  # name -> id
        self.jockey_cache: Dict[str, int] = {}
        self.trainer_cache: Dict[str, int] = {}
        self.track_cache: Dict[str, int] = {}  # code -> id
        self.pattern_cache: Dict[str, List] = defaultdict(list)

        # TF-IDF for search (lazy init)
        self._tfidf_vectorizer = None
        self._tfidf_matrix = None
        self._tfidf_docs = []

        self._init_database()
        self._load_caches()
        logger.info(f"Racing Brain initialized at {self.db_path}")

    def _connect(self) -> sqlite3.Connection:
        """Create WAL-mode SQLite connection."""
        conn = sqlite3.connect(str(self.db_path), timeout=10)
        conn.row_factory = sqlite3.Row
        conn.execute("PRAGMA journal_mode=WAL")
        conn.execute("PRAGMA foreign_keys=ON")
        return conn

    def _init_database(self):
        """Create all tables from schema, then run pending migrations."""
        conn = self._connect()
        try:
            conn.executescript(SCHEMA_SQL)
            conn.commit()
            run_migrations(conn)
        finally:
            conn.close()

    def _load_caches(self):
        """Load lookup caches from database."""
        conn = self._connect()
        try:
            for row in conn.execute("SELECT id, code FROM tracks"):
                self.track_cache[row["code"]] = row["id"]
            for row in conn.execute("SELECT id, name FROM horses"):
                self.horse_cache[row["name"]] = row["id"]
            for row in conn.execute("SELECT id, name FROM jockeys"):
                self.jockey_cache[row["name"]] = row["id"]
            for row in conn.execute("SELECT id, name FROM trainers"):
                self.trainer_cache[row["name"]] = row["id"]
        finally:
            conn.close()

    # ========================================================================
    # TRACK operations
    # ========================================================================

    def get_or_create_track(self, code: str, name: str = "", **kwargs) -> int:
        """Get track ID by code, creating if needed."""
        if code in self.track_cache:
            return self.track_cache[code]
        conn = self._connect()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO tracks (code, name, location, surface_types, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (code, name or code, kwargs.get("location", ""),
                 kwargs.get("surface_types", ""), now, now)
            )
            conn.commit()
            row = conn.execute("SELECT id FROM tracks WHERE code = ?", (code,)).fetchone()
            self.track_cache[code] = row["id"]
            return row["id"]
        finally:
            conn.close()

    # ========================================================================
    # HORSE operations
    # ========================================================================

    def get_or_create_horse(self, name: str, **kwargs) -> int:
        """Get horse ID by name, creating if needed."""
        if name in self.horse_cache:
            return self.horse_cache[name]
        conn = self._connect()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO horses (name, registration_id, sire, dam, dam_sire, "
                "birth_year, sex, color, breeder, owner, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (name, kwargs.get("registration_id"), kwargs.get("sire"),
                 kwargs.get("dam"), kwargs.get("dam_sire"),
                 kwargs.get("birth_year"), kwargs.get("sex"),
                 kwargs.get("color"), kwargs.get("breeder"),
                 kwargs.get("owner"), now, now)
            )
            conn.commit()
            row = conn.execute("SELECT id FROM horses WHERE name = ?", (name,)).fetchone()
            self.horse_cache[name] = row["id"]
            return row["id"]
        finally:
            conn.close()

    # ========================================================================
    # JOCKEY operations
    # ========================================================================

    def get_or_create_jockey(self, name: str, **kwargs) -> int:
        """Get jockey ID by name, creating if needed."""
        if name in self.jockey_cache:
            return self.jockey_cache[name]
        conn = self._connect()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO jockeys (name, license_id, agent, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, kwargs.get("license_id"), kwargs.get("agent"), now, now)
            )
            conn.commit()
            row = conn.execute("SELECT id FROM jockeys WHERE name = ?", (name,)).fetchone()
            self.jockey_cache[name] = row["id"]
            return row["id"]
        finally:
            conn.close()

    # ========================================================================
    # TRAINER operations
    # ========================================================================

    def get_or_create_trainer(self, name: str, **kwargs) -> int:
        """Get trainer ID by name, creating if needed."""
        if name in self.trainer_cache:
            return self.trainer_cache[name]
        conn = self._connect()
        try:
            now = datetime.now().isoformat()
            conn.execute(
                "INSERT OR IGNORE INTO trainers (name, license_id, specialty, created_at, updated_at) "
                "VALUES (?, ?, ?, ?, ?)",
                (name, kwargs.get("license_id"), kwargs.get("specialty"), now, now)
            )
            conn.commit()
            row = conn.execute("SELECT id FROM trainers WHERE name = ?", (name,)).fetchone()
            self.trainer_cache[name] = row["id"]
            return row["id"]
        finally:
            conn.close()

    # ========================================================================
    # RACE operations
    # ========================================================================

    def store_race(self, track_code: str, race_date: str, race_number: int,
                   **kwargs) -> int:
        """Store a race, returning its ID."""
        track_id = self.get_or_create_track(track_code)
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO races "
                "(track_id, race_date, race_number, race_name, race_type, grade, "
                "surface, distance_furlongs, purse, class_level, conditions, "
                "weather, track_condition, off_time) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (track_id, race_date, race_number,
                 kwargs.get("race_name"), kwargs.get("race_type", "allowance"),
                 kwargs.get("grade"), kwargs.get("surface", "dirt"),
                 kwargs.get("distance_furlongs", 6.0), kwargs.get("purse"),
                 kwargs.get("class_level"), kwargs.get("conditions"),
                 kwargs.get("weather"), kwargs.get("track_condition", "fast"),
                 kwargs.get("off_time"))
            )
            conn.commit()
            row = conn.execute(
                "SELECT id FROM races WHERE track_id = ? AND race_date = ? AND race_number = ?",
                (track_id, race_date, race_number)
            ).fetchone()
            return row["id"]
        finally:
            conn.close()

    def get_race(self, race_id: int) -> Optional[Dict]:
        """Get race details with entries."""
        conn = self._connect()
        try:
            race = conn.execute(
                "SELECT r.*, t.code as track_code, t.name as track_name "
                "FROM races r JOIN tracks t ON r.track_id = t.id "
                "WHERE r.id = ?", (race_id,)
            ).fetchone()
            if not race:
                return None
            result = dict(race)
            entries = conn.execute(
                "SELECT e.*, h.name as horse_name, j.name as jockey_name, "
                "tr.name as trainer_name "
                "FROM entries e "
                "JOIN horses h ON e.horse_id = h.id "
                "LEFT JOIN jockeys j ON e.jockey_id = j.id "
                "LEFT JOIN trainers tr ON e.trainer_id = tr.id "
                "WHERE e.race_id = ? AND e.scratched = 0 "
                "ORDER BY e.post_position",
                (race_id,)
            ).fetchall()
            result["entries"] = [dict(e) for e in entries]
            return result
        finally:
            conn.close()

    # ========================================================================
    # ENTRY operations
    # ========================================================================

    def store_entry(self, race_id: int, horse_name: str, **kwargs) -> int:
        """Store a race entry."""
        horse_id = self.get_or_create_horse(horse_name)
        jockey_id = None
        trainer_id = None
        if kwargs.get("jockey_name"):
            jockey_id = self.get_or_create_jockey(kwargs["jockey_name"])
        if kwargs.get("trainer_name"):
            trainer_id = self.get_or_create_trainer(kwargs["trainer_name"])

        conn = self._connect()
        try:
            cur = conn.execute(
                "INSERT OR IGNORE INTO entries "
                "(race_id, horse_id, jockey_id, trainer_id, post_position, "
                "morning_line_odds, weight_lbs, medication, equipment_changes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (race_id, horse_id, jockey_id, trainer_id,
                 kwargs.get("post_position"), kwargs.get("morning_line_odds"),
                 kwargs.get("weight_lbs"), kwargs.get("medication"),
                 kwargs.get("equipment_changes"))
            )
            if cur.rowcount > 0:
                entry_id = cur.lastrowid
            else:
                row = conn.execute(
                    "SELECT id FROM entries WHERE race_id = ? AND horse_id = ?",
                    (race_id, horse_id)
                ).fetchone()
                entry_id = row["id"] if row else None
            conn.commit()
            return entry_id
        finally:
            conn.close()

    # ========================================================================
    # RESULT operations
    # ========================================================================

    def store_result(self, entry_id: int, finish_position: int, **kwargs):
        """Store a race result."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO results "
                "(entry_id, finish_position, beaten_lengths, final_odds, speed_figure, "
                "final_time_seconds, fractional_times, running_position, comment, "
                "payout_win, payout_place, payout_show) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (entry_id, finish_position, kwargs.get("beaten_lengths"),
                 kwargs.get("final_odds"), kwargs.get("speed_figure"),
                 kwargs.get("final_time_seconds"),
                 json.dumps(kwargs.get("fractional_times", [])),
                 json.dumps(kwargs.get("running_position", {})),
                 kwargs.get("comment"),
                 kwargs.get("payout_win"), kwargs.get("payout_place"),
                 kwargs.get("payout_show"))
            )
            conn.commit()
        finally:
            conn.close()

    # ========================================================================
    # PAST PERFORMANCES
    # ========================================================================

    def store_past_performance(self, horse_name: str, race_date: str,
                               track_code: str, **kwargs):
        """Store a denormalized past performance record."""
        horse_id = self.get_or_create_horse(horse_name)
        conn = self._connect()
        try:
            conn.execute(
                "INSERT OR IGNORE INTO past_performances "
                "(horse_id, race_date, track_code, surface, distance_furlongs, "
                "track_condition, class_level, finish_position, field_size, "
                "speed_figure, beaten_lengths, final_time_seconds, weight_lbs, "
                "jockey_name, trainer_name, days_since_prev_race, comment) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (horse_id, race_date, track_code,
                 kwargs.get("surface"), kwargs.get("distance_furlongs"),
                 kwargs.get("track_condition"), kwargs.get("class_level"),
                 kwargs.get("finish_position"), kwargs.get("field_size"),
                 kwargs.get("speed_figure"), kwargs.get("beaten_lengths"),
                 kwargs.get("final_time_seconds"), kwargs.get("weight_lbs"),
                 kwargs.get("jockey_name"), kwargs.get("trainer_name"),
                 kwargs.get("days_since_prev_race"), kwargs.get("comment"))
            )
            conn.commit()
        finally:
            conn.close()

    def get_horse_pps(self, horse_id: int, limit: int = 10) -> List[Dict]:
        """Get past performances for a horse, most recent first."""
        conn = self._connect()
        try:
            rows = conn.execute(
                "SELECT * FROM past_performances WHERE horse_id = ? "
                "ORDER BY race_date DESC LIMIT ?",
                (horse_id, limit)
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ========================================================================
    # PREDICTIONS
    # ========================================================================

    def store_prediction(self, race_id: int, entry_id: int, predicted_rank: int,
                         **kwargs):
        """Store a model prediction for an entry."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO predictions "
                "(race_id, entry_id, predicted_rank, win_probability, "
                "place_probability, show_probability, confidence, "
                "model_version, features_snapshot) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (race_id, entry_id, predicted_rank,
                 kwargs.get("win_probability"), kwargs.get("place_probability"),
                 kwargs.get("show_probability"), kwargs.get("confidence"),
                 kwargs.get("model_version"),
                 json.dumps(kwargs.get("features_snapshot", {})))
            )
            conn.commit()
        finally:
            conn.close()

    # ========================================================================
    # BETS
    # ========================================================================

    def store_bet(self, race_id: int, bet_type: str, selections: list,
                  amount: float, **kwargs) -> int:
        """Record a placed bet. Returns bet ID."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO bets "
                "(user_id, race_id, bet_type, selections, amount, odds_at_bet, "
                "kelly_fraction, confidence_at_bet, notes) "
                "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (kwargs.get("user_id", "default"), race_id, bet_type,
                 json.dumps(selections), amount,
                 kwargs.get("odds_at_bet"), kwargs.get("kelly_fraction"),
                 kwargs.get("confidence_at_bet"), kwargs.get("notes"))
            )
            conn.commit()
            return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        finally:
            conn.close()

    def resolve_bet(self, bet_id: int, result: str, payout: float = 0):
        """Mark a bet as won/lost/scratched."""
        conn = self._connect()
        try:
            conn.execute(
                "UPDATE bets SET result = ?, payout = ?, resolved_at = datetime('now') "
                "WHERE id = ?",
                (result, payout, bet_id)
            )
            conn.commit()
        finally:
            conn.close()

    # ========================================================================
    # HANDICAPPING PATTERNS
    # ========================================================================

    def store_pattern(self, pattern_type: str, pattern_key: str,
                      pattern_data: dict, confidence: float = 0.5,
                      sample_size: int = 0):
        """Store or update a handicapping pattern."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO handicapping_patterns "
                "(pattern_type, pattern_key, pattern_data, confidence, sample_size, last_updated) "
                "VALUES (?, ?, ?, ?, ?, datetime('now')) "
                "ON CONFLICT(pattern_type, pattern_key) DO UPDATE SET "
                "pattern_data = excluded.pattern_data, confidence = excluded.confidence, "
                "sample_size = excluded.sample_size, last_updated = datetime('now')",
                (pattern_type, pattern_key, json.dumps(pattern_data),
                 confidence, sample_size)
            )
            conn.commit()
        finally:
            conn.close()

    def get_patterns(self, pattern_type: str = None,
                     min_confidence: float = 0) -> List[Dict]:
        """Get handicapping patterns, optionally filtered."""
        conn = self._connect()
        try:
            if pattern_type:
                rows = conn.execute(
                    "SELECT * FROM handicapping_patterns "
                    "WHERE pattern_type = ? AND confidence >= ? "
                    "ORDER BY confidence DESC",
                    (pattern_type, min_confidence)
                ).fetchall()
            else:
                rows = conn.execute(
                    "SELECT * FROM handicapping_patterns "
                    "WHERE confidence >= ? ORDER BY confidence DESC",
                    (min_confidence,)
                ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # ========================================================================
    # GAMBLING SESSION LOG
    # ========================================================================

    def start_session(self, user_id: str = "default") -> int:
        """Start a gambling session. Returns session ID."""
        conn = self._connect()
        try:
            conn.execute(
                "INSERT INTO gambling_session_log (user_id) VALUES (?)",
                (user_id,)
            )
            conn.commit()
            return conn.execute("SELECT last_insert_rowid()").fetchone()[0]
        finally:
            conn.close()

    def update_session(self, session_id: int, **kwargs):
        """Update session stats."""
        conn = self._connect()
        try:
            sets = []
            vals = []
            for key in ["total_wagered", "total_returned", "num_bets",
                         "loss_streak", "cooldown_triggered"]:
                if key in kwargs:
                    sets.append(f"{key} = ?")
                    vals.append(kwargs[key])
            if "end" in kwargs and kwargs["end"]:
                sets.append("session_end = datetime('now')")
            if sets:
                vals.append(session_id)
                conn.execute(
                    f"UPDATE gambling_session_log SET {', '.join(sets)} WHERE id = ?",
                    vals
                )
                conn.commit()
        finally:
            conn.close()

    def get_user_session_stats(self, user_id: str = "default",
                               days: int = 30) -> Dict:
        """Get aggregate session stats for a user."""
        conn = self._connect()
        try:
            row = conn.execute(
                "SELECT COUNT(*) as sessions, "
                "COALESCE(SUM(total_wagered), 0) as total_wagered, "
                "COALESCE(SUM(total_returned), 0) as total_returned, "
                "COALESCE(SUM(num_bets), 0) as total_bets "
                "FROM gambling_session_log "
                "WHERE user_id = ? AND session_start >= datetime('now', ?)",
                (user_id, f"-{days} days")
            ).fetchone()
            total_wagered = row["total_wagered"]
            total_returned = row["total_returned"]
            return {
                "sessions": row["sessions"],
                "total_wagered": total_wagered,
                "total_returned": total_returned,
                "net_pnl": total_returned - total_wagered,
                "total_bets": row["total_bets"],
                "roi": ((total_returned - total_wagered) / total_wagered * 100)
                       if total_wagered > 0 else 0,
            }
        finally:
            conn.close()

    # ========================================================================
    # SEARCH (TF-IDF)
    # ========================================================================

    def search(self, query: str, limit: int = 10) -> List[Dict]:
        """TF-IDF search across horse names, patterns, and comments."""
        try:
            from sklearn.feature_extraction.text import TfidfVectorizer
            from sklearn.metrics.pairwise import cosine_similarity
        except ImportError:
            logger.warning("scikit-learn not available for TF-IDF search")
            return self._fallback_search(query, limit)

        if not self._tfidf_matrix or not self._tfidf_docs:
            self._build_tfidf_index()

        if not self._tfidf_docs:
            return []

        query_vec = self._tfidf_vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, self._tfidf_matrix).flatten()
        top_indices = similarities.argsort()[-limit:][::-1]

        results = []
        for idx in top_indices:
            if similarities[idx] > 0.01:
                results.append({
                    "text": self._tfidf_docs[idx],
                    "score": float(similarities[idx])
                })
        return results

    def _build_tfidf_index(self):
        """Build TF-IDF index from horse names and pattern data."""
        from sklearn.feature_extraction.text import TfidfVectorizer

        docs = []
        conn = self._connect()
        try:
            for row in conn.execute("SELECT name, sire, dam FROM horses"):
                parts = [row["name"]]
                if row["sire"]:
                    parts.append(f"sire:{row['sire']}")
                if row["dam"]:
                    parts.append(f"dam:{row['dam']}")
                docs.append(" ".join(parts))

            for row in conn.execute("SELECT pattern_type, pattern_key, pattern_data FROM handicapping_patterns"):
                docs.append(f"{row['pattern_type']} {row['pattern_key']}")

            for row in conn.execute(
                "SELECT comment FROM past_performances WHERE comment IS NOT NULL "
                "ORDER BY race_date DESC LIMIT 1000"
            ):
                if row["comment"]:
                    docs.append(row["comment"])
        finally:
            conn.close()

        if docs:
            self._tfidf_vectorizer = TfidfVectorizer(max_features=5000)
            self._tfidf_matrix = self._tfidf_vectorizer.fit_transform(docs)
            self._tfidf_docs = docs

    def _fallback_search(self, query: str, limit: int) -> List[Dict]:
        """Simple LIKE search when scikit-learn not available."""
        conn = self._connect()
        try:
            results = []
            pattern = f"%{query}%"
            for row in conn.execute(
                "SELECT name FROM horses WHERE name LIKE ? LIMIT ?",
                (pattern, limit)
            ):
                results.append({"text": row["name"], "score": 1.0})
            return results
        finally:
            conn.close()

    # ========================================================================
    # STATS & UTILITIES
    # ========================================================================

    def get_memory_stats(self) -> Dict:
        """Get counts of all tables."""
        conn = self._connect()
        try:
            stats = {}
            tables = [
                "tracks", "horses", "jockeys", "trainers", "races",
                "entries", "results", "past_performances", "predictions",
                "bets", "handicapping_patterns", "gambling_session_log"
            ]
            for table in tables:
                row = conn.execute(f"SELECT COUNT(*) as cnt FROM {table}").fetchone()
                stats[table] = row["cnt"]

            # Bet P&L
            pnl_row = conn.execute(
                "SELECT COALESCE(SUM(payout), 0) - COALESCE(SUM(amount), 0) as net "
                "FROM bets WHERE result != 'pending'"
            ).fetchone()
            stats["net_pnl"] = pnl_row["net"]

            # Win rate
            resolved = conn.execute(
                "SELECT COUNT(*) as total, "
                "SUM(CASE WHEN result = 'won' THEN 1 ELSE 0 END) as wins "
                "FROM bets WHERE result != 'pending'"
            ).fetchone()
            stats["bet_win_rate"] = (
                resolved["wins"] / resolved["total"] * 100
                if resolved["total"] > 0 else 0
            )

            return stats
        finally:
            conn.close()

    def get_track_bias(self, track_code: str, surface: str = None,
                       days: int = 365) -> Dict:
        """Analyze track bias from historical results."""
        conn = self._connect()
        try:
            query = """
                SELECT e.post_position, COUNT(*) as starts,
                       SUM(CASE WHEN r.finish_position = 1 THEN 1 ELSE 0 END) as wins
                FROM results r
                JOIN entries e ON r.entry_id = e.id
                JOIN races ra ON e.race_id = ra.id
                JOIN tracks t ON ra.track_id = t.id
                WHERE t.code = ? AND ra.race_date >= date('now', ?)
            """
            params = [track_code, f"-{days} days"]
            if surface:
                query += " AND ra.surface = ?"
                params.append(surface)
            query += " GROUP BY e.post_position ORDER BY e.post_position"

            rows = conn.execute(query, params).fetchall()
            bias = {}
            for row in rows:
                pp = row["post_position"]
                starts = row["starts"]
                wins = row["wins"]
                bias[pp] = {
                    "starts": starts,
                    "wins": wins,
                    "win_pct": (wins / starts * 100) if starts > 0 else 0
                }
            return {"track": track_code, "surface": surface, "days": days, "bias": bias}
        finally:
            conn.close()

    def export_to_json(self, filepath: Optional[str] = None) -> str:
        """Export brain data to JSON for backup/sync."""
        path = filepath or str(PathManager.BACKUPS_DIR / f"brain_export_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json")
        Path(path).parent.mkdir(parents=True, exist_ok=True)

        conn = self._connect()
        try:
            data = {}
            tables = ["tracks", "horses", "jockeys", "trainers", "handicapping_patterns"]
            for table in tables:
                rows = conn.execute(f"SELECT * FROM {table}").fetchall()
                data[table] = [dict(r) for r in rows]
            with open(path, "w") as f:
                json.dump(data, f, indent=2, default=str)
            return path
        finally:
            conn.close()
