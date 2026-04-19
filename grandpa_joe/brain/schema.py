"""
Database schema for GRANDPA_JOE's Racing Brain.
12 tables for tracking horses, races, results, predictions, bets, and patterns.
"""

SCHEMA_SQL = """
-- ============================================================================
-- TRACKS
-- ============================================================================
CREATE TABLE IF NOT EXISTS tracks (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    code TEXT UNIQUE NOT NULL,
    name TEXT NOT NULL,
    location TEXT,
    surface_types TEXT,
    track_length_furlongs REAL,
    altitude_ft INTEGER,
    climate_zone TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);

-- ============================================================================
-- HORSES
-- ============================================================================
CREATE TABLE IF NOT EXISTS horses (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    registration_id TEXT UNIQUE,
    sire TEXT,
    dam TEXT,
    dam_sire TEXT,
    birth_year INTEGER,
    sex TEXT,
    color TEXT,
    breeder TEXT,
    owner TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_horses_name ON horses(name);

-- ============================================================================
-- JOCKEYS
-- ============================================================================
CREATE TABLE IF NOT EXISTS jockeys (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    license_id TEXT UNIQUE,
    agent TEXT,
    win_rate_current_meet REAL DEFAULT 0,
    win_rate_ytd REAL DEFAULT 0,
    win_rate_lifetime REAL DEFAULT 0,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_jockeys_name ON jockeys(name);

-- ============================================================================
-- TRAINERS
-- ============================================================================
CREATE TABLE IF NOT EXISTS trainers (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    name TEXT NOT NULL,
    license_id TEXT UNIQUE,
    win_rate_current_meet REAL DEFAULT 0,
    win_rate_ytd REAL DEFAULT 0,
    win_rate_lifetime REAL DEFAULT 0,
    specialty TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
CREATE INDEX IF NOT EXISTS idx_trainers_name ON trainers(name);

-- ============================================================================
-- RACES
-- ============================================================================
CREATE TABLE IF NOT EXISTS races (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    track_id INTEGER NOT NULL,
    race_date TEXT NOT NULL,
    race_number INTEGER NOT NULL,
    race_name TEXT,
    race_type TEXT NOT NULL,
    grade TEXT,
    surface TEXT NOT NULL,
    distance_furlongs REAL NOT NULL,
    purse INTEGER,
    class_level INTEGER,
    conditions TEXT,
    weather TEXT,
    track_condition TEXT,
    off_time TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (track_id) REFERENCES tracks(id),
    UNIQUE(track_id, race_date, race_number)
);
CREATE INDEX IF NOT EXISTS idx_races_date ON races(race_date);
CREATE INDEX IF NOT EXISTS idx_races_track_date ON races(track_id, race_date);

-- ============================================================================
-- ENTRIES (horses entered in a race)
-- ============================================================================
CREATE TABLE IF NOT EXISTS entries (
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
CREATE INDEX IF NOT EXISTS idx_entries_race ON entries(race_id);
CREATE INDEX IF NOT EXISTS idx_entries_horse ON entries(horse_id);

-- ============================================================================
-- RESULTS (actual race outcomes)
-- ============================================================================
CREATE TABLE IF NOT EXISTS results (
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
CREATE INDEX IF NOT EXISTS idx_results_entry ON results(entry_id);

-- ============================================================================
-- PAST PERFORMANCES (denormalized for fast ML feature reads)
-- ============================================================================
CREATE TABLE IF NOT EXISTS past_performances (
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
CREATE INDEX IF NOT EXISTS idx_pp_horse_date ON past_performances(horse_id, race_date DESC);
CREATE INDEX IF NOT EXISTS idx_pp_track ON past_performances(track_code);

-- ============================================================================
-- PREDICTIONS (model outputs)
-- ============================================================================
CREATE TABLE IF NOT EXISTS predictions (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    race_id INTEGER NOT NULL,
    entry_id INTEGER NOT NULL,
    predicted_rank INTEGER NOT NULL,
    win_probability REAL,
    place_probability REAL,
    show_probability REAL,
    confidence REAL,
    model_version TEXT,
    features_snapshot TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    FOREIGN KEY (race_id) REFERENCES races(id),
    FOREIGN KEY (entry_id) REFERENCES entries(id)
);
CREATE INDEX IF NOT EXISTS idx_predictions_race ON predictions(race_id);

-- ============================================================================
-- BETS (user wager tracking)
-- ============================================================================
CREATE TABLE IF NOT EXISTS bets (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT DEFAULT 'default',
    race_id INTEGER NOT NULL,
    bet_type TEXT NOT NULL,
    selections TEXT NOT NULL,
    amount REAL NOT NULL,
    odds_at_bet REAL,
    result TEXT DEFAULT 'pending',
    payout REAL,
    kelly_fraction REAL,
    confidence_at_bet REAL,
    placed_at TEXT NOT NULL DEFAULT (datetime('now')),
    resolved_at TEXT,
    notes TEXT,
    FOREIGN KEY (race_id) REFERENCES races(id)
);
CREATE INDEX IF NOT EXISTS idx_bets_user ON bets(user_id);
CREATE INDEX IF NOT EXISTS idx_bets_result ON bets(result);

-- ============================================================================
-- HANDICAPPING PATTERNS (learned insights)
-- ============================================================================
CREATE TABLE IF NOT EXISTS handicapping_patterns (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    pattern_type TEXT NOT NULL,
    pattern_key TEXT NOT NULL,
    pattern_data TEXT NOT NULL,
    confidence REAL DEFAULT 0.5,
    sample_size INTEGER DEFAULT 0,
    first_seen TEXT NOT NULL DEFAULT (datetime('now')),
    last_updated TEXT NOT NULL DEFAULT (datetime('now')),
    times_profitable INTEGER DEFAULT 0,
    total_times_applied INTEGER DEFAULT 0,
    UNIQUE(pattern_type, pattern_key)
);

-- ============================================================================
-- GAMBLING SESSION LOG (responsible gambling tracking)
-- ============================================================================
CREATE TABLE IF NOT EXISTS gambling_session_log (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id TEXT DEFAULT 'default',
    session_start TEXT NOT NULL DEFAULT (datetime('now')),
    session_end TEXT,
    total_wagered REAL DEFAULT 0,
    total_returned REAL DEFAULT 0,
    num_bets INTEGER DEFAULT 0,
    loss_streak INTEGER DEFAULT 0,
    mood_indicators TEXT,
    cooldown_triggered INTEGER DEFAULT 0,
    notes TEXT
);
CREATE INDEX IF NOT EXISTS idx_sessions_user ON gambling_session_log(user_id);

-- ============================================================================
-- PACE HISTORY (per-horse per-call splits from past performances)
-- ============================================================================
CREATE TABLE IF NOT EXISTS horse_pace_history (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    horse_id INTEGER NOT NULL,
    race_date TEXT NOT NULL,
    track_code TEXT NOT NULL,
    distance_furlongs REAL,
    surface TEXT,
    call_id TEXT NOT NULL,
    call_order INTEGER NOT NULL,
    position INTEGER,
    lengths_behind REAL,
    leader_time_sec REAL,
    horse_time_sec REAL,
    speed_figure INTEGER,
    FOREIGN KEY (horse_id) REFERENCES horses(id),
    UNIQUE(horse_id, race_date, track_code, call_id)
);
CREATE INDEX IF NOT EXISTS idx_pace_horse_date ON horse_pace_history(horse_id, race_date);
CREATE INDEX IF NOT EXISTS idx_pace_call ON horse_pace_history(call_id);
"""
