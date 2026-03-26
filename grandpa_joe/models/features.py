"""
Feature engineering pipeline for horse racing handicapping.
20 features computed per race entry for XGBoost prediction.
"""

import logging
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)

try:
    import numpy as np
    import pandas as pd
    PANDAS_AVAILABLE = True
except ImportError:
    PANDAS_AVAILABLE = False

from grandpa_joe.brain.queries import (
    get_horse_speed_figures,
    get_horse_distance_record,
    get_horse_surface_record,
    get_horse_condition_record,
    get_jockey_stats_at_track,
    get_trainer_stats_at_track,
    get_jockey_trainer_combo,
    get_post_position_stats,
    get_horse_class_history,
    get_horse_pace_profile,
)


FEATURE_NAMES = [
    "avg_speed_last3",         # 1. Mean of last 3 speed figures
    "best_speed_last5",        # 2. Best speed figure in last 5
    "speed_trend",             # 3. Slope of last 5 figures (improving?)
    "days_since_last",         # 4. Days since last race
    "distance_pref_win_pct",   # 5. Win% at today's distance
    "surface_pref_win_pct",    # 6. Win% on today's surface
    "class_change",            # 7. Class change from last race
    "jockey_win_pct_track",    # 8. Jockey win% at this track
    "trainer_win_pct_track",   # 9. Trainer win% at this track
    "jt_combo_win_pct",        # 10. Jockey/trainer combo win%
    "post_position_bias",      # 11. Historical win% for this post
    "weight_carried",          # 12. Weight in lbs
    "morning_line_odds",       # 13. Morning line (market signal)
    "avg_finish_last5",        # 14. Average finish position last 5
    "early_speed_rating",      # 15. Front-running tendency
    "late_speed_rating",       # 16. Closing tendency
    "condition_pref_win_pct",  # 17. Win% on today's track condition
    "field_size",              # 18. Number of entrants
    "layoff_category",         # 19. Rest pattern (0=short, 1=mid, 2=long, 3=layoff)
    "class_trend",             # 20. Class trajectory (dropping=positive edge)
]


def _compute_speed_trend(figures: List[int]) -> float:
    """Compute slope of speed figures (positive = improving)."""
    if len(figures) < 2:
        return 0.0
    # figures are most-recent-first, reverse for chronological
    chronological = list(reversed(figures[:5]))
    n = len(chronological)
    x = list(range(n))
    x_mean = sum(x) / n
    y_mean = sum(chronological) / n
    numerator = sum((x[i] - x_mean) * (chronological[i] - y_mean) for i in range(n))
    denominator = sum((x[i] - x_mean) ** 2 for i in range(n))
    if denominator == 0:
        return 0.0
    return numerator / denominator


def _categorize_layoff(days: Optional[int]) -> int:
    """Categorize layoff: 0=short(0-14), 1=mid(15-45), 2=long(46-180), 3=extended(180+)."""
    if days is None:
        return 2
    if days <= 14:
        return 0
    elif days <= 45:
        return 1
    elif days <= 180:
        return 2
    else:
        return 3


def build_features_for_entry(conn, entry_row: Dict, race_row: Dict) -> Dict:
    """
    Build all 20 features for a single entry.

    Args:
        conn: SQLite connection
        entry_row: dict with entry data (horse_id, jockey_id, trainer_id, etc.)
        race_row: dict with race data (track_code, surface, distance_furlongs, etc.)

    Returns:
        Dict of feature_name -> value
    """
    horse_id = entry_row["horse_id"]
    jockey_id = entry_row.get("jockey_id")
    trainer_id = entry_row.get("trainer_id")
    track_code = race_row.get("track_code", "UNK")
    surface = race_row.get("surface", "dirt")
    distance = race_row.get("distance_furlongs", 6.0)
    condition = race_row.get("track_condition", "fast")

    features = {}

    # 1-3: Speed figures
    speed_figs = get_horse_speed_figures(conn, horse_id, 5)
    features["avg_speed_last3"] = (
        sum(speed_figs[:3]) / len(speed_figs[:3]) if speed_figs else 0
    )
    features["best_speed_last5"] = max(speed_figs) if speed_figs else 0
    features["speed_trend"] = _compute_speed_trend(speed_figs)

    # 4: Days since last race
    last_pp = conn.execute(
        "SELECT days_since_prev_race FROM past_performances "
        "WHERE horse_id = ? ORDER BY race_date DESC LIMIT 1",
        (horse_id,)
    ).fetchone()
    days_since = last_pp["days_since_prev_race"] if last_pp and last_pp["days_since_prev_race"] else 30
    features["days_since_last"] = days_since

    # 5: Distance preference
    dist_record = get_horse_distance_record(conn, horse_id, distance)
    features["distance_pref_win_pct"] = dist_record["win_pct"]

    # 6: Surface preference
    surf_record = get_horse_surface_record(conn, horse_id, surface)
    features["surface_pref_win_pct"] = surf_record["win_pct"]

    # 7: Class change
    class_history = get_horse_class_history(conn, horse_id, 2)
    current_class = race_row.get("class_level") or 5
    if len(class_history) >= 1 and class_history[0]:
        features["class_change"] = class_history[0] - current_class
    else:
        features["class_change"] = 0

    # 8: Jockey win% at track
    if jockey_id:
        j_stats = get_jockey_stats_at_track(conn, jockey_id, track_code)
        features["jockey_win_pct_track"] = j_stats["win_pct"]
    else:
        features["jockey_win_pct_track"] = 0

    # 9: Trainer win% at track
    if trainer_id:
        t_stats = get_trainer_stats_at_track(conn, trainer_id, track_code)
        features["trainer_win_pct_track"] = t_stats["win_pct"]
    else:
        features["trainer_win_pct_track"] = 0

    # 10: Jockey/trainer combo
    if jockey_id and trainer_id:
        jt_stats = get_jockey_trainer_combo(conn, jockey_id, trainer_id)
        features["jt_combo_win_pct"] = jt_stats["win_pct"]
    else:
        features["jt_combo_win_pct"] = 0

    # 11: Post position bias
    pp = entry_row.get("post_position")
    if pp:
        pp_stats = get_post_position_stats(
            conn, track_code, surface,
            (distance - 0.5, distance + 0.5)
        )
        if pp in pp_stats:
            features["post_position_bias"] = pp_stats[pp]["win_pct"]
        else:
            features["post_position_bias"] = 0.1  # neutral default
    else:
        features["post_position_bias"] = 0.1

    # 12: Weight
    features["weight_carried"] = entry_row.get("weight_lbs") or 122

    # 13: Morning line odds
    ml = entry_row.get("morning_line_odds")
    features["morning_line_odds"] = ml if ml and ml > 0 else 10.0

    # 14: Average finish last 5
    recent_finishes = conn.execute(
        "SELECT finish_position FROM past_performances "
        "WHERE horse_id = ? AND finish_position IS NOT NULL "
        "ORDER BY race_date DESC LIMIT 5",
        (horse_id,)
    ).fetchall()
    if recent_finishes:
        features["avg_finish_last5"] = (
            sum(r["finish_position"] for r in recent_finishes) / len(recent_finishes)
        )
    else:
        features["avg_finish_last5"] = 5.0

    # 15-16: Pace profile
    pace = get_horse_pace_profile(conn, horse_id)
    features["early_speed_rating"] = pace["early_speed"]
    features["late_speed_rating"] = pace["closer"]

    # 17: Track condition preference
    cond_record = get_horse_condition_record(conn, horse_id, condition)
    features["condition_pref_win_pct"] = cond_record["win_pct"]

    # 18: Field size
    field_count = conn.execute(
        "SELECT COUNT(*) as cnt FROM entries WHERE race_id = ? AND scratched = 0",
        (entry_row.get("race_id") or race_row.get("id"),)
    ).fetchone()
    features["field_size"] = field_count["cnt"] if field_count else 8

    # 19: Layoff category
    features["layoff_category"] = _categorize_layoff(days_since)

    # 20: Class trend
    if len(class_history) >= 2:
        # Positive = dropping in class (easier competition)
        features["class_trend"] = sum(
            class_history[i] - class_history[i+1]
            for i in range(len(class_history) - 1)
        ) / (len(class_history) - 1)
    else:
        features["class_trend"] = 0

    return features


def build_features_for_race(brain, race_id: int):
    """
    Build feature matrix for all entries in a race.

    Returns:
        (DataFrame of features, list of entry dicts) or (None, []) if pandas unavailable
    """
    if not PANDAS_AVAILABLE:
        logger.warning("pandas not available — cannot build feature matrix")
        return None, []

    race = brain.get_race(race_id)
    if not race:
        return None, []

    conn = brain._connect()
    try:
        entries = race["entries"]
        if not entries:
            return None, []

        all_features = []
        for entry in entries:
            feats = build_features_for_entry(conn, entry, race)
            feats["entry_id"] = entry["id"]
            feats["horse_name"] = entry["horse_name"]
            all_features.append(feats)

        df = pd.DataFrame(all_features)
        return df, entries
    finally:
        conn.close()
