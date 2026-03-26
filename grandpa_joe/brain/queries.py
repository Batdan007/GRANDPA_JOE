"""
Canned SQL queries for feature engineering and analytics.
Used by the ML pipeline to extract features from the racing brain.
"""

from typing import Dict, List, Optional, Tuple


def get_horse_speed_figures(conn, horse_id: int, limit: int = 10) -> List[int]:
    """Get last N speed figures for a horse."""
    rows = conn.execute(
        "SELECT speed_figure FROM past_performances "
        "WHERE horse_id = ? AND speed_figure IS NOT NULL "
        "ORDER BY race_date DESC LIMIT ?",
        (horse_id, limit)
    ).fetchall()
    return [r["speed_figure"] for r in rows]


def get_horse_distance_record(conn, horse_id: int,
                               distance: float, tolerance: float = 0.5) -> Dict:
    """Get win/place/show record at a distance range."""
    rows = conn.execute(
        "SELECT finish_position, COUNT(*) as cnt "
        "FROM past_performances "
        "WHERE horse_id = ? "
        "AND distance_furlongs BETWEEN ? AND ? "
        "GROUP BY finish_position",
        (horse_id, distance - tolerance, distance + tolerance)
    ).fetchall()
    total = sum(r["cnt"] for r in rows)
    wins = sum(r["cnt"] for r in rows if r["finish_position"] == 1)
    top3 = sum(r["cnt"] for r in rows if r["finish_position"] <= 3)
    return {
        "starts": total,
        "wins": wins,
        "top3": top3,
        "win_pct": wins / total if total > 0 else 0,
        "itm_pct": top3 / total if total > 0 else 0,
    }


def get_horse_surface_record(conn, horse_id: int, surface: str) -> Dict:
    """Get record on a specific surface."""
    rows = conn.execute(
        "SELECT finish_position FROM past_performances "
        "WHERE horse_id = ? AND surface = ?",
        (horse_id, surface)
    ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if r["finish_position"] == 1)
    top3 = sum(1 for r in rows if r["finish_position"] <= 3)
    return {
        "starts": total,
        "wins": wins,
        "top3": top3,
        "win_pct": wins / total if total > 0 else 0,
    }


def get_horse_condition_record(conn, horse_id: int,
                                condition: str) -> Dict:
    """Get record on a specific track condition."""
    off_track = ("sloppy", "muddy", "yielding", "soft", "heavy")
    if condition in off_track:
        conditions = off_track
        placeholder = ",".join("?" for _ in conditions)
        rows = conn.execute(
            f"SELECT finish_position FROM past_performances "
            f"WHERE horse_id = ? AND track_condition IN ({placeholder})",
            (horse_id, *conditions)
        ).fetchall()
    else:
        rows = conn.execute(
            "SELECT finish_position FROM past_performances "
            "WHERE horse_id = ? AND track_condition = ?",
            (horse_id, condition)
        ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if r["finish_position"] == 1)
    return {
        "starts": total,
        "wins": wins,
        "win_pct": wins / total if total > 0 else 0,
    }


def get_jockey_stats_at_track(conn, jockey_id: int,
                               track_code: str, days: int = 365) -> Dict:
    """Get jockey win stats at a specific track."""
    rows = conn.execute(
        "SELECT r.finish_position "
        "FROM results r "
        "JOIN entries e ON r.entry_id = e.id "
        "JOIN races ra ON e.race_id = ra.id "
        "JOIN tracks t ON ra.track_id = t.id "
        "WHERE e.jockey_id = ? AND t.code = ? "
        "AND ra.race_date >= date('now', ?)",
        (jockey_id, track_code, f"-{days} days")
    ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if r["finish_position"] == 1)
    return {"starts": total, "wins": wins,
            "win_pct": wins / total if total > 0 else 0}


def get_trainer_stats_at_track(conn, trainer_id: int,
                                track_code: str, days: int = 365) -> Dict:
    """Get trainer win stats at a specific track."""
    rows = conn.execute(
        "SELECT r.finish_position "
        "FROM results r "
        "JOIN entries e ON r.entry_id = e.id "
        "JOIN races ra ON e.race_id = ra.id "
        "JOIN tracks t ON ra.track_id = t.id "
        "WHERE e.trainer_id = ? AND t.code = ? "
        "AND ra.race_date >= date('now', ?)",
        (trainer_id, track_code, f"-{days} days")
    ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if r["finish_position"] == 1)
    return {"starts": total, "wins": wins,
            "win_pct": wins / total if total > 0 else 0}


def get_jockey_trainer_combo(conn, jockey_id: int,
                              trainer_id: int, days: int = 365) -> Dict:
    """Get jockey/trainer combination stats."""
    rows = conn.execute(
        "SELECT r.finish_position "
        "FROM results r "
        "JOIN entries e ON r.entry_id = e.id "
        "JOIN races ra ON e.race_id = ra.id "
        "WHERE e.jockey_id = ? AND e.trainer_id = ? "
        "AND ra.race_date >= date('now', ?)",
        (jockey_id, trainer_id, f"-{days} days")
    ).fetchall()
    total = len(rows)
    wins = sum(1 for r in rows if r["finish_position"] == 1)
    return {"starts": total, "wins": wins,
            "win_pct": wins / total if total > 0 else 0}


def get_post_position_stats(conn, track_code: str, surface: str,
                             distance_range: Tuple[float, float],
                             days: int = 365) -> Dict[int, Dict]:
    """Get win rates by post position at track/surface/distance."""
    rows = conn.execute(
        "SELECT e.post_position, r.finish_position "
        "FROM results r "
        "JOIN entries e ON r.entry_id = e.id "
        "JOIN races ra ON e.race_id = ra.id "
        "JOIN tracks t ON ra.track_id = t.id "
        "WHERE t.code = ? AND ra.surface = ? "
        "AND ra.distance_furlongs BETWEEN ? AND ? "
        "AND ra.race_date >= date('now', ?) "
        "AND e.post_position IS NOT NULL",
        (track_code, surface, distance_range[0], distance_range[1],
         f"-{days} days")
    ).fetchall()

    pp_stats = {}
    for row in rows:
        pp = row["post_position"]
        if pp not in pp_stats:
            pp_stats[pp] = {"starts": 0, "wins": 0}
        pp_stats[pp]["starts"] += 1
        if row["finish_position"] == 1:
            pp_stats[pp]["wins"] += 1

    for pp in pp_stats:
        s = pp_stats[pp]
        s["win_pct"] = s["wins"] / s["starts"] if s["starts"] > 0 else 0

    return pp_stats


def get_horse_class_history(conn, horse_id: int, limit: int = 5) -> List[int]:
    """Get recent class levels for a horse (most recent first)."""
    rows = conn.execute(
        "SELECT class_level FROM past_performances "
        "WHERE horse_id = ? AND class_level IS NOT NULL "
        "ORDER BY race_date DESC LIMIT ?",
        (horse_id, limit)
    ).fetchall()
    return [r["class_level"] for r in rows]


def get_horse_pace_profile(conn, horse_id: int, limit: int = 5) -> Dict:
    """Get average running positions (early speed vs late speed)."""
    rows = conn.execute(
        "SELECT pp.finish_position, pp.comment "
        "FROM past_performances pp "
        "WHERE pp.horse_id = ? "
        "ORDER BY pp.race_date DESC LIMIT ?",
        (horse_id, limit)
    ).fetchall()

    if not rows:
        return {"early_speed": 0.5, "closer": 0.5, "sample": 0}

    # Approximate from finish positions and comments
    front_keywords = ["led", "set pace", "pressed", "stalked", "forwardly"]
    close_keywords = ["rallied", "came again", "closed", "late run", "finished well"]

    front_count = 0
    close_count = 0
    for r in rows:
        comment = (r["comment"] or "").lower()
        if any(k in comment for k in front_keywords):
            front_count += 1
        if any(k in comment for k in close_keywords):
            close_count += 1

    total = len(rows)
    return {
        "early_speed": front_count / total if total > 0 else 0.5,
        "closer": close_count / total if total > 0 else 0.5,
        "sample": total,
    }
