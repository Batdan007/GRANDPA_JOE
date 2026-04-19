"""Pace projection engine for GRANDPA_JOE.

Computes per-horse section averages from horse_pace_history, then
projects an entire race to simulate the order at each call point.

Call points: S (start), 1 (1/4), 2 (1/2), 3 (3/4), 5 (stretch), F (finish)
"""

import json
import logging
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

CALL_LABELS = {
    "S": "Start", "1": "Quarter", "2": "Half",
    "3": "Three-Quarter", "5": "Stretch", "F": "Finish",
}
CALL_ORDER = ["S", "1", "2", "3", "5", "F"]


def horse_pace_profile(brain, horse_id: int, before_date: str = "9999-99-99",
                       limit: int = 5) -> Dict:
    """Compute a horse's average pace profile from their last N PPs.

    Returns dict like:
      {
        "1": {"avg_position": 3.2, "avg_lengths_behind": 2.5, "avg_time": 23.8, "n": 5},
        "2": {"avg_position": 4.0, ...},
        ...
        "speed_figure_avg": 72.4,
        "speed_figure_best": 85,
        "speed_figure_recent": 78,
        "n_races": 5,
      }
    """
    conn = brain._connect()
    try:
        # Get distinct race dates for this horse (most recent N)
        dates = conn.execute(
            """
            SELECT DISTINCT race_date FROM horse_pace_history
            WHERE horse_id = ? AND race_date < ?
            ORDER BY race_date DESC LIMIT ?
            """,
            (horse_id, before_date, limit),
        ).fetchall()
        if not dates:
            return {}
        date_list = [d["race_date"] for d in dates]
        placeholders = ",".join("?" * len(date_list))

        rows = conn.execute(
            f"""
            SELECT call_id, position, lengths_behind, leader_time_sec,
                   horse_time_sec, speed_figure
            FROM horse_pace_history
            WHERE horse_id = ? AND race_date IN ({placeholders})
            ORDER BY race_date DESC, call_order
            """,
            [horse_id] + date_list,
        ).fetchall()

        # Aggregate per call
        by_call: Dict[str, dict] = {}
        speed_figs = []
        for row in rows:
            cid = row["call_id"]
            if cid not in by_call:
                by_call[cid] = {"positions": [], "lb": [], "times": []}
            if row["position"] and row["position"] > 0:
                by_call[cid]["positions"].append(row["position"])
            if row["lengths_behind"] is not None:
                by_call[cid]["lb"].append(row["lengths_behind"])
            if row["horse_time_sec"] and row["horse_time_sec"] > 0:
                by_call[cid]["times"].append(row["horse_time_sec"])
            if row["speed_figure"] and row["speed_figure"] > 0:
                speed_figs.append(row["speed_figure"])

        profile = {}
        for cid in CALL_ORDER:
            if cid not in by_call:
                continue
            d = by_call[cid]
            profile[cid] = {
                "avg_position": round(sum(d["positions"]) / len(d["positions"]), 2) if d["positions"] else None,
                "avg_lengths_behind": round(sum(d["lb"]) / len(d["lb"]), 2) if d["lb"] else None,
                "avg_time": round(sum(d["times"]) / len(d["times"]), 2) if d["times"] else None,
                "n": len(d["positions"]),
            }

        # Dedupe speed figures (one per race)
        unique_figs = list(dict.fromkeys(speed_figs))
        profile["speed_figure_avg"] = round(sum(unique_figs) / len(unique_figs), 1) if unique_figs else None
        profile["speed_figure_best"] = max(unique_figs) if unique_figs else None
        profile["speed_figure_recent"] = unique_figs[0] if unique_figs else None
        profile["n_races"] = len(date_list)
        return profile
    finally:
        conn.close()


def project_race(brain, race_id: int, limit: int = 5) -> List[Dict]:
    """Project pace for all entries in a race.

    Returns list of dicts sorted by projected finish, each containing:
      horse_name, projected positions at each call, speed figures, style tag.
    """
    conn = brain._connect()
    try:
        entries = conn.execute(
            """
            SELECT e.id as entry_id, e.horse_id, h.name as horse_name,
                   e.post_position, e.morning_line_odds
            FROM entries e
            JOIN horses h ON e.horse_id = h.id
            WHERE e.race_id = ? AND e.scratched = 0
            ORDER BY e.post_position
            """,
            (race_id,),
        ).fetchall()

        race = conn.execute(
            "SELECT race_date FROM races WHERE id = ?", (race_id,)
        ).fetchone()
        race_date = race["race_date"] if race else "9999-99-99"
    finally:
        conn.close()

    projections = []
    for entry in entries:
        profile = horse_pace_profile(brain, entry["horse_id"], before_date=race_date, limit=limit)
        style = _classify_style(profile)
        projections.append({
            "horse_name": entry["horse_name"],
            "horse_id": entry["horse_id"],
            "post_position": entry["post_position"],
            "morning_line": entry["morning_line_odds"],
            "profile": profile,
            "style": style,
        })

    # Rank at each call by avg_position (lowest = on the lead)
    for call_id in CALL_ORDER:
        horses_with_data = [
            p for p in projections
            if call_id in p["profile"] and p["profile"][call_id]["avg_position"] is not None
        ]
        horses_with_data.sort(key=lambda p: p["profile"][call_id]["avg_position"])
        for rank, p in enumerate(horses_with_data, 1):
            p.setdefault("projected_rank", {})[call_id] = rank

    # Sort by projected finish rank
    projections.sort(
        key=lambda p: p.get("projected_rank", {}).get("F", 99)
    )
    return projections


def _classify_style(profile: Dict) -> str:
    """Classify running style from pace profile."""
    if not profile:
        return "unknown"
    early = profile.get("1", {}).get("avg_position")
    finish = profile.get("F", {}).get("avg_position")
    if early is None or finish is None:
        return "unknown"
    diff = early - finish
    if early <= 2.5:
        return "front-runner" if diff >= -1 else "presser"
    elif early <= 4.5:
        return "stalker" if diff >= 0 else "presser"
    else:
        return "closer" if diff > 1 else "plodder"


def race_to_3d_data(brain, race_id: int, limit: int = 5) -> Dict:
    """Generate data structure for 3D visualization.

    Returns:
      {
        "horses": [
          {
            "name": "Horse Name",
            "style": "closer",
            "speed_figure_avg": 82,
            "calls": [
              {"call": "S", "label": "Start", "position": 5, "lb": 0, "time": 0},
              {"call": "1", "label": "Quarter", "position": 4.2, "lb": 3.1, "time": 24.1},
              ...
            ]
          },
          ...
        ],
        "call_labels": ["Start", "Quarter", "Half", "Three-Quarter", "Stretch", "Finish"],
        "field_size": 10,
        "projected_finish_order": ["Horse A", "Horse B", ...],
      }
    """
    projections = project_race(brain, race_id, limit=limit)
    horses_data = []
    for p in projections:
        calls = []
        for cid in CALL_ORDER:
            call_data = p["profile"].get(cid, {})
            calls.append({
                "call": cid,
                "label": CALL_LABELS[cid],
                "position": call_data.get("avg_position"),
                "lb": call_data.get("avg_lengths_behind"),
                "time": call_data.get("avg_time"),
            })
        horses_data.append({
            "name": p["horse_name"],
            "style": p["style"],
            "post_position": p["post_position"],
            "morning_line": p["morning_line"],
            "speed_figure_avg": p["profile"].get("speed_figure_avg"),
            "speed_figure_best": p["profile"].get("speed_figure_best"),
            "calls": calls,
            "projected_rank": p.get("projected_rank", {}),
        })

    finish_order = [
        h["name"] for h in sorted(
            horses_data,
            key=lambda h: h.get("projected_rank", {}).get("F", 99)
        )
    ]

    return {
        "horses": horses_data,
        "call_labels": [CALL_LABELS[c] for c in CALL_ORDER],
        "field_size": len(horses_data),
        "projected_finish_order": finish_order,
    }
