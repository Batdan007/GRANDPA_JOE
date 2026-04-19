"""Score DJR's favorite key-part-wheel bet structures on a holdout window.

Each structure is a list of slots — one per finishing position — naming which
of Joe's ranked picks may fill that slot. A ticket is any distinct 4-tuple
(3-tuple for trifectas) drawn from the slots. Hit = actual finishers match.

Structures (see memory/user_betting_style.md):
  Tri KPW   : 1   / 2,3,4   / 2,3,4                   (6 combos)
  Super KPW : 1   / 2,3,4,5 / 2,3,4,5   / 2,3,4,5     (24 combos)
  Super A   : 1,2 / 1,2,3   / 2,3,4,5   / 2,3,4,5     (60 combos)
  Super B   : 1,2 / 1,2     / 3,4,5     / 3,4,5       (12 combos)
  Super C   : 1   / 2,3     / 3,4,5     / 3,4,5,6     (13 combos)
  Super D   : 1   / 2,3     / 2,3,4     / 2,3,4,5     (8  combos)
"""

import argparse
import json
import sys
from itertools import product
from pathlib import Path

REPO = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO))

from grandpa_joe.brain.racing_brain import RacingBrain
from grandpa_joe.config import get_config
from grandpa_joe.models.handicapper import GrandpaJoeHandicapper


# Slot specs — each tuple is one finishing position; integers are Joe's ranks.
STRUCTURES = {
    "tri_kpw":   [(1,),      (2, 3, 4),     (2, 3, 4)],
    "super_kpw": [(1,),      (2, 3, 4, 5),  (2, 3, 4, 5),  (2, 3, 4, 5)],
    "super_a":   [(1, 2),    (1, 2, 3),     (2, 3, 4, 5),  (2, 3, 4, 5)],
    "super_b":   [(1, 2),    (1, 2),        (3, 4, 5),     (3, 4, 5)],
    "super_c":   [(1,),      (2, 3),        (3, 4, 5),     (3, 4, 5, 6)],
    "super_d":   [(1,),      (2, 3),        (2, 3, 4),     (2, 3, 4, 5)],
}


def structure_combo_count(slots) -> int:
    """Number of distinct tuples over the slots (no repeats)."""
    return sum(1 for c in product(*slots) if len(set(c)) == len(c))


def structure_hits(slots, joe_top, actual_by_position) -> bool:
    """True if actual finishers (1st..Nth) are each in their slot's pick set."""
    for pos, ranks in enumerate(slots, start=1):
        actual = actual_by_position.get(pos)
        if not actual:
            return False
        slot_horses = {joe_top[r - 1] for r in ranks if r - 1 < len(joe_top)}
        if actual not in slot_horses:
            return False
    return True


def _recent_beyer(conn, horse_name: str, before_date: str):
    """Most recent speed figure for a horse strictly before a cutoff date."""
    row = conn.execute(
        """
        SELECT pp.speed_figure
        FROM past_performances pp
        JOIN horses h ON h.id = pp.horse_id
        WHERE h.name = ? AND pp.race_date < ?
          AND pp.speed_figure IS NOT NULL AND pp.speed_figure > 0
        ORDER BY pp.race_date DESC
        LIMIT 1
        """,
        (horse_name, before_date),
    ).fetchone()
    return row["speed_figure"] if row else None


def _best_beyer_last5(conn, horse_name: str, before_date: str):
    row = conn.execute(
        """
        SELECT MAX(speed_figure) AS best FROM (
            SELECT pp.speed_figure FROM past_performances pp
            JOIN horses h ON h.id = pp.horse_id
            WHERE h.name = ? AND pp.race_date < ?
              AND pp.speed_figure IS NOT NULL AND pp.speed_figure > 0
            ORDER BY pp.race_date DESC LIMIT 5
        )
        """,
        (horse_name, before_date),
    ).fetchone()
    return row["best"] if row and row["best"] else None


def score_key_wheel(brain, start: str, end: str):
    handicapper = GrandpaJoeHandicapper(brain, get_config().model)
    conn = brain._connect()
    races = conn.execute(
        """
        SELECT ra.id, ra.race_date, ra.race_number, t.code AS track_code
        FROM races ra
        JOIN tracks t ON ra.track_id = t.id
        JOIN entries e ON e.race_id = ra.id
        JOIN results r ON r.entry_id = e.id
        WHERE ra.race_date BETWEEN ? AND ?
          AND ra.purse IS NOT NULL
        GROUP BY ra.id
        HAVING COUNT(e.id) >= 4
        ORDER BY ra.race_date, t.code, ra.race_number
        """,
        (start, end),
    ).fetchall()
    conn.close()

    structure_counts = {k: {"hits": 0, "total": 0} for k in STRUCTURES}
    field_size_sum = 0
    field_size_n = 0

    tri_wheel_beyer_sum = 0.0
    tri_wheel_beyer_n = 0
    super_wheel_beyer_sum = 0.0
    super_wheel_beyer_n = 0
    key_beyer_sum = 0.0
    key_beyer_n = 0

    for race_row in races:
        race_id = race_row["id"]
        race_date = race_row["race_date"]
        try:
            rankings = handicapper.predict(race_id)
        except Exception:
            continue
        if not rankings or len(rankings) < 4:
            continue

        conn = brain._connect()
        actual = conn.execute(
            """
            SELECT h.name AS horse_name, r.finish_position
            FROM entries e
            JOIN horses h ON e.horse_id = h.id
            JOIN results r ON r.entry_id = e.id
            WHERE e.race_id = ? AND e.scratched = 0
              AND r.finish_position IS NOT NULL
              AND r.finish_position > 0
            """,
            (race_id,),
        ).fetchall()

        by_horse = {row["horse_name"]: row["finish_position"] for row in actual}
        finisher_by_pos = {row["finish_position"]: row["horse_name"] for row in actual}
        joe_top5 = [r["horse_name"] for r in rankings[:5]]
        joe_top6 = [r["horse_name"] for r in rankings[:6]]

        # Beyer / speed figure averages for the wheel and key
        key_fig = _recent_beyer(conn, joe_top5[0], race_date)
        if key_fig is not None:
            key_beyer_sum += key_fig
            key_beyer_n += 1

        tri_wheel_figs = [
            _recent_beyer(conn, h, race_date) for h in joe_top5[1:4]
        ]
        tri_wheel_figs = [v for v in tri_wheel_figs if v is not None]
        if tri_wheel_figs:
            tri_wheel_beyer_sum += sum(tri_wheel_figs) / len(tri_wheel_figs)
            tri_wheel_beyer_n += 1

        if len(joe_top5) >= 5:
            super_wheel_figs = [
                _recent_beyer(conn, h, race_date) for h in joe_top5[1:5]
            ]
            super_wheel_figs = [v for v in super_wheel_figs if v is not None]
            if super_wheel_figs:
                super_wheel_beyer_sum += sum(super_wheel_figs) / len(super_wheel_figs)
                super_wheel_beyer_n += 1

        conn.close()
        if len(actual) < 4:
            continue

        field_size_sum += len(actual)
        field_size_n += 1

        for name, slots in STRUCTURES.items():
            n_slots = len(slots)
            required = set(range(1, n_slots + 1))
            if not required.issubset(finisher_by_pos):
                continue
            top_list = joe_top6 if name == "super_c" else joe_top5
            structure_counts[name]["total"] += 1
            if structure_hits(slots, top_list, finisher_by_pos):
                structure_counts[name]["hits"] += 1

    avg_field = field_size_sum / field_size_n if field_size_n else 0
    structures_out = {}
    for name, counts in structure_counts.items():
        structures_out[name] = {
            "combos_per_ticket": structure_combo_count(STRUCTURES[name]),
            "hits": counts["hits"],
            "total": counts["total"],
            "hit_rate_pct": round(100.0 * counts["hits"] / counts["total"], 2) if counts["total"] else None,
        }
    return {
        "window": f"{start} to {end}",
        "races_scored": field_size_n,
        "avg_field_size": round(avg_field, 2),
        "structures": structures_out,
        "beyer_avg": {
            "key_horse": round(key_beyer_sum / key_beyer_n, 2) if key_beyer_n else None,
            "tri_wheel_picks_2_3_4": round(tri_wheel_beyer_sum / tri_wheel_beyer_n, 2) if tri_wheel_beyer_n else None,
            "super_wheel_picks_2_3_4_5": round(super_wheel_beyer_sum / super_wheel_beyer_n, 2) if super_wheel_beyer_n else None,
        },
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("start", help="YYYY-MM-DD")
    ap.add_argument("end", help="YYYY-MM-DD")
    args = ap.parse_args()

    brain = RacingBrain()
    result = score_key_wheel(brain, args.start, args.end)
    print(json.dumps(result, indent=2))


if __name__ == "__main__":
    main()
