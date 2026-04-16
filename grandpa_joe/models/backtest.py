"""
Time-series backtest harness for GRANDPA_JOE.

Trains on races before a holdout cutoff, then predicts every race in the
holdout window and compares to actual finish positions.

Metrics:
  - top1_win_pct: predicted #1 actually won
  - top3_show_pct: predicted #1 finished in the top 3
  - mae_finish: mean absolute error of predicted rank vs finish position
  - roi_win: simulated ROI placing $1 on predicted favorite (uses actual payout_win)
  - morning_line_top1_baseline: same as top1 but using morning-line favorite

Usage:
  python -m grandpa_joe.models.backtest 2025-05-03 2025-05-03 --track CD
  python -m grandpa_joe.models.backtest 2025-04-01 2025-05-03 --retrain
"""

import argparse
import logging
from datetime import date as Date
from typing import Dict, Optional

logger = logging.getLogger(__name__)


def run_backtest(brain, holdout_start: str, holdout_end: str,
                 track_code: Optional[str] = None,
                 retrain: bool = False) -> Dict:
    """
    Backtest the handicapper over a holdout window.

    If retrain=True, trains a fresh model on races strictly before holdout_start.
    Otherwise uses whatever model is currently saved.
    """
    from grandpa_joe.config import get_config
    from grandpa_joe.models.handicapper import GrandpaJoeHandicapper

    if retrain:
        from grandpa_joe.models.trainer import train_model
        logger.info(f"Retraining on races before {holdout_start}...")
        metrics = train_model(brain, get_config().model, before_date=holdout_start)
        logger.info(f"Train metrics: {metrics}")

    handicapper = GrandpaJoeHandicapper(brain, get_config().model)

    conn = brain._connect()
    try:
        track_filter = "AND t.code = ?" if track_code else ""
        params = [holdout_start, holdout_end]
        if track_code:
            params.append(track_code.upper())

        races = conn.execute(f"""
            SELECT ra.id, ra.race_date, ra.race_number, t.code as track_code
            FROM races ra
            JOIN tracks t ON ra.track_id = t.id
            JOIN entries e ON e.race_id = ra.id
            JOIN results r ON r.entry_id = e.id
            WHERE ra.race_date >= ? AND ra.race_date <= ? {track_filter}
            GROUP BY ra.id
            ORDER BY ra.race_date, t.code, ra.race_number
        """, params).fetchall()
    finally:
        conn.close()

    if not races:
        return {"error": "No holdout races found", "holdout_start": holdout_start,
                "holdout_end": holdout_end, "track": track_code}

    top1_hits = 0
    top3_hits = 0
    ml_baseline_hits = 0
    total_with_pred = 0
    total_mae = 0.0
    mae_n = 0
    roi_numerator = 0.0
    roi_denominator = 0

    per_race: list[Dict] = []

    for race_row in races:
        race = dict(race_row)
        try:
            rankings = handicapper.predict(race["id"])
        except Exception as e:
            logger.debug(f"predict failed for race {race['id']}: {e}")
            continue

        if not rankings:
            continue

        race_detail = _score_race(brain, race, rankings)
        if race_detail is None:
            continue

        per_race.append(race_detail)
        total_with_pred += 1

        if race_detail["top1_hit"]:
            top1_hits += 1
        if race_detail["top3_hit"]:
            top3_hits += 1
        if race_detail["ml_top1_hit"]:
            ml_baseline_hits += 1
        if race_detail["mae"] is not None:
            total_mae += race_detail["mae"]
            mae_n += 1
        if race_detail["roi_contrib"] is not None:
            roi_numerator += race_detail["roi_contrib"]
            roi_denominator += 1

    return {
        "races_evaluated": total_with_pred,
        "top1_win_pct": _pct(top1_hits, total_with_pred),
        "top3_show_pct": _pct(top3_hits, total_with_pred),
        "morning_line_top1_baseline": _pct(ml_baseline_hits, total_with_pred),
        "mae_finish": round(total_mae / mae_n, 3) if mae_n else None,
        "roi_win": round(roi_numerator / roi_denominator, 4) if roi_denominator else None,
        "roi_races": roi_denominator,
        "per_race": per_race[:20],
    }


def _score_race(brain, race: Dict, rankings: list) -> Optional[Dict]:
    conn = brain._connect()
    try:
        rows = conn.execute("""
            SELECT e.id as entry_id, h.name as horse_name,
                   r.finish_position, r.payout_win, e.morning_line_odds
            FROM entries e
            JOIN horses h ON e.horse_id = h.id
            JOIN results r ON r.entry_id = e.id
            WHERE e.race_id = ? AND e.scratched = 0
        """, (race["id"],)).fetchall()
    finally:
        conn.close()

    if not rows:
        return None

    actual = {row["horse_name"]: dict(row) for row in rows}
    pred_top = rankings[0]
    pred_top_name = pred_top["horse_name"]

    ml_winner = min(
        (r for r in rows if r["morning_line_odds"] is not None),
        key=lambda r: r["morning_line_odds"],
        default=None,
    )

    top1_hit = False
    top3_hit = False
    roi_contrib = None
    if pred_top_name in actual:
        fin = actual[pred_top_name]["finish_position"]
        top1_hit = (fin == 1)
        top3_hit = (fin <= 3 and fin >= 1)
        payout = actual[pred_top_name].get("payout_win")
        if top1_hit and payout:
            roi_contrib = (payout / 2.0) - 1.0  # $2 base bet → profit per $1 staked
        else:
            roi_contrib = -1.0

    ml_hit = False
    if ml_winner:
        actual_row = actual.get(ml_winner["horse_name"])
        if actual_row:
            ml_hit = (actual_row["finish_position"] == 1)

    maes = []
    for rank_info in rankings:
        name = rank_info["horse_name"]
        if name in actual:
            maes.append(abs(rank_info["rank"] - actual[name]["finish_position"]))
    mae = sum(maes) / len(maes) if maes else None

    return {
        "race_id": race["id"],
        "track": race["track_code"],
        "date": race["race_date"],
        "race_number": race["race_number"],
        "predicted_winner": pred_top_name,
        "actual_winner": next((n for n, r in actual.items() if r["finish_position"] == 1), None),
        "top1_hit": top1_hit,
        "top3_hit": top3_hit,
        "ml_top1_hit": ml_hit,
        "mae": mae,
        "roi_contrib": roi_contrib,
    }


def _pct(num: int, den: int) -> Optional[float]:
    return round(100.0 * num / den, 2) if den else None


def main():
    ap = argparse.ArgumentParser(description="GRANDPA_JOE backtest harness")
    ap.add_argument("holdout_start", help="YYYY-MM-DD")
    ap.add_argument("holdout_end", help="YYYY-MM-DD")
    ap.add_argument("--track", help="Filter by track code (e.g. CD)")
    ap.add_argument("--retrain", action="store_true",
                    help="Retrain model on races before holdout_start first")
    args = ap.parse_args()

    from grandpa_joe.brain import RacingBrain
    brain = RacingBrain()

    result = run_backtest(brain, args.holdout_start, args.holdout_end,
                          track_code=args.track, retrain=args.retrain)

    import json as _json
    print(_json.dumps(result, indent=2, default=str))


if __name__ == "__main__":
    main()
