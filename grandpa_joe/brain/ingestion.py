"""
CSV data ingestion for GRANDPA_JOE.
Parses Equibase-style and DRF-style CSV formats into the racing brain.
"""

import csv
import logging
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional

logger = logging.getLogger(__name__)


# ============================================================================
# Column mapping for common CSV formats
# ============================================================================

EQUIBASE_COLUMNS = {
    "track": "track_code",
    "Track": "track_code",
    "track_code": "track_code",
    "date": "race_date",
    "Date": "race_date",
    "race_date": "race_date",
    "race": "race_number",
    "Race": "race_number",
    "race_number": "race_number",
    "race_num": "race_number",
    "horse": "horse_name",
    "Horse": "horse_name",
    "horse_name": "horse_name",
    "HorseName": "horse_name",
    "jockey": "jockey_name",
    "Jockey": "jockey_name",
    "jockey_name": "jockey_name",
    "trainer": "trainer_name",
    "Trainer": "trainer_name",
    "trainer_name": "trainer_name",
    "pp": "post_position",
    "PP": "post_position",
    "post_position": "post_position",
    "PostPosition": "post_position",
    "ml_odds": "morning_line_odds",
    "ML": "morning_line_odds",
    "morning_line": "morning_line_odds",
    "MorningLine": "morning_line_odds",
    "morning_line_odds": "morning_line_odds",
    "finish": "finish_position",
    "Finish": "finish_position",
    "finish_position": "finish_position",
    "FinishPosition": "finish_position",
    "FP": "finish_position",
    "odds": "final_odds",
    "Odds": "final_odds",
    "final_odds": "final_odds",
    "FinalOdds": "final_odds",
    "speed": "speed_figure",
    "Speed": "speed_figure",
    "speed_figure": "speed_figure",
    "SpeedFigure": "speed_figure",
    "Beyer": "speed_figure",
    "beyer": "speed_figure",
    "BSF": "speed_figure",
    "surface": "surface",
    "Surface": "surface",
    "dist": "distance_furlongs",
    "Dist": "distance_furlongs",
    "distance": "distance_furlongs",
    "Distance": "distance_furlongs",
    "distance_furlongs": "distance_furlongs",
    "condition": "track_condition",
    "Condition": "track_condition",
    "track_condition": "track_condition",
    "TrackCondition": "track_condition",
    "race_type": "race_type",
    "RaceType": "race_type",
    "Type": "race_type",
    "type": "race_type",
    "grade": "grade",
    "Grade": "grade",
    "purse": "purse",
    "Purse": "purse",
    "weight": "weight_lbs",
    "Weight": "weight_lbs",
    "weight_lbs": "weight_lbs",
    "Wgt": "weight_lbs",
    "beaten_lengths": "beaten_lengths",
    "BL": "beaten_lengths",
    "BeatenLengths": "beaten_lengths",
    "time": "final_time_seconds",
    "Time": "final_time_seconds",
    "FinalTime": "final_time_seconds",
    "final_time": "final_time_seconds",
    "comment": "comment",
    "Comment": "comment",
    "TripNote": "comment",
    "sire": "sire",
    "Sire": "sire",
    "dam": "dam",
    "Dam": "dam",
    "sex": "sex",
    "Sex": "sex",
    "age": "age",
    "Age": "age",
    "medication": "medication",
    "Med": "medication",
    "Medication": "medication",
    "class_level": "class_level",
    "Class": "class_level",
    "field_size": "field_size",
    "FieldSize": "field_size",
    "Runners": "field_size",
    "payout_win": "payout_win",
    "WinPay": "payout_win",
    "payout_place": "payout_place",
    "PlacePay": "payout_place",
    "payout_show": "payout_show",
    "ShowPay": "payout_show",
}


def _normalize_columns(headers: list) -> Dict[str, str]:
    """Map CSV headers to our standard column names."""
    mapping = {}
    for h in headers:
        h_stripped = h.strip()
        if h_stripped in EQUIBASE_COLUMNS:
            mapping[h_stripped] = EQUIBASE_COLUMNS[h_stripped]
        else:
            # Try lowercase
            h_lower = h_stripped.lower().replace(" ", "_")
            if h_lower in EQUIBASE_COLUMNS:
                mapping[h_stripped] = EQUIBASE_COLUMNS[h_lower]
    return mapping


def _safe_float(val, default=None):
    """Safely convert to float."""
    if val is None or val == "" or val == "N/A":
        return default
    try:
        # Handle odds like "5-2" -> 2.5
        if isinstance(val, str) and "-" in val and val[0] != "-":
            parts = val.split("-")
            if len(parts) == 2:
                return float(parts[0]) / float(parts[1])
        # Handle time like "1:10.2" -> 70.2
        if isinstance(val, str) and ":" in val:
            parts = val.split(":")
            return float(parts[0]) * 60 + float(parts[1])
        return float(val.replace(",", "").replace("$", ""))
    except (ValueError, ZeroDivisionError):
        return default


def _safe_int(val, default=None):
    """Safely convert to int."""
    if val is None or val == "" or val == "N/A":
        return default
    try:
        return int(float(val))
    except ValueError:
        return default


def _normalize_surface(val: str) -> str:
    """Normalize surface codes."""
    if not val:
        return "dirt"
    v = val.strip().lower()
    if v in ("d", "dirt", "dt"):
        return "dirt"
    elif v in ("t", "turf", "tf"):
        return "turf"
    elif v in ("s", "syn", "synthetic", "aw", "all weather", "poly", "tapeta"):
        return "synthetic"
    return v


def _normalize_condition(val: str) -> str:
    """Normalize track condition codes."""
    if not val:
        return "fast"
    v = val.strip().lower()
    conditions = {
        "ft": "fast", "fast": "fast",
        "gd": "good", "good": "good",
        "yl": "yielding", "yielding": "yielding",
        "sy": "sloppy", "sloppy": "sloppy",
        "my": "muddy", "muddy": "muddy",
        "sf": "soft", "soft": "soft",
        "fm": "firm", "firm": "firm",
        "hy": "heavy", "heavy": "heavy",
        "fr": "frozen", "frozen": "frozen",
    }
    return conditions.get(v, v)


def ingest_csv(brain, filepath: str, format_hint: str = "auto") -> Dict:
    """
    Ingest a CSV file into the racing brain.

    Supports flexible column naming - maps common Equibase/DRF column names
    to internal schema.

    Returns dict with counts of ingested records.
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"CSV file not found: {filepath}")

    counts = {
        "tracks": 0, "horses": 0, "jockeys": 0, "trainers": 0,
        "races": 0, "entries": 0, "results": 0, "past_performances": 0,
        "rows_processed": 0, "rows_skipped": 0,
    }

    with open(path, "r", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        col_map = _normalize_columns(reader.fieldnames or [])

        if not col_map:
            raise ValueError(
                f"Could not map any columns. Headers found: {reader.fieldnames}"
            )

        logger.info(f"Column mapping: {col_map}")

        # Check for required minimum columns
        mapped_cols = set(col_map.values())
        if "horse_name" not in mapped_cols:
            raise ValueError("CSV must have a horse name column")

        for row_num, raw_row in enumerate(reader, start=1):
            try:
                # Remap columns
                row = {}
                for orig_col, mapped_col in col_map.items():
                    row[mapped_col] = raw_row.get(orig_col, "").strip()

                horse_name = row.get("horse_name", "").strip()
                if not horse_name:
                    counts["rows_skipped"] += 1
                    continue

                track_code = row.get("track_code", "UNK").strip().upper()
                race_date = row.get("race_date", "")
                race_number = _safe_int(row.get("race_number"), 0)

                # Store race if we have enough info
                race_id = None
                if race_date and race_number:
                    race_id = brain.store_race(
                        track_code=track_code,
                        race_date=race_date,
                        race_number=race_number,
                        race_type=row.get("race_type", "allowance"),
                        grade=row.get("grade"),
                        surface=_normalize_surface(row.get("surface", "")),
                        distance_furlongs=_safe_float(row.get("distance_furlongs"), 6.0),
                        purse=_safe_int(row.get("purse")),
                        class_level=_safe_int(row.get("class_level")),
                        track_condition=_normalize_condition(row.get("track_condition", "")),
                    )
                    counts["races"] += 1

                # Store entry
                entry_id = None
                if race_id:
                    entry_id = brain.store_entry(
                        race_id=race_id,
                        horse_name=horse_name,
                        jockey_name=row.get("jockey_name"),
                        trainer_name=row.get("trainer_name"),
                        post_position=_safe_int(row.get("post_position")),
                        morning_line_odds=_safe_float(row.get("morning_line_odds")),
                        weight_lbs=_safe_float(row.get("weight_lbs")),
                        medication=row.get("medication"),
                    )
                    counts["entries"] += 1

                # Store result if we have finish position
                finish_pos = _safe_int(row.get("finish_position"))
                if entry_id and finish_pos is not None:
                    brain.store_result(
                        entry_id=entry_id,
                        finish_position=finish_pos,
                        beaten_lengths=_safe_float(row.get("beaten_lengths")),
                        final_odds=_safe_float(row.get("final_odds")),
                        speed_figure=_safe_int(row.get("speed_figure")),
                        final_time_seconds=_safe_float(row.get("final_time_seconds")),
                        comment=row.get("comment"),
                        payout_win=_safe_float(row.get("payout_win")),
                        payout_place=_safe_float(row.get("payout_place")),
                        payout_show=_safe_float(row.get("payout_show")),
                    )
                    counts["results"] += 1

                # Store past performance (denormalized)
                if finish_pos is not None:
                    brain.store_past_performance(
                        horse_name=horse_name,
                        race_date=race_date or "unknown",
                        track_code=track_code,
                        surface=_normalize_surface(row.get("surface", "")),
                        distance_furlongs=_safe_float(row.get("distance_furlongs")),
                        track_condition=_normalize_condition(row.get("track_condition", "")),
                        class_level=_safe_int(row.get("class_level")),
                        finish_position=finish_pos,
                        field_size=_safe_int(row.get("field_size")),
                        speed_figure=_safe_int(row.get("speed_figure")),
                        beaten_lengths=_safe_float(row.get("beaten_lengths")),
                        final_time_seconds=_safe_float(row.get("final_time_seconds")),
                        weight_lbs=_safe_float(row.get("weight_lbs")),
                        jockey_name=row.get("jockey_name"),
                        trainer_name=row.get("trainer_name"),
                        comment=row.get("comment"),
                    )
                    counts["past_performances"] += 1

                counts["rows_processed"] += 1

            except Exception as e:
                logger.warning(f"Row {row_num}: {e}")
                counts["rows_skipped"] += 1

    logger.info(f"Ingestion complete: {counts}")
    return counts
