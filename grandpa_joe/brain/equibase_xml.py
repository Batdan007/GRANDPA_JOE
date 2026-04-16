"""
Equibase XML chart parser for GRANDPA_JOE.
Parses Equibase downloadable chart XML files into the racing brain.

Equibase XML charts contain full result chart data including:
- Race conditions (track, date, surface, distance, purse, etc.)
- All entries with post positions, jockeys, trainers, weights
- Results with finish positions, margins, odds, speed figures
- Fractional times and running positions
- Payouts (win/place/show/exotics)
"""

import logging
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional

logger = logging.getLogger(__name__)


# Equibase XML uses various tag patterns. These are the most common.
# The exact structure depends on the download product version.

def _text(element, tag, default=""):
    """Safely extract text from an XML element's child tag."""
    child = element.find(tag)
    if child is not None and child.text:
        return child.text.strip()
    return default


def _float(element, tag, default=None):
    """Safely extract float from XML."""
    val = _text(element, tag, "")
    if not val:
        return default
    try:
        return float(val.replace(",", "").replace("$", ""))
    except ValueError:
        return default


def _int(element, tag, default=None):
    """Safely extract int from XML."""
    val = _text(element, tag, "")
    if not val:
        return default
    try:
        return int(float(val))
    except ValueError:
        return default


def _parse_time(time_str: str) -> Optional[float]:
    """Parse a race time string like '1:10.24' into seconds."""
    if not time_str:
        return None
    try:
        if ":" in time_str:
            parts = time_str.split(":")
            return float(parts[0]) * 60 + float(parts[1])
        return float(time_str)
    except ValueError:
        return None


def _parse_odds(odds_str: str) -> Optional[float]:
    """Parse odds like '5-2' or '3.50' into decimal form."""
    if not odds_str:
        return None
    try:
        if "-" in odds_str and odds_str[0] != "-":
            parts = odds_str.split("-")
            if len(parts) == 2 and parts[1]:
                return float(parts[0]) / float(parts[1])
        return float(odds_str)
    except (ValueError, ZeroDivisionError):
        return None


def _normalize_surface(surface: str) -> str:
    """Normalize surface codes from XML."""
    if not surface:
        return "dirt"
    s = surface.strip().lower()
    mapping = {
        "d": "dirt", "dirt": "dirt", "dt": "dirt",
        "t": "turf", "turf": "turf", "tf": "turf",
        "s": "synthetic", "syn": "synthetic", "aw": "synthetic",
        "all weather": "synthetic", "poly": "synthetic", "tapeta": "synthetic",
        "inner turf": "turf", "outer turf": "turf",
    }
    return mapping.get(s, s)


def _normalize_condition(condition: str) -> str:
    """Normalize track condition from XML."""
    if not condition:
        return "fast"
    c = condition.strip().lower()
    mapping = {
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
    return mapping.get(c, c)


def _find_races(root):
    """Find race elements in the XML tree, handling various Equibase structures."""
    # Try common Equibase XML structures
    for tag_path in [
        ".//Race", ".//race", ".//RACE",
        ".//RaceChart", ".//racechart",
        ".//Chart/Race", ".//Charts/Chart",
        ".//RaceResult", ".//raceresult",
    ]:
        races = root.findall(tag_path)
        if races:
            return races

    # If root itself contains race data (single-race XML)
    if root.tag.lower() in ("race", "racechart", "raceresult"):
        return [root]

    # Last resort: look for any element with race_number/RaceNumber child
    for child in root:
        if child.find("RaceNumber") is not None or child.find("race_number") is not None:
            return list(root)

    return []


def _find_entries(race_elem):
    """Find entry/starter elements within a race."""
    for tag_path in [
        ".//Starter", ".//starter", ".//STARTER",
        ".//Entry", ".//entry", ".//ENTRY",
        ".//Horse", ".//horse",
        ".//Runner", ".//runner",
    ]:
        entries = race_elem.findall(tag_path)
        if entries:
            return entries
    return []


def _extract_race_data(race_elem) -> Dict:
    """Extract race-level data from various XML tag conventions."""
    data = {}

    # Track code
    for tag in ["TrackID", "TrackCode", "Track", "track_code", "track",
                "TrackAbbreviation", "track_id"]:
        val = _text(race_elem, tag)
        if val:
            data["track_code"] = val.upper()
            break
    if "track_code" not in data:
        track_attr = race_elem.get("track", race_elem.get("TrackID", "UNK"))
        data["track_code"] = track_attr.upper()

    # Track name
    for tag in ["TrackName", "track_name", "TrackFullName"]:
        val = _text(race_elem, tag)
        if val:
            data["track_name"] = val
            break

    # Race date
    for tag in ["RaceDate", "race_date", "Date", "date", "CardDate"]:
        val = _text(race_elem, tag)
        if val:
            data["race_date"] = val
            break
    if "race_date" not in data:
        data["race_date"] = race_elem.get("date", "")

    # Race number
    for tag in ["RaceNumber", "race_number", "RaceNum", "Number", "number"]:
        val = _int(race_elem, tag)
        if val is not None:
            data["race_number"] = val
            break
    if "race_number" not in data:
        data["race_number"] = int(race_elem.get("number", "0") or "0")

    # Surface
    for tag in ["Surface", "surface", "TrackSurface"]:
        val = _text(race_elem, tag)
        if val:
            data["surface"] = _normalize_surface(val)
            break
    data.setdefault("surface", "dirt")

    # Distance
    for tag in ["DistanceFurlongs", "distance_furlongs", "Distance", "distance",
                "DistanceID"]:
        val = _float(race_elem, tag)
        if val is not None:
            # If distance is in yards, convert to furlongs
            if val > 50:
                val = val / 220.0
            data["distance_furlongs"] = val
            break
    data.setdefault("distance_furlongs", 6.0)

    # Track condition
    for tag in ["TrackCondition", "track_condition", "Condition", "condition",
                "TrackCond"]:
        val = _text(race_elem, tag)
        if val:
            data["track_condition"] = _normalize_condition(val)
            break
    data.setdefault("track_condition", "fast")

    # Race type / class
    for tag in ["RaceType", "race_type", "Type", "type"]:
        val = _text(race_elem, tag)
        if val:
            data["race_type"] = val.lower()
            break
    data.setdefault("race_type", "allowance")

    # Grade
    for tag in ["Grade", "grade", "StakeGrade"]:
        val = _text(race_elem, tag)
        if val:
            data["grade"] = val
            break

    # Race name
    for tag in ["RaceName", "race_name", "Name", "name", "StakeName"]:
        val = _text(race_elem, tag)
        if val:
            data["race_name"] = val
            break

    # Purse
    for tag in ["Purse", "purse", "TotalPurse"]:
        val = _float(race_elem, tag)
        if val is not None:
            data["purse"] = int(val)
            break

    # Conditions text
    for tag in ["Conditions", "conditions", "RaceConditions"]:
        val = _text(race_elem, tag)
        if val:
            data["conditions"] = val
            break

    # Weather
    for tag in ["Weather", "weather"]:
        val = _text(race_elem, tag)
        if val:
            data["weather"] = val
            break

    return data


def _extract_entry_data(entry_elem) -> Dict:
    """Extract entry/starter data from various XML tag conventions."""
    data = {}

    # Horse name
    for tag in ["HorseName", "horse_name", "Horse", "horse", "Name", "name",
                "ProgramName"]:
        val = _text(entry_elem, tag)
        if val:
            data["horse_name"] = val
            break
    if "horse_name" not in data:
        data["horse_name"] = entry_elem.get("name", "Unknown")

    # Sire / Dam
    for tag in ["Sire", "sire", "SireName"]:
        val = _text(entry_elem, tag)
        if val:
            data["sire"] = val
            break
    for tag in ["Dam", "dam", "DamName"]:
        val = _text(entry_elem, tag)
        if val:
            data["dam"] = val
            break
    for tag in ["DamSire", "dam_sire", "DamSireName", "BroodmareSire"]:
        val = _text(entry_elem, tag)
        if val:
            data["dam_sire"] = val
            break

    # Sex / Age / Color
    for tag in ["Sex", "sex", "Gender"]:
        val = _text(entry_elem, tag)
        if val:
            data["sex"] = val
            break
    for tag in ["Age", "age", "YearOfBirth", "BirthYear"]:
        val = _text(entry_elem, tag)
        if val:
            data["age"] = val
            break
    for tag in ["Color", "color"]:
        val = _text(entry_elem, tag)
        if val:
            data["color"] = val
            break

    # Owner / Breeder
    for tag in ["Owner", "owner", "OwnerName"]:
        val = _text(entry_elem, tag)
        if val:
            data["owner"] = val
            break
    for tag in ["Breeder", "breeder"]:
        val = _text(entry_elem, tag)
        if val:
            data["breeder"] = val
            break

    # Jockey
    for tag in ["Jockey", "jockey", "JockeyName", "jockey_name", "Rider"]:
        val = _text(entry_elem, tag)
        if val:
            data["jockey_name"] = val
            break

    # Trainer
    for tag in ["Trainer", "trainer", "TrainerName", "trainer_name"]:
        val = _text(entry_elem, tag)
        if val:
            data["trainer_name"] = val
            break

    # Post position
    for tag in ["PostPosition", "post_position", "PP", "pp", "Post",
                "ProgramNumber"]:
        val = _int(entry_elem, tag)
        if val is not None:
            data["post_position"] = val
            break

    # Morning line odds
    for tag in ["MorningLineOdds", "morning_line_odds", "MorningLine", "ML",
                "MLOdds"]:
        val = _text(entry_elem, tag)
        if val:
            data["morning_line_odds"] = _parse_odds(val)
            break

    # Weight
    for tag in ["Weight", "weight", "WeightCarried", "weight_lbs", "Wgt"]:
        val = _float(entry_elem, tag)
        if val is not None:
            data["weight_lbs"] = val
            break

    # Medication
    for tag in ["Medication", "medication", "Med", "MedicationEquipment"]:
        val = _text(entry_elem, tag)
        if val:
            data["medication"] = val
            break

    # Finish position
    for tag in ["FinishPosition", "finish_position", "Finish", "finish",
                "OfficialFinish", "Position"]:
        val = _int(entry_elem, tag)
        if val is not None:
            data["finish_position"] = val
            break

    # Final odds
    for tag in ["FinalOdds", "final_odds", "Odds", "odds", "WagerOdds"]:
        val = _text(entry_elem, tag)
        if val:
            data["final_odds"] = _parse_odds(val)
            break

    # Speed figure
    for tag in ["SpeedFigure", "speed_figure", "Beyer", "BSF", "Speed",
                "EquibaseFigure"]:
        val = _int(entry_elem, tag)
        if val is not None:
            data["speed_figure"] = val
            break

    # Beaten lengths
    for tag in ["BeatenLengths", "beaten_lengths", "Margin", "margin", "BL"]:
        val = _float(entry_elem, tag)
        if val is not None:
            data["beaten_lengths"] = val
            break

    # Final time
    for tag in ["FinalTime", "final_time", "Time", "time", "FinishTime"]:
        val = _text(entry_elem, tag)
        if val:
            data["final_time_seconds"] = _parse_time(val)
            break

    # Comment / trip note
    for tag in ["Comment", "comment", "TripNote", "trip_note", "ChartComment"]:
        val = _text(entry_elem, tag)
        if val:
            data["comment"] = val
            break

    # Payouts (usually only on winners/placers)
    for tag in ["WinPayout", "payout_win", "WinPay"]:
        val = _float(entry_elem, tag)
        if val is not None:
            data["payout_win"] = val
            break
    for tag in ["PlacePayout", "payout_place", "PlacePay"]:
        val = _float(entry_elem, tag)
        if val is not None:
            data["payout_place"] = val
            break
    for tag in ["ShowPayout", "payout_show", "ShowPay"]:
        val = _float(entry_elem, tag)
        if val is not None:
            data["payout_show"] = val
            break

    # Running positions (fractional calls)
    running_pos = {}
    for tag in ["FirstCall", "first_call", "Call1"]:
        val = _int(entry_elem, tag)
        if val is not None:
            running_pos["first_call"] = val
            break
    for tag in ["SecondCall", "second_call", "Call2"]:
        val = _int(entry_elem, tag)
        if val is not None:
            running_pos["second_call"] = val
            break
    for tag in ["ThirdCall", "third_call", "Call3"]:
        val = _int(entry_elem, tag)
        if val is not None:
            running_pos["third_call"] = val
            break
    for tag in ["StretchCall", "stretch_call", "Stretch"]:
        val = _int(entry_elem, tag)
        if val is not None:
            running_pos["stretch"] = val
            break
    if running_pos:
        data["running_position"] = running_pos

    return data


def ingest_xml(brain, filepath: str) -> Dict:
    """
    Ingest an Equibase XML chart file into the racing brain.

    Handles various Equibase XML structures (chart downloads, result charts).

    Args:
        brain: RacingBrain instance
        filepath: Path to XML file

    Returns:
        Dict with counts of ingested records
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"XML file not found: {filepath}")

    counts = {
        "tracks": 0, "horses": 0, "races": 0, "entries": 0,
        "results": 0, "past_performances": 0, "files_processed": 1,
        "races_skipped": 0, "entries_skipped": 0,
    }

    tree = ET.parse(str(path))
    root = tree.getroot()

    races = _find_races(root)
    if not races:
        logger.warning(f"No race elements found in {filepath}")
        logger.info(f"Root tag: {root.tag}, children: {[c.tag for c in root]}")
        return counts

    logger.info(f"Found {len(races)} race(s) in {filepath}")

    for race_elem in races:
        try:
            race_data = _extract_race_data(race_elem)

            if not race_data.get("race_date") or not race_data.get("race_number"):
                logger.warning(f"Skipping race with missing date/number: {race_data}")
                counts["races_skipped"] += 1
                continue

            track_code = race_data.get("track_code", "UNK")
            brain.get_or_create_track(
                track_code, race_data.get("track_name", track_code)
            )
            counts["tracks"] += 1

            race_id = brain.store_race(
                track_code=track_code,
                race_date=race_data["race_date"],
                race_number=race_data["race_number"],
                race_name=race_data.get("race_name"),
                race_type=race_data.get("race_type", "allowance"),
                grade=race_data.get("grade"),
                surface=race_data.get("surface", "dirt"),
                distance_furlongs=race_data.get("distance_furlongs", 6.0),
                purse=race_data.get("purse"),
                conditions=race_data.get("conditions"),
                weather=race_data.get("weather"),
                track_condition=race_data.get("track_condition", "fast"),
            )
            counts["races"] += 1

            entries = _find_entries(race_elem)
            field_size = len(entries)

            for entry_elem in entries:
                try:
                    entry_data = _extract_entry_data(entry_elem)
                    horse_name = entry_data.get("horse_name", "").strip()
                    if not horse_name:
                        counts["entries_skipped"] += 1
                        continue

                    # Create horse with pedigree info
                    horse_kwargs = {}
                    for field in ["sire", "dam", "dam_sire", "sex", "color",
                                  "owner", "breeder"]:
                        if field in entry_data:
                            horse_kwargs[field] = entry_data[field]
                    if "age" in entry_data:
                        try:
                            age = int(entry_data["age"])
                            # Approximate birth year from race date
                            race_year = int(race_data["race_date"][:4])
                            horse_kwargs["birth_year"] = race_year - age
                        except (ValueError, TypeError):
                            pass

                    brain.get_or_create_horse(horse_name, **horse_kwargs)
                    counts["horses"] += 1

                    # Store entry
                    entry_id = brain.store_entry(
                        race_id=race_id,
                        horse_name=horse_name,
                        jockey_name=entry_data.get("jockey_name"),
                        trainer_name=entry_data.get("trainer_name"),
                        post_position=entry_data.get("post_position"),
                        morning_line_odds=entry_data.get("morning_line_odds"),
                        weight_lbs=entry_data.get("weight_lbs"),
                        medication=entry_data.get("medication"),
                    )
                    counts["entries"] += 1

                    # Store result if finish position available
                    finish_pos = entry_data.get("finish_position")
                    if finish_pos is not None:
                        import json
                        brain.store_result(
                            entry_id=entry_id,
                            finish_position=finish_pos,
                            beaten_lengths=entry_data.get("beaten_lengths"),
                            final_odds=entry_data.get("final_odds"),
                            speed_figure=entry_data.get("speed_figure"),
                            final_time_seconds=entry_data.get("final_time_seconds"),
                            running_position=entry_data.get("running_position", {}),
                            comment=entry_data.get("comment"),
                            payout_win=entry_data.get("payout_win"),
                            payout_place=entry_data.get("payout_place"),
                            payout_show=entry_data.get("payout_show"),
                        )
                        counts["results"] += 1

                        # Store past performance
                        brain.store_past_performance(
                            horse_name=horse_name,
                            race_date=race_data["race_date"],
                            track_code=track_code,
                            surface=race_data.get("surface", "dirt"),
                            distance_furlongs=race_data.get("distance_furlongs"),
                            track_condition=race_data.get("track_condition"),
                            finish_position=finish_pos,
                            field_size=field_size,
                            speed_figure=entry_data.get("speed_figure"),
                            beaten_lengths=entry_data.get("beaten_lengths"),
                            final_time_seconds=entry_data.get("final_time_seconds"),
                            weight_lbs=entry_data.get("weight_lbs"),
                            jockey_name=entry_data.get("jockey_name"),
                            trainer_name=entry_data.get("trainer_name"),
                            comment=entry_data.get("comment"),
                        )
                        counts["past_performances"] += 1

                except Exception as e:
                    logger.warning(f"Entry parse error: {e}")
                    counts["entries_skipped"] += 1

        except Exception as e:
            logger.warning(f"Race parse error: {e}")
            counts["races_skipped"] += 1

    logger.info(f"XML ingestion complete: {counts}")
    return counts


def ingest_xml_directory(brain, directory: str, pattern: str = "*.xml") -> Dict:
    """
    Ingest all XML files in a directory.

    Args:
        brain: RacingBrain instance
        directory: Path to directory containing XML files
        pattern: Glob pattern for XML files

    Returns:
        Aggregate counts
    """
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    xml_files = sorted(dir_path.glob(pattern))
    if not xml_files:
        logger.warning(f"No {pattern} files found in {directory}")
        return {"files_processed": 0}

    totals = {}
    for xml_file in xml_files:
        logger.info(f"Processing {xml_file.name}...")
        try:
            counts = ingest_xml(brain, str(xml_file))
            for k, v in counts.items():
                totals[k] = totals.get(k, 0) + v
        except Exception as e:
            logger.error(f"Failed to process {xml_file.name}: {e}")
            totals["files_failed"] = totals.get("files_failed", 0) + 1

    return totals
