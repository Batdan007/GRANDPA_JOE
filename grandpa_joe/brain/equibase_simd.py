"""
Equibase SIMD (Past Performance / Entry Card) XML parser for GRANDPA_JOE.

Parses Equibase 'simulcast' XML files (SIMDyyyymmddTRK_CTR.xml) which contain
full entry cards with embedded past performance data per horse.

File naming: SIMDyyyymmddTRK_CTR.xml
Schema: http://ifd.equibase.com/schema/simulcast.xsd

Key format details:
- Root element: <EntryRaceCard>
- Values nested in <Value>/<Description> pairs
- DistanceId in hundredths of furlongs (600 = 6.0F)
- Times in hundredths of seconds (14091 = 1:40.91)
- Lengths in hundredths (625 = 6.25 lengths)
- SpeedFigure scaled x10 (460 = 46)
- Odds as fraction "20/1" or hundredths integer "5875" (58.75-1)
- Race dates as "2023-01-01+00:00"
"""

import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)


def _val(elem, tag: str, default: str = "") -> str:
    """Get text from a tag, handling Equibase <Value> nesting."""
    child = elem.find(tag)
    if child is None:
        return default
    # Direct text
    if child.text and child.text.strip():
        return child.text.strip()
    # Nested <Value> element
    val_child = child.find("Value")
    if val_child is not None and val_child.text and val_child.text.strip():
        return val_child.text.strip()
    return default


def _desc(elem, tag: str, default: str = "") -> str:
    """Get description from a nested <Description> element."""
    child = elem.find(tag)
    if child is None:
        return default
    desc = child.find("Description")
    if desc is not None and desc.text and desc.text.strip():
        return desc.text.strip()
    return default


def _float_text(elem, tag: str, default=None) -> Optional[float]:
    """Get float from element text."""
    child = elem.find(tag)
    if child is None or not child.text:
        return default
    try:
        return float(child.text.strip().replace(",", ""))
    except (ValueError, TypeError):
        return default


def _int_text(elem, tag: str, default=None) -> Optional[int]:
    """Get int from element text."""
    child = elem.find(tag)
    if child is None or not child.text:
        return default
    try:
        return int(float(child.text.strip().replace(",", "")))
    except (ValueError, TypeError):
        return default


def _parse_date(date_str: str) -> str:
    """Parse Equibase date like '2023-01-01+00:00' to '2023-01-01'."""
    if not date_str:
        return ""
    return date_str.split("+")[0].split("T")[0].strip()


def _parse_distance(elem) -> float:
    """Parse distance from <Distance> element. Returns furlongs."""
    dist_elem = elem.find("Distance")
    if dist_elem is None:
        return 6.0
    dist_id = _int_text(dist_elem, "DistanceId")
    if dist_id and dist_id > 0:
        # DistanceId is in hundredths of furlongs
        return dist_id / 100.0
    return 6.0


def _parse_odds(odds_str: str) -> Optional[float]:
    """Parse odds from '20/1' fraction or '5875' hundredths format."""
    if not odds_str:
        return None
    odds_str = odds_str.strip()
    try:
        if "/" in odds_str:
            parts = odds_str.split("/")
            if len(parts) == 2 and parts[1]:
                return float(parts[0]) / float(parts[1])
        elif "-" in odds_str and odds_str[0] != "-":
            parts = odds_str.split("-")
            if len(parts) == 2 and parts[1]:
                return float(parts[0]) / float(parts[1])
        val = float(odds_str)
        # If integer and large, it's in hundredths
        if val > 100:
            return val / 100.0
        return val
    except (ValueError, ZeroDivisionError):
        return None


def _parse_surface(elem) -> str:
    """Parse surface from <Course> element."""
    course = elem.find("Course")
    if course is None:
        return "dirt"
    val = _val(course, "CourseType")
    if not val:
        val = _val(course, "Surface")
    mapping = {
        "D": "dirt", "T": "turf", "S": "synthetic",
        "O": "turf",  # outer turf
        "I": "turf",  # inner turf
    }
    return mapping.get(val.upper(), "dirt") if val else "dirt"


def _parse_condition(elem) -> str:
    """Parse track condition from <TrackCondition> element."""
    val = _val(elem, "TrackCondition")
    if not val:
        return "fast"
    mapping = {
        "FT": "fast", "GD": "good", "YL": "yielding",
        "SY": "sloppy", "MY": "muddy", "SF": "soft",
        "FM": "firm", "HY": "heavy", "FR": "frozen",
    }
    return mapping.get(val.upper(), val.lower())


def _person_name(elem, tag: str) -> str:
    """Extract person name from <Jockey>/<Trainer> element: 'First Last'."""
    person = elem.find(tag)
    if person is None:
        return ""
    first = ""
    last = ""
    fn = person.find("FirstName")
    if fn is not None and fn.text:
        first = fn.text.strip()
    ln = person.find("LastName")
    if ln is not None and ln.text:
        last = ln.text.strip()
    if first and last:
        return f"{first} {last}"
    return last or first or ""


def _parse_track_from_filename(filepath: str) -> Tuple[str, str]:
    """Extract track code and date from SIMD filename.

    SIMDyyyymmddTRK_CTR.xml -> (TRK, yyyy-mm-dd)
    """
    name = Path(filepath).stem
    match = re.match(r"SIMD(\d{4})(\d{2})(\d{2})([A-Z0-9]+)_([A-Z]+)", name)
    if match:
        year, month, day = match.group(1), match.group(2), match.group(3)
        track = match.group(4)
        return track, f"{year}-{month}-{day}"
    return "UNK", ""


def ingest_simd(brain, filepath: str) -> Dict:
    """
    Ingest an Equibase SIMD past performance XML file.

    Extracts:
    - Race card entries (track, date, race conditions, horses, jockeys, trainers)
    - Past performance history for each horse (the key training data)

    Args:
        brain: RacingBrain instance
        filepath: Path to SIMD XML file

    Returns:
        Dict with counts of ingested records
    """
    path = Path(filepath)
    if not path.exists():
        raise FileNotFoundError(f"SIMD file not found: {filepath}")

    counts = {
        "tracks": 0, "horses": 0, "races": 0, "entries": 0,
        "results": 0, "past_performances": 0, "files_processed": 1,
        "races_skipped": 0, "entries_skipped": 0, "pp_skipped": 0,
    }

    file_track, file_date = _parse_track_from_filename(filepath)

    try:
        tree = ET.parse(str(path))
    except ET.ParseError as e:
        logger.error(f"XML parse error in {filepath}: {e}")
        return counts

    root = tree.getroot()

    # Find races - root is <EntryRaceCard>, children are <Race>
    races = root.findall("Race")
    if not races:
        races = root.findall(".//Race")
    if not races:
        logger.warning(f"No Race elements in {filepath} (root={root.tag})")
        return counts

    # Create track
    brain.get_or_create_track(file_track, file_track)
    counts["tracks"] = 1

    for race_elem in races:
        try:
            race_number = _int_text(race_elem, "RaceNumber")
            if not race_number:
                counts["races_skipped"] += 1
                continue

            race_date = file_date
            surface = _parse_surface(race_elem)
            distance = _parse_distance(race_elem)
            race_type_elem = race_elem.find("RaceType")
            race_type = ""
            race_type_desc = ""
            if race_type_elem is not None:
                rt = race_type_elem.find("RaceType")
                if rt is not None and rt.text:
                    race_type = rt.text.strip()
                rd = race_type_elem.find("Description")
                if rd is not None and rd.text:
                    race_type_desc = rd.text.strip()

            purse = _float_text(race_elem, "PurseUSA")
            race_name_text = _val(race_elem, "RaceName")
            grade = _val(race_elem, "Grade")
            condition_text = _val(race_elem, "ConditionText")
            num_runners = _int_text(race_elem, "NumberOfRunners")

            race_id = brain.store_race(
                track_code=file_track,
                race_date=race_date,
                race_number=race_number,
                race_name=race_name_text or race_type_desc,
                race_type=race_type.lower() if race_type else "allowance",
                grade=grade or None,
                surface=surface,
                distance_furlongs=distance,
                purse=int(purse) if purse else None,
                conditions=condition_text or None,
                track_condition="fast",
            )
            counts["races"] += 1

            # Each <Starters> element IS one starter (one horse per element)
            for starter_elem in race_elem.findall("Starters"):
                try:
                    _ingest_one_starter(
                        brain, list(starter_elem), race_id, file_track,
                        race_date, surface, distance, num_runners, counts
                    )
                except Exception as e:
                    logger.warning(f"Starter parse error: {e}")
                    counts["entries_skipped"] += 1

        except Exception as e:
            logger.warning(f"Race parse error in {filepath}: {e}")
            counts["races_skipped"] += 1

    return counts



def _ingest_one_starter(brain, elements, race_id, track_code, race_date,
                        surface, distance, num_runners, counts):
    """Ingest a single starter from grouped sibling elements."""
    # Build a lookup of tag -> element for quick access
    elems_by_tag = {}
    horse_elem = None
    past_perfs = []

    for elem in elements:
        if elem.tag == "Horse":
            horse_elem = elem
        elif elem.tag == "PastPerformance":
            past_perfs.append(elem)
        else:
            elems_by_tag[elem.tag] = elem

    if horse_elem is None:
        counts["entries_skipped"] += 1
        return

    # Extract horse info
    horse_name_elem = horse_elem.find("HorseName")
    if horse_name_elem is None or not horse_name_elem.text:
        counts["entries_skipped"] += 1
        return
    horse_name = horse_name_elem.text.strip()
    if not horse_name:
        counts["entries_skipped"] += 1
        return

    # Pedigree
    sire_name = ""
    sire_elem = horse_elem.find("Sire")
    if sire_elem is not None:
        sn = sire_elem.find("HorseName")
        if sn is not None and sn.text:
            sire_name = sn.text.strip()

    dam_name = ""
    dam_elem = horse_elem.find("Dam")
    if dam_elem is not None:
        dn = dam_elem.find("HorseName")
        if dn is not None and dn.text:
            dam_name = dn.text.strip()
        # Dam sire
        dam_sire_elem = dam_elem.find("Sire")
        dam_sire = ""
        if dam_sire_elem is not None:
            ds = dam_sire_elem.find("HorseName")
            if ds is not None and ds.text:
                dam_sire = ds.text.strip()
    else:
        dam_sire = ""

    sex = _val(horse_elem, "Sex")
    color = _val(horse_elem, "Color")
    breeder_elem = horse_elem.find("BreederName")
    breeder = breeder_elem.text.strip() if breeder_elem is not None and breeder_elem.text else ""
    year_elem = horse_elem.find("YearOfBirth")
    birth_year = None
    if year_elem is not None and year_elem.text:
        try:
            birth_year = int(year_elem.text.strip())
        except ValueError:
            pass
    reg_elem = horse_elem.find("RegistrationNumber")
    reg_id = reg_elem.text.strip() if reg_elem is not None and reg_elem.text else None

    # Owner
    owner_elem = elems_by_tag.get("OwnerName")
    owner = ""
    if owner_elem is not None and owner_elem.text:
        owner = owner_elem.text.strip()

    brain.get_or_create_horse(
        horse_name,
        registration_id=reg_id,
        sire=sire_name or None,
        dam=dam_name or None,
        dam_sire=dam_sire or None,
        birth_year=birth_year,
        sex=sex or None,
        color=color or None,
        breeder=breeder or None,
        owner=owner or None,
    )
    counts["horses"] += 1

    # Entry data from sibling elements
    post_elem = elems_by_tag.get("PostPosition")
    post_position = None
    if post_elem is not None and post_elem.text:
        try:
            post_position = int(post_elem.text.strip())
        except ValueError:
            pass

    odds_elem = elems_by_tag.get("Odds")
    morning_line = None
    if odds_elem is not None and odds_elem.text:
        morning_line = _parse_odds(odds_elem.text.strip())

    weight_elem = elems_by_tag.get("WeightCarried")
    weight = None
    if weight_elem is not None and weight_elem.text:
        try:
            weight = float(weight_elem.text.strip())
        except ValueError:
            pass

    jockey_name = _person_name_from_siblings(elems_by_tag, "Jockey")
    trainer_name = _person_name_from_siblings(elems_by_tag, "Trainer")

    medication = _val_from_siblings(elems_by_tag, "Medication")
    equipment = _val_from_siblings(elems_by_tag, "Equipment")

    # Check if scratched
    scratch_val = _val_from_siblings(elems_by_tag, "ScratchIndicator")
    if scratch_val and scratch_val.upper() in ("Y", "YES", "S"):
        # Still store the entry but mark scratched - skip for now to keep data clean
        counts["entries_skipped"] += 1
        return

    entry_id = brain.store_entry(
        race_id=race_id,
        horse_name=horse_name,
        jockey_name=jockey_name or None,
        trainer_name=trainer_name or None,
        post_position=post_position,
        morning_line_odds=morning_line,
        weight_lbs=weight,
        medication=medication or None,
        equipment_changes=equipment or None,
    )
    counts["entries"] += 1

    # Ingest past performances - this is the gold for model training
    for pp_elem in past_perfs:
        try:
            _ingest_past_performance(brain, pp_elem, horse_name, counts)
        except Exception as e:
            logger.debug(f"PP parse error for {horse_name}: {e}")
            counts["pp_skipped"] += 1


def _person_name_from_siblings(elems: dict, tag: str) -> str:
    """Extract person name from sibling element dict."""
    elem = elems.get(tag)
    if elem is None:
        return ""
    first = ""
    last = ""
    fn = elem.find("FirstName")
    if fn is not None and fn.text:
        first = fn.text.strip()
    ln = elem.find("LastName")
    if ln is not None and ln.text:
        last = ln.text.strip()
    if first and last:
        return f"{first} {last}"
    return last or first or ""


def _val_from_siblings(elems: dict, tag: str) -> str:
    """Extract value from sibling element, handling <Value> nesting."""
    elem = elems.get(tag)
    if elem is None:
        return ""
    val = elem.find("Value")
    if val is not None and val.text and val.text.strip():
        return val.text.strip()
    if elem.text and elem.text.strip():
        return elem.text.strip()
    return ""


def _ingest_past_performance(brain, pp_elem, horse_name: str, counts: Dict):
    """Ingest a single past performance from a <PastPerformance> element."""
    # Track
    track_elem = pp_elem.find("Track")
    pp_track = "UNK"
    if track_elem is not None:
        tid = track_elem.find("TrackID")
        if tid is not None and tid.text:
            pp_track = tid.text.strip().upper()

    # Date
    pp_date = _parse_date(_val(pp_elem, "RaceDate"))
    if not pp_date:
        counts["pp_skipped"] += 1
        return

    # Surface & condition
    pp_surface = _parse_surface(pp_elem)
    pp_condition = _parse_condition(pp_elem)

    # Distance
    pp_distance = _parse_distance(pp_elem)

    # Field size
    pp_field_size = _int_text(pp_elem, "NumberOfStarters")

    # Race type
    race_type_elem = pp_elem.find("RaceType")
    pp_race_type = ""
    if race_type_elem is not None:
        rt = race_type_elem.find("RaceType")
        if rt is not None and rt.text:
            pp_race_type = rt.text.strip()

    # The <Start> element has the horse's specific running data
    start_elem = pp_elem.find("Start")
    if start_elem is None:
        counts["pp_skipped"] += 1
        return

    finish = _int_text(start_elem, "OfficialFinish")
    if finish is None or finish == 0:
        counts["pp_skipped"] += 1
        return

    # Speed figure (scaled x10 in Equibase)
    raw_speed = _int_text(start_elem, "SpeedFigure")
    speed_figure = None
    if raw_speed is not None and raw_speed > 0:
        speed_figure = raw_speed // 10  # 460 -> 46

    # Weight
    pp_weight = _float_text(start_elem, "WeightCarried")

    # Beaten lengths (hundredths)
    raw_behind = _int_text(start_elem, "LengthsBehind")
    beaten_lengths = None
    # Get from final PointOfCall
    for poc in start_elem.findall("PointOfCall"):
        poc_name = _val(poc, "PointOfCall")
        if poc_name == "F":  # Finish
            bl = _int_text(poc, "LengthsBehind")
            if bl is not None:
                beaten_lengths = bl / 100.0
            break

    # Final time from race fractions (winner time W)
    final_time = None
    for frac in pp_elem.findall("Fractions"):
        frac_id = _val(frac, "Fraction")
        if frac_id == "W":
            raw_time = _int_text(frac, "Time")
            if raw_time and raw_time > 0:
                final_time = raw_time / 100.0  # hundredths of seconds
            break

    # Odds
    raw_odds = _val(start_elem, "Odds")
    final_odds = _parse_odds(raw_odds) if raw_odds else None

    # Comment
    comment = _val(start_elem, "ShortComment")
    if not comment:
        comment = _val(start_elem, "LongComment")

    # Jockey/trainer for this PP
    pp_jockey = _person_name(start_elem, "Jockey")
    pp_trainer = _person_name(start_elem, "Trainer")

    # Running position (points of call)
    running_pos = {}
    call_map = {"S": "start", "1": "first_call", "2": "second_call",
                "3": "third_call", "5": "stretch", "F": "finish"}
    for poc in start_elem.findall("PointOfCall"):
        poc_id = _val(poc, "PointOfCall")
        pos = _int_text(poc, "Position")
        if poc_id in call_map and pos and pos > 0:
            running_pos[call_map[poc_id]] = pos

    # Store the past performance
    brain.store_past_performance(
        horse_name=horse_name,
        race_date=pp_date,
        track_code=pp_track,
        surface=pp_surface,
        distance_furlongs=pp_distance,
        track_condition=pp_condition,
        finish_position=finish,
        field_size=pp_field_size,
        speed_figure=speed_figure,
        beaten_lengths=beaten_lengths,
        final_time_seconds=final_time,
        weight_lbs=pp_weight,
        jockey_name=pp_jockey or None,
        trainer_name=pp_trainer or None,
        comment=comment or None,
    )
    counts["past_performances"] += 1

    # Also store the PP's race and result in the main tables for completeness
    pp_race_number = _int_text(pp_elem, "RaceNumber")
    if pp_race_number and pp_date:
        brain.get_or_create_track(pp_track, pp_track)
        race_id = brain.store_race(
            track_code=pp_track,
            race_date=pp_date,
            race_number=pp_race_number,
            race_type=pp_race_type.lower() if pp_race_type else "allowance",
            surface=pp_surface,
            distance_furlongs=pp_distance,
            track_condition=pp_condition,
        )

        entry_id = brain.store_entry(
            race_id=race_id,
            horse_name=horse_name,
            jockey_name=pp_jockey or None,
            trainer_name=pp_trainer or None,
            post_position=_int_text(start_elem, "PostPosition"),
            weight_lbs=pp_weight,
        )

        brain.store_result(
            entry_id=entry_id,
            finish_position=finish,
            beaten_lengths=beaten_lengths,
            final_odds=final_odds,
            speed_figure=speed_figure,
            final_time_seconds=final_time,
            running_position=running_pos,
            comment=comment or None,
        )
        counts["results"] += 1


def ingest_simd_directory(brain, directory: str) -> Dict:
    """
    Ingest all SIMD XML files from a directory.

    Args:
        brain: RacingBrain instance
        directory: Path to directory containing SIMD XML files

    Returns:
        Aggregate counts
    """
    dir_path = Path(directory)
    if not dir_path.is_dir():
        raise NotADirectoryError(f"Not a directory: {directory}")

    # Find SIMD XML files
    simd_files = sorted(dir_path.glob("SIMD*.xml"))
    if not simd_files:
        logger.warning(f"No SIMD XML files in {directory}")
        return {"files_processed": 0}

    totals = {"files_processed": 0, "files_failed": 0}

    for i, xml_file in enumerate(simd_files):
        if (i + 1) % 100 == 0:
            logger.info(f"Progress: {i + 1}/{len(simd_files)} files...")
        try:
            counts = ingest_simd(brain, str(xml_file))
            for k, v in counts.items():
                if k == "files_processed":
                    continue
                totals[k] = totals.get(k, 0) + v
            totals["files_processed"] += 1
        except Exception as e:
            logger.error(f"Failed: {xml_file.name}: {e}")
            totals["files_failed"] += 1

    logger.info(f"SIMD ingestion complete: {totals}")
    return totals


def is_simd_file(filepath: str) -> bool:
    """Check if a file is a SIMD past performance XML."""
    return Path(filepath).name.upper().startswith("SIMD")
