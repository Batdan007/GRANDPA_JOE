"""Extract per-call pace data from Equibase SIMD XML files.

Walks each SIMD XML, and for every <PastPerformance>/<Start>/<PointOfCall>
emits one pace row per call point. Uses the race's <Fractions> leader times
plus the horse's LengthsBehind at that call to compute the horse's time.

Conversion: 1 length ≈ 0.17 seconds (standard thoroughbred pace rule).
"""

import logging
import re
import xml.etree.ElementTree as ET
from pathlib import Path
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger(__name__)

LENGTH_TO_SEC = 0.17

# Fraction ID ordering: 1 = first internal split, 2 = second, ... W = winner/final
# Point-of-call IDs used as call_id: S, 1, 2, 3, 4, 5, F
CALL_ORDER = {"S": 0, "1": 1, "2": 2, "3": 3, "4": 4, "5": 5, "F": 6}


def _track_from_filename(filepath: str) -> Tuple[str, str]:
    name = Path(filepath).stem
    m = re.match(r"SIMD(\d{4})(\d{2})(\d{2})([A-Z0-9]+)_([A-Z]+)", name)
    if not m:
        return "UNK", ""
    return m.group(4), f"{m.group(1)}-{m.group(2)}-{m.group(3)}"


def _parse_date(date_str: str) -> str:
    if not date_str:
        return ""
    return date_str.split("+")[0].split("T")[0].strip()


def _int(elem, tag: str) -> Optional[int]:
    child = elem.find(tag)
    if child is None or not child.text:
        return None
    try:
        return int(float(child.text.strip()))
    except (ValueError, TypeError):
        return None


def _extract_fractions(pp_elem) -> Dict[str, float]:
    """Return {fraction_id: seconds} for the race's split times."""
    out: Dict[str, float] = {}
    for f in pp_elem.findall("Fractions"):
        fid = f.findtext("Fraction")
        t = f.findtext("Time")
        if fid and t:
            try:
                tv = int(t) / 100.0
            except (ValueError, TypeError):
                continue
            if tv > 0:
                out[fid.strip()] = tv
    return out


def _pp_race_meta(pp_elem) -> Tuple[str, float, str]:
    """Return (pp_track, pp_distance_furlongs, pp_surface)."""
    track_elem = pp_elem.find("Track")
    pp_track = "UNK"
    if track_elem is not None:
        tid = track_elem.findtext("TrackID")
        if tid:
            pp_track = tid.strip().upper()
    dist_elem = pp_elem.find("Distance")
    dist_furl = 0.0
    if dist_elem is not None:
        dist_id = dist_elem.findtext("DistanceId")
        if dist_id:
            try:
                dist_furl = int(dist_id) / 100.0
            except (ValueError, TypeError):
                pass
    course = pp_elem.find("Course")
    surface = "dirt"
    if course is not None:
        ct = course.findtext("CourseType")
        if ct:
            surface_map = {"D": "dirt", "T": "turf", "S": "synthetic", "O": "turf", "I": "turf"}
            surface = surface_map.get(ct.upper(), "dirt")
    return pp_track, dist_furl, surface


def extract_pace_rows(xml_path: str) -> List[Dict]:
    """Return one dict per (horse, race_date, call_id) with pace numbers.

    Caller provides (horse_name, race_date, track_code, call_id, ...); the
    loader resolves horse_id via brain.horse_cache.
    """
    tree = ET.parse(xml_path)
    root = tree.getroot()
    rows: List[Dict] = []

    for race in root.findall(".//Race"):
        for starter in race.findall("Starters"):
            horse_elem = starter.find("Horse")
            if horse_elem is None:
                continue
            horse_name = (horse_elem.findtext("HorseName") or "").strip()
            if not horse_name:
                continue
            for pp in starter.findall("PastPerformance"):
                start = pp.find("Start")
                if start is None:
                    continue
                pp_date = _parse_date(pp.findtext("RaceDate") or "")
                if not pp_date:
                    continue
                pp_track, dist_furl, surface = _pp_race_meta(pp)
                fractions = _extract_fractions(pp)
                speed_figure = None
                sf = _int(start, "SpeedFigure")
                if sf is not None and sf > 0:
                    speed_figure = sf // 10

                for poc in start.findall("PointOfCall"):
                    call_id = (poc.findtext("PointOfCall") or "").strip()
                    if not call_id or call_id not in CALL_ORDER:
                        continue
                    position = _int(poc, "Position")
                    lb_hundredths = _int(poc, "LengthsBehind")
                    if lb_hundredths is None:
                        lb_hundredths = 0
                    lengths_behind = lb_hundredths / 100.0

                    # Map call_id → fraction_id for leader cumulative time.
                    # S has no fraction. F uses fraction "W" (winner final).
                    # Calls 1..5 use fraction "1".."5".
                    if call_id == "S":
                        leader_time = 0.0
                    elif call_id == "F":
                        leader_time = fractions.get("W", 0.0)
                    else:
                        leader_time = fractions.get(call_id, 0.0)

                    horse_time = (
                        leader_time + lengths_behind * LENGTH_TO_SEC
                        if leader_time > 0 or call_id == "S"
                        else None
                    )

                    rows.append(
                        {
                            "horse_name": horse_name,
                            "race_date": pp_date,
                            "track_code": pp_track,
                            "distance_furlongs": dist_furl or None,
                            "surface": surface,
                            "call_id": call_id,
                            "call_order": CALL_ORDER[call_id],
                            "position": position,
                            "lengths_behind": lengths_behind,
                            "leader_time_sec": leader_time if leader_time > 0 else None,
                            "horse_time_sec": horse_time if horse_time and horse_time > 0 else None,
                            "speed_figure": speed_figure,
                        }
                    )
    return rows
