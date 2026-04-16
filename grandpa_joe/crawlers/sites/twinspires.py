"""
TwinSpires adapter — JS-rendered results pages.

TwinSpires renders results client-side, so we use the dynamic fetcher.
URL pattern: https://www.twinspires.com/bet/results?date=YYYY-MM-DD
"""

import logging
import re
from datetime import date as Date
from typing import Optional

from grandpa_joe.crawlers.base import StaticFetcher, now_iso
from grandpa_joe.crawlers.dynamic import fetch_dynamic, CRAWL4AI_AVAILABLE
from grandpa_joe.crawlers.models import CrawledEntry, CrawledRace, CrawledResult

logger = logging.getLogger(__name__)

SITE = "twinspires"
BASE = "https://www.twinspires.com"


def results_url(target_date: Optional[str] = None) -> str:
    if target_date:
        return f"{BASE}/bet/results?date={target_date}"
    return f"{BASE}/bet/results"


def crawl_results(target_date: Optional[str] = None,
                  fetcher: Optional[StaticFetcher] = None) -> CrawledResult:
    """
    Crawl TwinSpires results for a given date (default: today).

    Falls back to the static fetcher if crawl4ai is unavailable — you'll
    get less data but the crawl won't fail.
    """
    url = results_url(target_date)
    html: Optional[str] = None

    if CRAWL4AI_AVAILABLE:
        html = fetch_dynamic(url, wait_for_selector=".results-track, [data-track]",
                             timeout_ms=25000)

    if not html:
        logger.info(f"Falling back to static fetch for {url}")
        own_fetcher = fetcher is None
        if own_fetcher:
            fetcher = StaticFetcher()
        try:
            html = fetcher.get(url)
        finally:
            if own_fetcher:
                fetcher.close()

    result = CrawledResult(site=SITE, url=url, fetched_at=now_iso())
    if not html:
        result.errors.append(f"No HTML returned for {url}")
        return result

    result.races = _parse(html, target_date or str(Date.today()))
    return result


def _parse(html: str, race_date: str) -> list[CrawledRace]:
    """
    Parse TwinSpires results HTML.

    NOTE: TwinSpires markup changes frequently. Selectors here target their
    Angular result components. If parsing breaks, inspect the live page and
    update the selectors.
    """
    from selectolax.parser import HTMLParser
    tree = HTMLParser(html)
    races: list[CrawledRace] = []

    for track_block in tree.css("[data-track], .results-track, .track-results"):
        track_code = (track_block.attributes.get("data-track")
                      or _text(track_block, ".track-code")
                      or _text(track_block, ".track-name"))
        if not track_code:
            continue
        track_code = track_code.strip().upper()[:3]

        for race_block in track_block.css("[data-race], .race-result, .race-block"):
            race_num_str = (race_block.attributes.get("data-race")
                            or _text(race_block, ".race-number"))
            race_num = _int(race_num_str)
            if not race_num:
                continue

            race = CrawledRace(
                track_code=track_code,
                race_date=race_date,
                race_number=race_num,
                source_url=results_url(race_date),
                source_site=SITE,
            )
            race.distance_furlongs = _parse_distance(_text(race_block, ".distance"))
            race.surface = _parse_surface(_text(race_block, ".surface"))
            race.track_condition = _text(race_block, ".condition") or None

            for finisher in race_block.css("tr.finisher, .finisher-row, [data-finish]"):
                horse = _text(finisher, ".horse-name") or _text(finisher, ".runner")
                if not horse:
                    continue
                race.entries.append(CrawledEntry(
                    horse_name=horse.strip(),
                    finish_position=_int(_text(finisher, ".finish, .position")
                                         or finisher.attributes.get("data-finish")),
                    jockey_name=_clean(_text(finisher, ".jockey")),
                    trainer_name=_clean(_text(finisher, ".trainer")),
                    final_odds=_parse_odds(_text(finisher, ".odds")),
                    payout_win=_money(_text(finisher, ".win-pay, .payout-win")),
                    payout_place=_money(_text(finisher, ".place-pay, .payout-place")),
                    payout_show=_money(_text(finisher, ".show-pay, .payout-show")),
                ))

            if race.entries:
                race.field_size = len(race.entries)
                races.append(race)

    return races


def _text(node, selector: str) -> str:
    if not node:
        return ""
    n = node.css_first(selector)
    return n.text(strip=True) if n else ""


def _clean(s: Optional[str]) -> Optional[str]:
    if not s:
        return None
    s = s.strip()
    return s or None


def _int(val) -> Optional[int]:
    if not val:
        return None
    m = re.search(r"\d+", str(val))
    return int(m.group(0)) if m else None


def _money(val) -> Optional[float]:
    if not val:
        return None
    m = re.search(r"[\d,]+\.?\d*", str(val).replace("$", ""))
    if not m:
        return None
    try:
        return float(m.group(0).replace(",", ""))
    except ValueError:
        return None


def _parse_odds(val) -> Optional[float]:
    if not val:
        return None
    s = str(val).strip()
    if "-" in s and s[0] != "-":
        a, b = s.split("-", 1)
        try:
            return float(a) / float(b)
        except (ValueError, ZeroDivisionError):
            return None
    try:
        return float(s)
    except ValueError:
        return None


def _parse_distance(val) -> Optional[float]:
    if not val:
        return None
    s = str(val).lower()
    m = re.search(r"([\d.]+)\s*(f|furlong|mile|m)", s)
    if not m:
        return None
    num = float(m.group(1))
    unit = m.group(2)
    if unit.startswith("m"):
        return num * 8.0
    return num


def _parse_surface(val) -> str:
    if not val:
        return "dirt"
    v = str(val).strip().lower()
    if "turf" in v or v.startswith("t"):
        return "turf"
    if "syn" in v or "poly" in v or "tapeta" in v or "aw" in v:
        return "synthetic"
    return "dirt"
