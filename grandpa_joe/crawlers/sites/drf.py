"""
Daily Racing Form (DRF) adapter — static entries & results.

https://www.drf.com/results/raceDate/YYYY-MM-DD/track/TRK
"""

import logging
import re
from datetime import date as Date
from typing import Optional

from grandpa_joe.crawlers.base import StaticFetcher, now_iso
from grandpa_joe.crawlers.models import CrawledEntry, CrawledRace, CrawledResult

logger = logging.getLogger(__name__)

SITE = "drf"
BASE = "https://www.drf.com"


def results_url(track_code: str, target_date: str) -> str:
    return f"{BASE}/results/raceDate/{target_date}/track/{track_code.upper()}"


def crawl_results(track_code: str, target_date: Optional[str] = None,
                  fetcher: Optional[StaticFetcher] = None) -> CrawledResult:
    target_date = target_date or str(Date.today())
    url = results_url(track_code, target_date)

    own = fetcher is None
    if own:
        fetcher = StaticFetcher()
    try:
        html = fetcher.get(url)
    finally:
        if own:
            fetcher.close()

    result = CrawledResult(site=SITE, url=url, fetched_at=now_iso())
    if not html:
        result.errors.append(f"No HTML from {url}")
        return result

    result.races = _parse(html, track_code.upper(), target_date, url)
    return result


def _parse(html: str, track_code: str, race_date: str, url: str) -> list[CrawledRace]:
    from selectolax.parser import HTMLParser
    tree = HTMLParser(html)
    races: list[CrawledRace] = []

    for block in tree.css(".race-result, .raceResultBlock, .result-race"):
        header = block.css_first(".race-header, .raceHeader, h2, h3")
        if not header:
            continue
        m = re.search(r"Race\s*(\d+)", header.text(strip=True), re.IGNORECASE)
        if not m:
            continue

        race = CrawledRace(
            track_code=track_code,
            race_date=race_date,
            race_number=int(m.group(1)),
            source_url=url,
            source_site=SITE,
        )

        for row in block.css("tr.finisher, .finisher-row"):
            cells = row.css("td")
            if len(cells) < 2:
                continue
            texts = [c.text(strip=True) for c in cells]

            finish = _int(texts[0])
            horse = texts[1] if len(texts) > 1 else None
            if not horse or finish is None:
                continue

            race.entries.append(CrawledEntry(
                horse_name=horse,
                finish_position=finish,
                jockey_name=texts[2] if len(texts) > 2 else None,
                trainer_name=texts[3] if len(texts) > 3 else None,
                final_odds=_odds(texts[4]) if len(texts) > 4 else None,
            ))

        if race.entries:
            race.field_size = len(race.entries)
            races.append(race)

    return races


def _int(val) -> Optional[int]:
    if not val:
        return None
    m = re.search(r"\d+", str(val))
    return int(m.group(0)) if m else None


def _odds(val) -> Optional[float]:
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
