"""
Equibase adapter — static results/entries pages.

Equibase serves most result charts as static HTML (plus XML/PDF for
registered members). We scrape the public entries page:
https://www.equibase.com/premium/eqpRaceEntriesList.cfm?TRK=CD&DATE=MM-DD-YYYY
and results chart:
https://www.equibase.com/premium/eqpRaceResultsList.cfm?TRK=CD&DATE=MM-DD-YYYY

For Derby prep scraping we focus on result pages.
"""

import logging
import re
from datetime import date as Date, datetime
from typing import Optional

from grandpa_joe.crawlers.base import StaticFetcher, now_iso
from grandpa_joe.crawlers.models import CrawledEntry, CrawledRace, CrawledResult

logger = logging.getLogger(__name__)

SITE = "equibase"
BASE = "https://www.equibase.com"


def results_url(track_code: str, target_date: str) -> str:
    d = datetime.fromisoformat(target_date).strftime("%m-%d-%Y")
    return f"{BASE}/premium/eqpRaceResultsList.cfm?TRK={track_code.upper()}&DATE={d}"


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

    for block in tree.css("table.raceResults, div.raceResultsBlock, .race-result"):
        header = block.css_first("caption, .raceHeader, h3")
        if not header:
            continue
        header_text = header.text(strip=True)
        m = re.search(r"Race\s*(\d+)", header_text, re.IGNORECASE)
        if not m:
            continue
        race_num = int(m.group(1))

        race = CrawledRace(
            track_code=track_code,
            race_date=race_date,
            race_number=race_num,
            source_url=url,
            source_site=SITE,
        )

        dist_match = re.search(r"([\d./]+)\s*(furlong|mile)", header_text, re.IGNORECASE)
        if dist_match:
            try:
                num = float(dist_match.group(1))
                race.distance_furlongs = num * 8.0 if "mile" in dist_match.group(2).lower() else num
            except ValueError:
                pass

        if "turf" in header_text.lower():
            race.surface = "turf"

        for row in block.css("tbody tr, tr.resultRow"):
            cells = row.css("td")
            if len(cells) < 3:
                continue
            texts = [c.text(strip=True) for c in cells]

            finish = _int(texts[0])
            horse = texts[2] if len(texts) > 2 else texts[1]
            if not horse or not finish:
                continue

            race.entries.append(CrawledEntry(
                horse_name=horse,
                finish_position=finish,
                jockey_name=texts[3] if len(texts) > 3 else None,
                trainer_name=texts[4] if len(texts) > 4 else None,
                final_odds=_odds(texts[5]) if len(texts) > 5 else None,
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
