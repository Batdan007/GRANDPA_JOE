"""
Bridge between crawled results and the RacingBrain.

Takes a CrawledResult and writes it through the same brain.store_* methods
used by CSV/XML ingestion — one canonical write path.
"""

import logging
from typing import Optional

from grandpa_joe.crawlers.models import CrawledResult

logger = logging.getLogger(__name__)


def ingest_crawled(brain, crawled: CrawledResult,
                   nexus_client=None) -> dict:
    """
    Write a CrawledResult into the brain. Optionally forward to CORTEX via NEXUS.

    Returns counts dict.
    """
    counts = {"races": 0, "entries": 0, "results": 0, "past_performances": 0,
              "rows_skipped": 0}

    for race in crawled.races:
        try:
            race_id = brain.store_race(
                track_code=race.track_code,
                race_date=race.race_date,
                race_number=race.race_number,
                race_type=race.race_type or "allowance",
                grade=race.grade,
                surface=race.surface or "dirt",
                distance_furlongs=race.distance_furlongs or 6.0,
                purse=race.purse,
                class_level=race.class_level,
                track_condition=race.track_condition or "fast",
            )
            counts["races"] += 1
        except Exception as e:
            logger.warning(f"store_race failed for {race.track_code} R{race.race_number}: {e}")
            counts["rows_skipped"] += 1
            continue

        for entry in race.entries:
            try:
                entry_id = brain.store_entry(
                    race_id=race_id,
                    horse_name=entry.horse_name,
                    jockey_name=entry.jockey_name,
                    trainer_name=entry.trainer_name,
                    post_position=entry.post_position,
                    morning_line_odds=entry.morning_line_odds,
                    weight_lbs=entry.weight_lbs,
                    medication=entry.medication,
                )
                counts["entries"] += 1

                if entry.finish_position is not None:
                    brain.store_result(
                        entry_id=entry_id,
                        finish_position=entry.finish_position,
                        beaten_lengths=entry.beaten_lengths,
                        final_odds=entry.final_odds,
                        speed_figure=entry.speed_figure,
                        final_time_seconds=None,
                        comment=entry.comment,
                        payout_win=entry.payout_win,
                        payout_place=entry.payout_place,
                        payout_show=entry.payout_show,
                    )
                    counts["results"] += 1

                    brain.store_past_performance(
                        horse_name=entry.horse_name,
                        race_date=race.race_date,
                        track_code=race.track_code,
                        surface=race.surface or "dirt",
                        distance_furlongs=race.distance_furlongs,
                        track_condition=race.track_condition or "fast",
                        class_level=race.class_level,
                        finish_position=entry.finish_position,
                        field_size=race.field_size,
                        speed_figure=entry.speed_figure,
                        beaten_lengths=entry.beaten_lengths,
                        final_time_seconds=None,
                        weight_lbs=entry.weight_lbs,
                        jockey_name=entry.jockey_name,
                        trainer_name=entry.trainer_name,
                        comment=entry.comment,
                    )
                    counts["past_performances"] += 1
            except Exception as e:
                logger.warning(f"entry write failed for {entry.horse_name}: {e}")
                counts["rows_skipped"] += 1

    if nexus_client is not None:
        _forward_to_cortex(nexus_client, crawled, counts)

    return counts


def _forward_to_cortex(nexus_client, crawled: CrawledResult, counts: dict) -> None:
    """Ask CORTEX to capture this crawl — fire and forget."""
    try:
        content = _summarize(crawled, counts)
        importance = _importance_for(crawled)
        metadata = {
            "source": f"crawl:{crawled.site}",
            "url": crawled.url,
            "fetched_at": crawled.fetched_at,
            "race_count": counts.get("races", 0),
            "entry_count": counts.get("entries", 0),
            "result_count": counts.get("results", 0),
        }
        nexus_client.cortex_capture(
            content=content,
            importance=importance,
            topic=f"racing:{crawled.site}",
            metadata=metadata,
        )
    except Exception as e:
        logger.debug(f"CORTEX forward failed (non-fatal): {e}")


def _summarize(crawled: CrawledResult, counts: dict) -> str:
    tracks = sorted({r.track_code for r in crawled.races})
    dates = sorted({r.race_date for r in crawled.races})
    return (
        f"Crawled {crawled.site}: {counts.get('races', 0)} races, "
        f"{counts.get('results', 0)} finishers across tracks={tracks} dates={dates}"
    )


def _importance_for(crawled: CrawledResult) -> float:
    """Fresh race data = hotter. CORTEX auto-tiers from this hint."""
    from datetime import date as Date, datetime
    today = Date.today()
    derby_day = Date(2026, 5, 2)

    fresh = 0
    for race in crawled.races:
        try:
            d = datetime.fromisoformat(race.race_date).date()
        except (ValueError, TypeError):
            continue
        if d == today:
            return 8.0
        if abs((derby_day - d).days) <= 90:
            fresh += 1
    if fresh > 0:
        return 6.5
    return 4.0
