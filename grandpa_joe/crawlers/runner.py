"""
Crawl orchestrator — runs one or more site adapters and ingests into the brain.
"""

import logging
from datetime import date as Date
from typing import Iterable, Optional

from grandpa_joe.crawlers import sites as _sites
from grandpa_joe.crawlers.base import StaticFetcher, now_iso
from grandpa_joe.crawlers.ingestion_bridge import ingest_crawled
from grandpa_joe.crawlers.models import CrawlSummary

logger = logging.getLogger(__name__)


DERBY_PREP_TRACKS = ["CD", "KEE", "GP", "SA", "OP", "FG", "AQU", "TAM", "TP"]


def run_crawl(brain,
              site_names: Optional[Iterable[str]] = None,
              track_codes: Optional[Iterable[str]] = None,
              target_date: Optional[str] = None,
              use_nexus: bool = False) -> CrawlSummary:
    """
    Crawl the requested sites and ingest into the brain.

    Args:
        brain: RacingBrain instance
        site_names: ("twinspires", "equibase", "drf") — defaults to all
        track_codes: list of 2-3 char track codes for site adapters that need them
        target_date: YYYY-MM-DD, defaults to today
        use_nexus: if True, forward to ALFRED/CORTEX via NEXUS bridge

    Returns:
        CrawlSummary
    """
    site_names = list(site_names) if site_names else _sites.all_sites()
    track_codes = list(track_codes) if track_codes else DERBY_PREP_TRACKS
    target_date = target_date or str(Date.today())

    nexus_client = None
    if use_nexus:
        try:
            from grandpa_joe.nexus.client import NexusClient
            from grandpa_joe.config import get_config
            cfg = get_config()
            nexus_client = NexusClient(
                alfred_url=cfg.nexus.alfred_url,
                secret=cfg.nexus.nexus_secret,
            )
        except Exception as e:
            logger.debug(f"NEXUS unavailable, continuing without: {e}")

    summary = CrawlSummary(started_at=now_iso(), finished_at=now_iso())

    with StaticFetcher() as fetcher:
        for site_name in site_names:
            adapter = _sites.get(site_name)
            if adapter is None:
                summary.errors.append(f"unknown site: {site_name}")
                continue

            summary.sites_run.append(site_name)

            try:
                if site_name == "twinspires":
                    batches = [adapter.crawl_results(target_date, fetcher=fetcher)]
                else:
                    batches = [adapter.crawl_results(tc, target_date, fetcher=fetcher)
                               for tc in track_codes]
            except Exception as e:
                logger.warning(f"{site_name} crawl failed: {e}")
                summary.errors.append(f"{site_name}: {e}")
                continue

            for batch in batches:
                summary.races_crawled += len(batch.races)
                summary.entries_crawled += sum(len(r.entries) for r in batch.races)
                summary.errors.extend(f"{site_name}: {e}" for e in batch.errors)

                counts = ingest_crawled(brain, batch, nexus_client=nexus_client)
                summary.results_ingested += counts.get("results", 0)

    summary.finished_at = now_iso()
    return summary
