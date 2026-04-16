"""
GRANDPA_JOE web crawlers.

Hybrid design:
- httpx + selectolax for static HTML (fast, lightweight)
- crawl4ai fallback for JS-rendered pages (heavy but handles TwinSpires etc.)

All crawled results are ingested into the RacingBrain and can optionally be
tiered by CORTEX via the NEXUS bridge to ALFRED.

Sites supported (adapters in crawlers/sites/):
  - twinspires (JS, dynamic)
  - equibase (static, results charts)
  - drf (static, past performances)
  - bloodhorse (static, news)
  - horseracingnation (static)
"""

from grandpa_joe.crawlers.models import (
    CrawledEntry,
    CrawledRace,
    CrawledResult,
    CrawlSummary,
)

__all__ = ["CrawledEntry", "CrawledRace", "CrawledResult", "CrawlSummary"]
