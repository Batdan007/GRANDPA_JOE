"""
Dynamic/JS-rendered page fetcher — uses crawl4ai only when needed.

crawl4ai pulls in Playwright (~400MB of browsers), so we only reach for it
on sites that require JS execution to produce the data (TwinSpires is the
canonical example). Keep this optional.
"""

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

try:
    from crawl4ai import AsyncWebCrawler
    CRAWL4AI_AVAILABLE = True
except ImportError:
    CRAWL4AI_AVAILABLE = False


def fetch_dynamic(url: str, wait_for_selector: Optional[str] = None,
                  timeout_ms: int = 20000) -> Optional[str]:
    """
    Render a page with headless Chromium and return the final HTML.

    Returns None if crawl4ai isn't installed or the fetch fails.
    """
    if not CRAWL4AI_AVAILABLE:
        logger.warning(
            "crawl4ai not installed — cannot render JS page. "
            "Install with: pip install 'grandpa-joe[crawl-dynamic]'"
        )
        return None

    async def _run() -> Optional[str]:
        try:
            async with AsyncWebCrawler(verbose=False) as crawler:
                kwargs = {"url": url, "timeout": timeout_ms}
                if wait_for_selector:
                    kwargs["wait_for"] = wait_for_selector
                result = await crawler.arun(**kwargs)
                if result and result.success:
                    return result.html
                logger.warning(f"crawl4ai failed for {url}")
                return None
        except Exception as e:
            logger.warning(f"crawl4ai exception for {url}: {e}")
            return None

    try:
        return asyncio.run(_run())
    except RuntimeError:
        loop = asyncio.new_event_loop()
        try:
            return loop.run_until_complete(_run())
        finally:
            loop.close()
