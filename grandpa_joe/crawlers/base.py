"""
Base HTTP fetcher for static HTML pages.

Uses httpx + selectolax for speed. Respects robots.txt and rate limits.
Graceful degradation — if httpx is missing we raise with install hint.
"""

import logging
import time
from datetime import datetime
from typing import Optional
from urllib.parse import urlparse
from urllib.robotparser import RobotFileParser

logger = logging.getLogger(__name__)

try:
    import httpx
    HTTPX_AVAILABLE = True
except ImportError:
    HTTPX_AVAILABLE = False

try:
    from selectolax.parser import HTMLParser
    SELECTOLAX_AVAILABLE = True
except ImportError:
    SELECTOLAX_AVAILABLE = False


USER_AGENT = "GRANDPA_JOE/0.1 (+https://github.com/Batdan007; horse racing research)"
DEFAULT_RATE_LIMIT_SEC = 1.0


class StaticFetcher:
    """Thin wrapper over httpx with per-host rate limiting and robots.txt check."""

    def __init__(self, rate_limit_sec: float = DEFAULT_RATE_LIMIT_SEC,
                 timeout: float = 15.0, respect_robots: bool = True):
        if not HTTPX_AVAILABLE:
            raise RuntimeError(
                "httpx not installed. Run: pip install 'grandpa-joe[crawl]'"
            )
        self.rate_limit_sec = rate_limit_sec
        self.timeout = timeout
        self.respect_robots = respect_robots
        self._last_hit: dict[str, float] = {}
        self._robots_cache: dict[str, RobotFileParser] = {}
        self._client = httpx.Client(
            timeout=timeout,
            headers={"User-Agent": USER_AGENT},
            follow_redirects=True,
        )

    def close(self):
        self._client.close()

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        self.close()

    def _host(self, url: str) -> str:
        return urlparse(url).netloc

    def _rate_limit(self, host: str):
        now = time.monotonic()
        last = self._last_hit.get(host, 0.0)
        wait = self.rate_limit_sec - (now - last)
        if wait > 0:
            time.sleep(wait)
        self._last_hit[host] = time.monotonic()

    def _robots_ok(self, url: str) -> bool:
        if not self.respect_robots:
            return True
        host = self._host(url)
        rp = self._robots_cache.get(host)
        if rp is None:
            rp = RobotFileParser()
            rp.set_url(f"https://{host}/robots.txt")
            try:
                rp.read()
            except Exception as e:
                logger.debug(f"robots.txt unreachable for {host}: {e}")
            self._robots_cache[host] = rp
        try:
            return rp.can_fetch(USER_AGENT, url)
        except Exception:
            return True

    def get(self, url: str) -> Optional[str]:
        """Fetch URL and return HTML text. Returns None on failure."""
        if not self._robots_ok(url):
            logger.warning(f"robots.txt disallows {url} — skipping")
            return None

        self._rate_limit(self._host(url))
        try:
            resp = self._client.get(url)
            resp.raise_for_status()
            return resp.text
        except Exception as e:
            logger.warning(f"GET {url} failed: {e}")
            return None

    def parse(self, html: str) -> "HTMLParser":
        if not SELECTOLAX_AVAILABLE:
            raise RuntimeError(
                "selectolax not installed. Run: pip install 'grandpa-joe[crawl]'"
            )
        return HTMLParser(html)


def now_iso() -> str:
    return datetime.utcnow().isoformat(timespec="seconds") + "Z"
