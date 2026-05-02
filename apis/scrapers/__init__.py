"""apis.scrapers -- alternative web scraping backends.

Joveo's primary scraper stack is ``web_scraper_router.scrape_url`` (Apify ->
Jina -> Tavily -> LLM-assisted -> Cache -> stdlib urllib). This sub-package
hosts *optional* alternative backends that can be plugged in when the primary
stack is exhausted or when their unique capabilities are needed.

Currently exposed:
    * ``scrape_with_crawl4ai`` -- Crawl4AI (open-source self-hosted async
      browser scraper, BM25 noise filter, clean markdown output).
    * ``scrape_many`` -- batch wrapper with bounded concurrency.

Both helpers degrade gracefully when the optional ``crawl4ai`` package is not
installed or the ``CRAWL4AI_ENABLED`` feature flag is off; callers always get a
normalized result dict with ``error`` populated rather than an exception.
"""

from __future__ import annotations

from .crawl4ai_client import scrape_many, scrape_with_crawl4ai

__all__ = ["scrape_many", "scrape_with_crawl4ai"]
