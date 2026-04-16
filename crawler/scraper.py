import asyncio
import random
from typing import Callable, List, Optional

import httpx
from bs4 import BeautifulSoup
from loguru import logger
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from config.settings import settings
from models.schemas import ChapterMeta

_USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 14_2) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.2 Safari/605.1.15",
]

_CONTENT_SELECTORS = [
    ".chapter-content",
    "#chapter-content",
    ".content-chapter",
    ".text-content",
    ".chapter-body",
    "div[class*='chapter']",
]

_TITLE_SELECTORS = [
    "h2.chapter-title",
    "h1.chapter-title",
    ".chapter-title",
    "h2",
    "h1",
]


def _random_headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


def parse_chapter(html: str, url: str, chapter_num: int) -> ChapterMeta:
    """Parse raw HTML into ChapterMeta. Raises ValueError if content not found."""
    soup = BeautifulSoup(html, "lxml")

    title = f"Chương {chapter_num}"
    for selector in _TITLE_SELECTORS:
        tag = soup.select_one(selector)
        if tag:
            title = tag.get_text(strip=True)
            break

    content_tag = None
    for selector in _CONTENT_SELECTORS:
        content_tag = soup.select_one(selector)
        if content_tag:
            break

    if not content_tag:
        raise ValueError(
            f"Cannot find chapter content for chapter {chapter_num} at {url}"
        )

    # Remove noise tags
    for noise in content_tag(["script", "style", "ins", "iframe", "a"]):
        noise.decompose()

    content = content_tag.get_text(separator="\n", strip=True)

    if len(content) < 100:
        raise ValueError(
            f"Chapter content too short ({len(content)} chars) for chapter {chapter_num}"
        )

    return ChapterMeta(
        chapter_num=chapter_num,
        title=title,
        url=url,
        content=content,
        status="CRAWLED",
    )


async def _fetch_with_retry(
    client: httpx.AsyncClient,
    url: str,
    chapter_num: int,
) -> ChapterMeta:
    @retry(
        stop=stop_after_attempt(settings.crawler_max_retries),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        retry=retry_if_exception_type(
            (httpx.HTTPError, httpx.TimeoutException, ValueError)
        ),
        reraise=True,
    )
    async def _do_fetch() -> ChapterMeta:
        logger.debug("Fetching chapter {} | url={}", chapter_num, url)
        response = await client.get(
            url, headers=_random_headers(), timeout=30.0, follow_redirects=True
        )
        response.raise_for_status()
        return parse_chapter(response.text, url, chapter_num)

    return await _do_fetch()


async def crawl_chapters(
    chapter_nums: List[int],
    on_fetched: Optional[Callable[[ChapterMeta], None]] = None,
) -> List[ChapterMeta]:
    """Crawl a list of chapters. Rate-limited to 1 req/s via sleep.
    Returns all ChapterMeta (with status CRAWLED or ERROR).
    """
    results: List[ChapterMeta] = []
    delay = 1.0 / settings.crawler_rate_limit

    async with httpx.AsyncClient() as client:
        for chapter_num in chapter_nums:
            url = settings.get_chapter_url(chapter_num)
            try:
                chapter = await _fetch_with_retry(client, url, chapter_num)
                results.append(chapter)
                if on_fetched:
                    on_fetched(chapter)
            except Exception as exc:
                logger.error(
                    "Failed to fetch chapter {} | error={}", chapter_num, str(exc)
                )
                results.append(
                    ChapterMeta(
                        chapter_num=chapter_num,
                        title=f"Chương {chapter_num}",
                        url=url,
                        status="ERROR",
                        error_msg=str(exc),
                    )
                )
            await asyncio.sleep(delay)

    return results
