import asyncio
import random
from abc import ABC, abstractmethod
from pathlib import Path
from typing import Callable, Dict, List, Optional
from urllib.parse import urlparse

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


def _random_headers() -> dict:
    return {
        "User-Agent": random.choice(_USER_AGENTS),
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "vi-VN,vi;q=0.9,en;q=0.8",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }


# ---------------------------------------------------------------------------
# Abstract base
# ---------------------------------------------------------------------------

class BaseScraper(ABC):
    """Base class for all per-site scrapers."""

    title_selectors: List[str] = ["h2", "h1"]
    content_selectors: List[str] = [".chapter-content", "div[class*='chapter']"]
    title_strip_tags: List[str] = []

    def _extract(self, html: str, url: str, chapter_num: int) -> ChapterMeta:
        """Parse raw HTML into a single ChapterMeta (no chunking)."""
        soup = BeautifulSoup(html, "lxml")

        title = f"Chương {chapter_num}"
        for sel in self.title_selectors:
            tag = soup.select_one(sel)
            if tag:
                for noise_tag in self.title_strip_tags:
                    for el in tag(noise_tag):
                        el.decompose()
                title = tag.get_text(strip=True) or title
                break

        content_tag = None
        for sel in self.content_selectors:
            content_tag = soup.select_one(sel)
            if content_tag:
                break

        if not content_tag:
            raise ValueError(
                f"Cannot find chapter content for chapter {chapter_num} at {url}"
            )

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

    @abstractmethod
    def parse(self, html: str, url: str, chapter_num: int) -> List[ChapterMeta]:
        """Parse HTML into one or more ChapterMeta objects."""
        ...

    async def _fetch_with_retry(
        self,
        client: httpx.AsyncClient,
        url: str,
        chapter_num: int,
    ) -> List[ChapterMeta]:
        @retry(
            stop=stop_after_attempt(settings.crawler_max_retries),
            wait=wait_exponential(multiplier=1, min=2, max=60),
            retry=retry_if_exception_type(
                (httpx.HTTPError, httpx.TimeoutException, ValueError)
            ),
            reraise=True,
        )
        async def _do() -> List[ChapterMeta]:
            logger.debug("Fetching chapter {} | url={}", chapter_num, url)
            response = await client.get(
                url, headers=_random_headers(), timeout=30.0, follow_redirects=True
            )
            response.raise_for_status()
            return self.parse(response.text, url, chapter_num)

        return await _do()


# ---------------------------------------------------------------------------
# SequentialScraper — truyencv.io and similar sites
# ---------------------------------------------------------------------------

class SequentialScraper(BaseScraper):
    """Sites with sequential numbered chapter URLs.
    One page → one chapter.
    """

    title_selectors = [".chapter-title", "h2", "h1"]
    content_selectors = [
        ".chapter-content",
        "#chapter-content",
        ".content-chapter",
        ".text-content",
        ".chapter-body",
    ]
    title_strip_tags = []

    def parse(self, html: str, url: str, chapter_num: int) -> List[ChapterMeta]:
        return [self._extract(html, url, chapter_num)]

    async def crawl(
        self,
        chapter_nums: List[int],
        on_fetched: Optional[Callable[[ChapterMeta], None]] = None,
    ) -> List[ChapterMeta]:
        """Crawl by chapter number, URL derived from settings.get_chapter_url()."""
        results: List[ChapterMeta] = []
        delay = 1.0 / settings.crawler_rate_limit

        async with httpx.AsyncClient() as client:
            for chapter_num in chapter_nums:
                url = settings.get_chapter_url(chapter_num)
                try:
                    chapters = await self._fetch_with_retry(client, url, chapter_num)
                    results.extend(chapters)
                    for ch in chapters:
                        if on_fetched:
                            on_fetched(ch)
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


# ---------------------------------------------------------------------------
# ChunkedScraper — truyenc.com and similar sites
# ---------------------------------------------------------------------------

class ChunkedScraper(BaseScraper):
    """Sites where one page can be a very long story.
    Content is split at paragraph boundaries into chunks of ~`chunk_size` chars,
    each chunk becoming a separate ChapterMeta.
    """

    title_selectors = [".card .content h1"]
    content_selectors = [".story-content"]
    title_strip_tags = ["i", "span"]
    chunk_size: int = settings.wiki_pass2_budget  # align with LLM budget → 1 chunk = 1 LLM call, no content lost

    # Selector for the server-rendered chapter list on the story index page.
    # Maps domain-specific container → href pattern to accept.
    chapter_list_selector: str = "#tab-latest-chapters a[href]"

    async def fetch_chapter_list(self, story_url: str) -> List[str]:
        """Fetch the chapter URL list from a story index page.

        Returns an ordered list of absolute chapter URLs.
        Raises ValueError if no chapter links are found.
        """
        headers = _random_headers()
        async with httpx.AsyncClient(follow_redirects=True) as client:
            response = await client.get(story_url, headers=headers, timeout=30.0)
            response.raise_for_status()

        soup = BeautifulSoup(response.text, "lxml")
        base = str(response.url)
        from urllib.parse import urljoin

        links: List[str] = []
        seen: set = set()
        for a in soup.select(self.chapter_list_selector):
            href = a.get("href", "").strip()
            if not href or href.startswith("javascript"):
                continue
            abs_href = urljoin(base, href)
            if abs_href not in seen:
                seen.add(abs_href)
                links.append(abs_href)

        if not links:
            raise ValueError(
                f"No chapter links found at {story_url} "
                f"(selector: {self.chapter_list_selector!r})"
            )

        logger.info(
            "fetch_chapter_list: {} chapters found at {}", len(links), story_url
        )
        return links

    def _chunk(
        self, content: str, title: str, url: str, starting_num: int
    ) -> List[ChapterMeta]:
        """Split content into paragraph-aligned chunks.

        If a single paragraph exceeds chunk_size it is force-split at character
        boundaries so very long wall-of-text pages are still chunked correctly.
        """
        paragraphs = [p for p in content.split("\n") if p.strip()]

        chunks: List[str] = []
        current: List[str] = []
        current_len = 0

        for para in paragraphs:
            if len(para) > self.chunk_size:
                # Flush accumulated text first
                if current:
                    chunks.append("\n".join(current))
                    current = []
                    current_len = 0
                # Force-split the oversized paragraph at character boundaries
                for i in range(0, len(para), self.chunk_size):
                    chunks.append(para[i : i + self.chunk_size])
            elif current and current_len + len(para) > self.chunk_size:
                chunks.append("\n".join(current))
                current = [para]
                current_len = len(para)
            else:
                current.append(para)
                current_len += len(para)

        if current:
            chunks.append("\n".join(current))

        total = len(chunks)
        return [
            ChapterMeta(
                chapter_num=starting_num + i,
                title=f"{title} ({i + 1}/{total})" if total > 1 else title,
                url=url,
                content=chunk_text,
                status="CRAWLED",
            )
            for i, chunk_text in enumerate(chunks)
        ]

    def parse(self, html: str, url: str, chapter_num: int) -> List[ChapterMeta]:
        base = self._extract(html, url, chapter_num)
        if len(base.content) <= self.chunk_size:
            return [base]
        return self._chunk(base.content, base.title, url, chapter_num)

    async def crawl(
        self,
        urls: List[str],
        starting_num: int = 1,
        on_fetched: Optional[Callable[[ChapterMeta], None]] = None,
    ) -> List[ChapterMeta]:
        """Crawl from explicit URL list. Long pages are auto-chunked.
        Chapter numbers are assigned sequentially accounting for chunks produced.
        """
        results: List[ChapterMeta] = []
        delay = 1.0 / settings.crawler_rate_limit
        next_num = starting_num

        async with httpx.AsyncClient() as client:
            for url in urls:
                try:
                    chapters = await self._fetch_with_retry(client, url, next_num)
                    results.extend(chapters)
                    for ch in chapters:
                        if on_fetched:
                            on_fetched(ch)
                    next_num += len(chapters)
                except Exception as exc:
                    logger.error("Failed to fetch url={} | error={}", url, str(exc))
                    results.append(
                        ChapterMeta(
                            chapter_num=next_num,
                            title=f"Chương {next_num}",
                            url=url,
                            status="ERROR",
                            error_msg=str(exc),
                        )
                    )
                    next_num += 1
                await asyncio.sleep(delay)

        return results


# ---------------------------------------------------------------------------
# LocalRemakeScraper — data/remake as chapter source
# ---------------------------------------------------------------------------

class LocalRemakeScraper(BaseScraper):
    """Read chapter content from markdown files in data/remake."""

    def _source_dir(self) -> Path:
        source_dir_raw = settings.chapter_source_dir
        if not source_dir_raw:
            raise ValueError("chapter_source_dir is not configured")
        source_dir = Path(source_dir_raw)
        if not source_dir.is_absolute():
            source_dir = Path(__file__).resolve().parent.parent / source_dir
        if not source_dir.exists():
            raise ValueError(f"Local chapter source dir not found: {source_dir}")
        return source_dir

    def parse(self, html: str, url: str, chapter_num: int) -> List[ChapterMeta]:
        raise NotImplementedError("LocalRemakeScraper does not parse HTML")

    @staticmethod
    def _title_from_markdown(text: str, fallback: str) -> str:
        for line in text.splitlines():
            stripped = line.strip()
            if stripped.startswith("#"):
                return stripped.lstrip("#").strip() or fallback
        return fallback

    def _read_step(self, source_dir: Path, chapter_num: int) -> Optional[ChapterMeta]:
        narrative_path = source_dir / f"step{chapter_num}.md"
        if not narrative_path.exists():
            return None

        narrative_text = narrative_path.read_text(encoding="utf-8").strip()
        content = narrative_text
        if not content:
            return None

        return ChapterMeta(
            chapter_num=chapter_num,
            title=self._title_from_markdown(narrative_text, f"Step {chapter_num}"),
            url=f"remake://step{chapter_num}",
            content=content,
            status="CRAWLED",
        )

    async def crawl(
        self,
        chapter_nums: List[int],
        on_fetched: Optional[Callable[[ChapterMeta], None]] = None,
    ) -> List[ChapterMeta]:
        results: List[ChapterMeta] = []
        source_dir = self._source_dir()

        for chapter_num in chapter_nums:
            chapter = self._read_step(source_dir, chapter_num)
            if not chapter:
                logger.error("Missing local remake chapter | chapter={}", chapter_num)
                results.append(
                    ChapterMeta(
                        chapter_num=chapter_num,
                        title=f"Step {chapter_num}",
                        url=f"remake://step{chapter_num}",
                        status="ERROR",
                        error_msg=f"Missing local remake files for step {chapter_num}",
                    )
                )
                continue

            results.append(chapter)
            if on_fetched:
                on_fetched(chapter)

        return results


# ---------------------------------------------------------------------------
# Registry & factory
# ---------------------------------------------------------------------------

_REGISTRY: Dict[str, BaseScraper] = {
    "truyencv.io": SequentialScraper(),
    "truyenc.com": ChunkedScraper(),
}

_DEFAULT_SCRAPER: BaseScraper = SequentialScraper()


def get_scraper(url: str) -> BaseScraper:
    """Return the appropriate scraper for the given URL's domain."""
    host = urlparse(url).hostname or ""
    domain = host.removeprefix("www.")
    return _REGISTRY.get(domain, _DEFAULT_SCRAPER)


# ---------------------------------------------------------------------------
# Backward-compatible top-level functions
# ---------------------------------------------------------------------------

def parse_chapter(html: str, url: str, chapter_num: int) -> ChapterMeta:
    """Parse HTML using the domain-appropriate scraper. Returns the first chunk."""
    return get_scraper(url)._extract(html, url, chapter_num)


async def crawl_chapters(
    chapter_nums: List[int],
    on_fetched: Optional[Callable[[ChapterMeta], None]] = None,
) -> List[ChapterMeta]:
    """Crawl chapters via settings.get_chapter_url(), using the domain-appropriate scraper."""
    if settings.chapter_source_dir:
        source_dir = Path(settings.chapter_source_dir)
        if not source_dir.is_absolute():
            source_dir = Path(__file__).resolve().parent.parent / source_dir
        if source_dir.exists():
            return await LocalRemakeScraper().crawl(chapter_nums, on_fetched=on_fetched)

    scraper = get_scraper(settings.base_url)
    if isinstance(scraper, SequentialScraper):
        return await scraper.crawl(chapter_nums, on_fetched=on_fetched)
    # ChunkedScraper (or other): build explicit URL list from chapter numbers
    urls = [settings.get_chapter_url(n) for n in chapter_nums]
    starting_num = chapter_nums[0] if chapter_nums else 1
    return await scraper.crawl(urls, starting_num=starting_num, on_fetched=on_fetched)


async def crawl_chapter_urls(
    chapter_urls: List[str],
    starting_num: int = 1,
    on_fetched: Optional[Callable[[ChapterMeta], None]] = None,
) -> List[ChapterMeta]:
    """Crawl from explicit URL list, auto-selecting scraper by domain."""
    if not chapter_urls:
        return []
    scraper = get_scraper(chapter_urls[0])
    if isinstance(scraper, ChunkedScraper):
        return await scraper.crawl(chapter_urls, starting_num=starting_num, on_fetched=on_fetched)
    # SequentialScraper or default: fetch each URL with explicit chapter numbers
    results: List[ChapterMeta] = []
    delay = 1.0 / settings.crawler_rate_limit
    async with httpx.AsyncClient() as client:
        for offset, url in enumerate(chapter_urls):
            chapter_num = starting_num + offset
            try:
                chapters = await scraper._fetch_with_retry(client, url, chapter_num)
                results.extend(chapters)
                for ch in chapters:
                    if on_fetched:
                        on_fetched(ch)
            except Exception as exc:
                logger.error(
                    "Failed to fetch chapter {} | url={} | error={}",
                    chapter_num, url, str(exc),
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
