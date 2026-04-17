from datetime import datetime, timezone
from typing import Optional

from loguru import logger

from models.schemas import ChapterMeta


def save_chapter(chapter: ChapterMeta, db) -> bool:
    """Save chapter content to DB. Idempotent.
    Returns True on success, False on error.
    """
    if chapter.status == "ERROR":
        db.set_chapter_status(
            chapter.chapter_num, "ERROR", error_msg=chapter.error_msg
        )
        return False

    db.upsert_chapter(
        chapter_num=chapter.chapter_num,
        title=chapter.title,
        url=chapter.url,
        file_path="",
        status="CRAWLED",
        crawled_at=datetime.now(timezone.utc),
        content=chapter.content,
    )

    logger.info("Saved chapter {} to DB", chapter.chapter_num)
    return True


def load_chapter_content(chapter_num: int, db) -> Optional[str]:
    """Load chapter text content from DB. Returns None if not found."""
    content = db.get_chapter_content(chapter_num)
    if content is None:
        logger.warning("Chapter content not found in DB | chapter={}", chapter_num)
    return content
