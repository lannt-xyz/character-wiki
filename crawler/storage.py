from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from config.settings import settings
from models.schemas import ChapterMeta


def save_chapter(chapter: ChapterMeta, db) -> bool:
    """Save chapter content to file and update DB state. Idempotent.
    Returns True on success, False on error.
    """
    if chapter.status == "ERROR":
        db.set_chapter_status(
            chapter.chapter_num, "ERROR", error_msg=chapter.error_msg
        )
        return False

    chapters_dir = Path(settings.data_dir) / "chapters"
    chapters_dir.mkdir(parents=True, exist_ok=True)

    file_path = chapters_dir / f"chuong-{chapter.chapter_num:04d}.txt"

    if chapter.content:
        file_path.write_text(chapter.content, encoding="utf-8")

    db.upsert_chapter(
        chapter_num=chapter.chapter_num,
        title=chapter.title,
        url=chapter.url,
        file_path=str(file_path),
        status="CRAWLED",
        crawled_at=datetime.now(timezone.utc),
    )

    logger.info("Saved chapter {} | path={}", chapter.chapter_num, file_path)
    return True


def load_chapter_content(chapter_num: int) -> Optional[str]:
    """Load chapter text content from disk. Returns None if not found."""
    file_path = (
        Path(settings.data_dir) / "chapters" / f"chuong-{chapter_num:04d}.txt"
    )
    if not file_path.exists():
        logger.warning("Chapter file not found | chapter={}", chapter_num)
        return None
    return file_path.read_text(encoding="utf-8")
