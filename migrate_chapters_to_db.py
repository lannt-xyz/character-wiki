"""migrate_chapters_to_db.py — One-time migration of crawled .txt files into the DB.

Reads every data/chapters/chuong-XXXX.txt and upserts its content into the
chapters table. Chapters already present in DB with content are skipped by
default (use --force to overwrite).

Usage:
    uv run python3 migrate_chapters_to_db.py            # skip chapters that already have content
    uv run python3 migrate_chapters_to_db.py --force    # overwrite existing content
    uv run python3 migrate_chapters_to_db.py --dry-run  # preview only, no writes
"""

import argparse
import re
import sys
from pathlib import Path

from loguru import logger

# ---------------------------------------------------------------------------
# Bootstrap project root on sys.path
# ---------------------------------------------------------------------------
_PROJECT_ROOT = Path(__file__).parent
sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import settings  # noqa: E402
from db.database import SQLiteDB  # noqa: E402

_CHAPTERS_DIR = _PROJECT_ROOT / settings.data_dir / "chapters"
_CHAPTER_RE = re.compile(r"^chuong-(\d{4})\.txt$")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Migrate chapter .txt files to SQLite DB")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing content in DB even if already present",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview only — do not write anything to DB",
    )
    return parser.parse_args()


def main() -> None:
    args = _parse_args()
    db = SQLiteDB(str(_PROJECT_ROOT / settings.db_path))

    txt_files = sorted(_CHAPTERS_DIR.glob("chuong-*.txt"))
    if not txt_files:
        logger.warning("No .txt files found in {}", _CHAPTERS_DIR)
        return

    total = len(txt_files)
    migrated = 0
    skipped = 0
    errors = 0

    logger.info(
        "Starting migration | files={} | force={} | dry_run={}",
        total, args.force, args.dry_run,
    )

    for txt_path in txt_files:
        m = _CHAPTER_RE.match(txt_path.name)
        if not m:
            continue
        chapter_num = int(m.group(1))

        # Skip if content already exists (unless --force)
        if not args.force:
            existing = db.get_chapter_content(chapter_num)
            if existing:
                skipped += 1
                continue

        try:
            content = txt_path.read_text(encoding="utf-8")
        except Exception as exc:
            logger.error("Read error | chapter={} | {}", chapter_num, exc)
            errors += 1
            continue

        if args.dry_run:
            logger.info("[dry-run] Would migrate chapter {}", chapter_num)
            migrated += 1
            continue

        # Check if chapter row already exists to preserve title/url
        row = db._conn.execute(
            "SELECT title, url FROM chapters WHERE chapter_num=?", (chapter_num,)
        ).fetchone()

        db.upsert_chapter(
            chapter_num=chapter_num,
            title=row["title"] if row else f"Chương {chapter_num}",
            url=row["url"] if row else "",
            status="CRAWLED",
            content=content,
        )
        migrated += 1

        if migrated % 100 == 0:
            logger.info("Progress: {}/{}", migrated, total - skipped)

    logger.info(
        "Migration complete | migrated={} | skipped={} | errors={}",
        migrated, skipped, errors,
    )


if __name__ == "__main__":
    main()
