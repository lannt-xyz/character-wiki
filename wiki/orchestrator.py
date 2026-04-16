"""wiki/orchestrator.py — Batch-by-batch pipeline loop.

State machine per batch: PENDING → CRAWLED → EXTRACTED → MERGED

Features:
- Resume-safe: skip MERGED batches; re-extract CRAWLED (crash recovery)
- Progress & ETA: rolling average over last 10 batches
- Consecutive fail safeguard via ExtractionFatalError
- dry_run: crawl + extract, skip merge + DB writes
"""

import asyncio
import time
from collections import deque
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from config.settings import settings
from crawler.scraper import crawl_chapters
from crawler.storage import load_chapter_content, save_chapter
from db.database import SQLiteDB
from wiki.extractor import ExtractionFatalError, extract_batch
from wiki.merger import merge_extraction_result


def _init_batches(db: SQLiteDB) -> None:
    """Populate wiki_batches table for all batches if not present."""
    batch_size = settings.wiki_batch_size
    total = settings.total_chapters
    version = 1

    existing = {b["batch_id"] for b in db.get_all_batches()}

    for chapter_start in range(1, total + 1, batch_size):
        if chapter_start in existing:
            continue
        chapter_end = min(chapter_start + batch_size - 1, total)
        db.upsert_batch(
            batch_id=chapter_start,
            chapter_start=chapter_start,
            chapter_end=chapter_end,
            status="PENDING",
            extraction_version=version,
        )

    logger.info(
        "Batches initialized | total_batches={}",
        db.count_total_batches(),
    )


def _eta_str(seconds: float) -> str:
    if seconds <= 0:
        return "?"
    h = int(seconds // 3600)
    m = int((seconds % 3600) // 60)
    if h > 0:
        return f"~{h}h{m:02d}m"
    return f"~{m}m"


def run_pipeline(
    db: SQLiteDB,
    from_batch: Optional[int] = None,
    dry_run: bool = False,
    max_batches: Optional[int] = None,
) -> None:
    """Main pipeline loop. Processes batches sequentially.

    Args:
        db: Initialized SQLiteDB instance.
        from_batch: If set, only process batches with batch_id >= from_batch.
        dry_run: If True, crawl + extract but skip writing wiki to DB.
        max_batches: If set, stop after processing this many batches.
    """
    _init_batches(db)

    all_batches = db.get_all_batches()
    total_batches = len(all_batches)

    if from_batch is not None:
        pending = [b for b in all_batches if b["batch_id"] >= from_batch and b["status"] != "MERGED"]
    else:
        pending = [b for b in all_batches if b["status"] != "MERGED"]

    if max_batches is not None:
        pending = pending[:max_batches]

    if not pending:
        logger.info("No pending batches. Pipeline complete.")
        return

    logger.info(
        "Starting pipeline | pending={}/{} batches | dry_run={}",
        len(pending),
        total_batches,
        dry_run,
    )

    consecutive_fail_counter: list[int] = [0]
    recent_batch_times: deque[float] = deque(maxlen=10)
    merged_count = db.count_merged_batches()

    for batch in pending:
        batch_id = batch["batch_id"]
        chapter_start = batch["chapter_start"]
        chapter_end = batch["chapter_end"]
        status = batch["status"]
        extraction_version = batch["extraction_version"]

        batch_start_time = time.monotonic()

        # ----------------------------------------------------------------
        # Progress & ETA
        # ----------------------------------------------------------------
        pct = (merged_count / total_batches * 100) if total_batches else 0
        avg_sec = (sum(recent_batch_times) / len(recent_batch_times)) if recent_batch_times else 0
        remaining = total_batches - merged_count
        eta = _eta_str(avg_sec * remaining)
        logger.info(
            "[Batch {}/{}  | {:.1f}% | ETA {}] Processing Ch {}-{}",
            batch_id,
            total_batches,
            pct,
            eta,
            chapter_start,
            chapter_end,
        )

        try:
            # ------------------------------------------------------------
            # Step 1: Crawl (skip if already CRAWLED/EXTRACTED)
            # ------------------------------------------------------------
            if status == "PENDING":
                chapters = asyncio.run(
                    crawl_chapters(list(range(chapter_start, chapter_end + 1)))
                )
                for ch in chapters:
                    if not dry_run:
                        save_chapter(ch, db)
                    else:
                        logger.debug("dry_run: skip save_chapter for ch {}", ch.chapter_num)

                if not dry_run:
                    db.upsert_batch(
                        batch_id=batch_id,
                        chapter_start=chapter_start,
                        chapter_end=chapter_end,
                        status="CRAWLED",
                        extraction_version=extraction_version,
                    )
                status = "CRAWLED"
                logger.info("Crawled | batch={} chapters={}", batch_id, len(chapters))
                time.sleep(settings.crawler_delay_sec)

            # ------------------------------------------------------------
            # Step 2: Load text for extraction
            # ------------------------------------------------------------
            if status in ("CRAWLED", "EXTRACTED"):
                batch_text = _load_batch_text(chapter_start, chapter_end)
                if not batch_text.strip():
                    logger.warning("Empty batch text | batch={}", batch_id)

                # ------------------------------------------------------------
                # Step 3: Extract
                # ------------------------------------------------------------
                extraction_result = extract_batch(
                    batch_text=batch_text,
                    chapter_start=chapter_start,
                    chapter_end=chapter_end,
                    get_characters_by_names_fn=db.get_characters_by_names,
                    get_all_characters_fn=db.get_all_characters,
                    get_latest_snapshot_fn=db.get_latest_snapshot,
                    consecutive_fail_counter=consecutive_fail_counter,
                )

                if not dry_run:
                    db.upsert_batch(
                        batch_id=batch_id,
                        chapter_start=chapter_start,
                        chapter_end=chapter_end,
                        status="EXTRACTED",
                        extraction_version=extraction_version,
                        extracted_at=datetime.now(timezone.utc),
                    )
                status = "EXTRACTED"

            # ------------------------------------------------------------
            # Step 4: Merge
            # ------------------------------------------------------------
            if status == "EXTRACTED" and not dry_run:
                n_new, n_updated, n_skipped = merge_extraction_result(
                    extraction_result,
                    db,
                    extraction_version=extraction_version,
                )
                db.upsert_batch(
                    batch_id=batch_id,
                    chapter_start=chapter_start,
                    chapter_end=chapter_end,
                    status="MERGED",
                    extraction_version=extraction_version,
                    merged_at=datetime.now(timezone.utc),
                )
                merged_count += 1
                logger.info(
                    "[Batch {}/{}  | {:.1f}%] MERGED | +{} new, {} updated, {} skipped",
                    batch_id,
                    total_batches,
                    (merged_count / total_batches * 100),
                    n_new,
                    n_updated,
                    n_skipped,
                )
            elif dry_run:
                logger.info(
                    "[Batch {}] dry_run: skip merge | new_chars={} updated={}",
                    batch_id,
                    len(extraction_result.new_characters),
                    len(extraction_result.updated_characters),
                )

        except ExtractionFatalError as exc:
            logger.error("FATAL: {} | Stopping pipeline.", exc)
            raise

        except Exception as exc:
            logger.error("Unexpected error | batch={} error={}", batch_id, exc)
            raise

        elapsed = time.monotonic() - batch_start_time
        recent_batch_times.append(elapsed)

    logger.info("Pipeline run complete | merged={}/{}", merged_count, total_batches)


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _load_batch_text(chapter_start: int, chapter_end: int) -> str:
    """Concatenate chapter text files for the batch range."""
    parts = []
    for ch_num in range(chapter_start, chapter_end + 1):
        content = load_chapter_content(ch_num)
        if content:
            parts.append(f"\n--- Chương {ch_num} ---\n{content}")
        else:
            logger.warning("Missing chapter content | ch={}", ch_num)
    return "\n".join(parts)
