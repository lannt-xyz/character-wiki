"""main_wiki.py — CLI entry point for the story wiki pipeline.

Usage:
    python main_wiki.py                          -- run full pipeline (resume from last)
    python main_wiki.py --from-batch 100         -- resume from batch_id=100
    python main_wiki.py --max-batches 1          -- process only 1 batch then stop
    python main_wiki.py --dry-run                -- crawl + extract, skip DB write
    python main_wiki.py --export                 -- export wiki to data/wiki/*.json
    python main_wiki.py --stats                  -- print progress stats, then exit
"""

import argparse
import sys
from pathlib import Path

from loguru import logger

from config.settings import settings
from db.database import SQLiteDB
from wiki.orchestrator import run_pipeline
from wiki.validator import export_wiki


def _setup_logging() -> None:
    logger.remove()
    logger.add(sys.stderr, level="INFO")
    logs_dir = Path(settings.logs_dir)
    logs_dir.mkdir(parents=True, exist_ok=True)
    logger.add(str(logs_dir / "wiki.log"), rotation="100 MB", level="DEBUG")


def _print_stats(db: SQLiteDB) -> None:
    s = db.stats()
    total = s["total_batches"]
    merged = s["merged_batches"]
    pct = (merged / total * 100) if total else 0
    print(f"Batches MERGED : {merged}/{total} ({pct:.1f}%)")
    print(f"Characters     : {s['total_characters']}")
    print(f"Snapshots      : {s['total_snapshots']}")


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Story Wiki Pipeline")
    parser.add_argument(
        "--from-batch",
        type=int,
        default=None,
        metavar="CHAPTER_START",
        help="Resume from batch with this chapter_start (deterministic batch_id)",
    )
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        metavar="N",
        help="Stop after processing N batches (useful for testing)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Crawl + extract but do not write wiki data to DB",
    )
    parser.add_argument(
        "--export",
        action="store_true",
        help="Export all characters to data/wiki/*.json",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print pipeline progress stats and exit (no pipeline run)",
    )

    args = parser.parse_args(argv)

    _setup_logging()
    db = SQLiteDB(settings.db_path)

    try:
        if args.stats:
            _print_stats(db)
            return 0

        if args.export:
            n = export_wiki(db, output_dir=str(Path(settings.data_dir) / "wiki"))
            print(f"Exported {n} character files.")
            return 0

        run_pipeline(
            db=db,
            from_batch=args.from_batch,
            dry_run=args.dry_run,
            max_batches=args.max_batches,
        )
        return 0

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
        return 1
    except Exception as exc:
        logger.error("Pipeline failed: {}", exc)
        return 2
    finally:
        db.close()


if __name__ == "__main__":
    sys.exit(main())
