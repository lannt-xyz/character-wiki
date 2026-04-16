"""wiki/validator.py — Post-merge per-batch sanity checks.

Checks per batch:
1. Every character mentioned in the batch has at least one snapshot
2. No duplicate chapter_start for the same character within the same extraction_version
3. extraction_version is consistent within the batch

Also provides export() to generate data/wiki/{name_normalized}.json files.
"""

import json
import re
from pathlib import Path
from typing import Optional

from loguru import logger

from db.database import SQLiteDB


class ValidationError(Exception):
    pass


def validate_batch(db: SQLiteDB, batch_id: int) -> list[str]:
    """Run sanity checks for a single MERGED batch.

    Returns list of warning strings. Empty list = all good.
    Raises ValidationError for critical failures.
    """
    issues: list[str] = []

    batch = db.get_batch(batch_id)
    if not batch:
        raise ValidationError(f"Batch {batch_id} not found in DB")

    if batch["status"] != "MERGED":
        issues.append(f"Batch {batch_id} is not MERGED (status={batch['status']})")
        return issues

    chapter_start = batch["chapter_start"]
    chapter_end = batch["chapter_end"]
    extraction_version = batch["extraction_version"]

    # Check 1: every character has at least one snapshot
    all_chars = db.get_all_characters()
    for char in all_chars:
        char_id = char["character_id"]
        snaps = db.get_all_snapshots(char_id)
        if not snaps:
            issues.append(f"Character {char_id!r} has no snapshots")

    # Check 2: no duplicate chapter_start per character per version
    conn = db._conn
    rows = conn.execute(
        """
        SELECT character_id, chapter_start, extraction_version, COUNT(*) as cnt
        FROM wiki_snapshots
        GROUP BY character_id, chapter_start, extraction_version
        HAVING cnt > 1
        """
    ).fetchall()
    for row in rows:
        issues.append(
            f"Duplicate snapshot | char={row['character_id']} "
            f"chapter_start={row['chapter_start']} version={row['extraction_version']} count={row['cnt']}"
        )

    # Check 3: extraction_version consistency in snapshots touched by this batch
    snaps_in_batch = conn.execute(
        """
        SELECT DISTINCT extraction_version
        FROM wiki_snapshots
        WHERE chapter_start BETWEEN ? AND ?
        """,
        (chapter_start, chapter_end),
    ).fetchall()
    versions = {r["extraction_version"] for r in snaps_in_batch}
    if len(versions) > 1:
        issues.append(
            f"Batch {batch_id} has mixed extraction_versions in snapshots: {versions}"
        )

    if issues:
        for issue in issues:
            logger.warning("Validation issue | {}", issue)
    else:
        logger.info("Batch {} validation OK", batch_id)

    return issues


def export_wiki(db: SQLiteDB, output_dir: str = "data/wiki") -> int:
    """Export all characters to {output_dir}/{name_normalized}.json.

    Returns number of files written.
    """
    out_path = Path(output_dir)
    out_path.mkdir(parents=True, exist_ok=True)

    all_chars = db.get_all_characters()
    written = 0

    for char in all_chars:
        char_id = char["character_id"]
        all_snaps = db.get_all_snapshots(char_id)
        relations = db.get_relations(char_id)

        # Build snapshot list with derived chapter_end
        snap_dicts = []
        for i, snap in enumerate(all_snaps):
            next_snap = all_snaps[i + 1] if i + 1 < len(all_snaps) else None
            chapter_end = (next_snap["chapter_start"] - 1) if next_snap else None
            snap_dicts.append({
                "chapter_start": snap["chapter_start"],
                "chapter_end": chapter_end,
                "is_active": bool(snap["is_active"]),
                "level": snap["level"],
                "outfit": snap["outfit"],
                "weapon": snap["weapon"],
                "vfx_vibes": snap["vfx_vibes"],
                "physical_description": snap["physical_description"],
                "visual_importance": snap["visual_importance"],
                "extraction_version": snap["extraction_version"],
            })

        export_data = {
            "character_id": char_id,
            "name": char["name"],
            "aliases": json.loads(char["aliases_json"]),
            "traits": json.loads(char["traits_json"]),
            "visual_anchor": char["visual_anchor"],
            "relations": [
                {
                    "related_name": r["related_name"],
                    "description": r["description"],
                    "chapter_start": r["chapter_start"],
                }
                for r in relations
            ],
            "snapshots": snap_dicts,
        }

        filename = char["character_id"] + ".json"
        (out_path / filename).write_text(
            json.dumps(export_data, ensure_ascii=False, indent=2),
            encoding="utf-8",
        )
        written += 1

    logger.info("Exported {} character files to {}", written, output_dir)
    return written


def _safe_filename(name_normalized: str) -> str:
    """Convert name_normalized to a filesystem-safe filename (spaces → hyphens)."""
    name = name_normalized.replace(" ", "-")
    name = re.sub(r"[^\w\-]", "", name)
    return name or "unknown"
