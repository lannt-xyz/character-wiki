"""wiki/merger.py — Merge ExtractionResult into SQLite DB.

Rules (append-only, NEVER UPDATE):
- New character → upsert_character() + add_snapshot()
- Existing character with change → load latest snapshot as base, apply patch
  with selective inherit, add_snapshot() if enough fields changed
- Existing character with no patch → do nothing
- Alias dedup: merge aliases into wiki_characters.aliases_json
- snapshot_min_change: skip snapshot if changed field count < threshold
"""

import json
import re
import unicodedata
from typing import Optional

from loguru import logger

from config.settings import settings
from db.database import SQLiteDB
from models.schemas import CharacterPatch, ExtractionResult


# Persistent fields: inherit from latest snapshot when patch field is None
_PERSISTENT_FIELDS = ("level", "outfit", "weapon", "vfx_vibes")

# Transient fields: reset to NULL when patch field is None (not inherited)
_TRANSIENT_FIELDS = ("physical_description",)

# All snapshot fields managed by merger
_ALL_SNAPSHOT_FIELDS = _PERSISTENT_FIELDS + _TRANSIENT_FIELDS + ("visual_importance", "is_active")


def normalize_name(name: str) -> str:
    return unicodedata.normalize("NFC", name).lower().strip()


def slugify_vi(text: str) -> str:
    """Vietnamese text → slug: lowercase, no diacritics, spaces/special chars → underscore.

    Example: "Diệp Đại Bảo" → "diep_dai_bao"
    """
    # Replace đ/Đ before NFD decomposition (they don't decompose)
    text = text.replace("đ", "d").replace("Đ", "D")
    # NFD decompose → strip combining (diacritics) marks
    text = unicodedata.normalize("NFD", text)
    text = "".join(c for c in text if unicodedata.category(c) != "Mn")
    text = text.lower().strip()
    # Replace non-alphanumeric runs with underscore
    text = re.sub(r"[^a-z0-9]+", "_", text)
    return text.strip("_")


def merge_extraction_result(
    result: ExtractionResult,
    db: SQLiteDB,
    extraction_version: int = 1,
) -> tuple[int, int, int]:
    """Merge one batch's ExtractionResult into the DB.

    Returns (n_new, n_updated, n_skipped).
    """
    n_new = 0
    n_updated = 0
    n_skipped = 0

    # --- New characters ---
    for entry in result.new_characters:
        char_data = entry.get("character", {})
        snap_data = entry.get("snapshot", {})

        if not char_data or not char_data.get("character_id"):
            logger.warning("Skipping malformed new_character entry | batch={}", result.batch_chapter_start)
            continue

        character_id = slugify_vi(char_data["character_id"])
        name = char_data.get("name", character_id)
        name_norm = normalize_name(char_data.get("name_normalized") or name)
        aliases = char_data.get("aliases") or []
        traits = char_data.get("traits") or []
        visual_anchor = char_data.get("visual_anchor")
        relations = char_data.get("relations") or []

        # Reuse existing character_id if name_normalized already exists
        # (LLM may return different id variants for the same name)
        _existing = db.get_character_by_name(name_norm)
        if _existing and _existing["character_id"] != character_id:
            logger.debug(
                "Reusing existing character_id | old={} new={} name_norm={}",
                _existing["character_id"], character_id, name_norm,
            )
            character_id = _existing["character_id"]

        # upsert identity
        db.upsert_character(
            character_id=character_id,
            name=name,
            name_normalized=name_norm,
            aliases=aliases,
            traits=traits,
            visual_anchor=visual_anchor,
        )

        # add initial snapshot
        db.add_snapshot(
            character_id=character_id,
            chapter_start=snap_data.get("chapter_start", result.batch_chapter_start),
            is_active=snap_data.get("is_active", True),
            level=snap_data.get("level"),
            outfit=snap_data.get("outfit"),
            weapon=snap_data.get("weapon"),
            vfx_vibes=snap_data.get("vfx_vibes"),
            physical_description=snap_data.get("physical_description"),
            visual_importance=snap_data.get("visual_importance", 5),
            extraction_version=extraction_version,
        )

        # append relations
        for rel in relations:
            db.add_relation(
                character_id=character_id,
                related_name=rel.get("related_name", ""),
                description=rel.get("description"),
                chapter_start=rel.get("chapter_start", result.batch_chapter_start),
            )

        n_new += 1
        logger.debug("New character | id={} name={}", character_id, name)

    # --- Updated characters ---
    for patch in result.updated_characters:
        character_id = slugify_vi(patch.character_id)

        char_row = db.get_character_by_id(character_id)
        if not char_row:
            logger.warning("Patch for unknown character | id={}", character_id)
            n_skipped += 1
            continue

        base = db.get_latest_snapshot(character_id)

        # Apply selective inherit
        merged = _apply_patch(base, patch)

        # Merge aliases if provided
        if patch.aliases:
            db.merge_aliases(character_id, patch.aliases)

        # Count how many fields changed vs base
        changes = _count_changes(base, merged)

        if changes < settings.wiki_snapshot_min_change:
            n_skipped += 1
            logger.debug(
                "Skip snapshot (no change) | id={} changes={}", character_id, changes
            )
            continue

        db.add_snapshot(
            character_id=character_id,
            chapter_start=result.batch_chapter_start,
            is_active=merged["is_active"],
            level=merged["level"],
            outfit=merged["outfit"],
            weapon=merged["weapon"],
            vfx_vibes=merged["vfx_vibes"],
            physical_description=merged["physical_description"],
            visual_importance=merged["visual_importance"],
            extraction_version=extraction_version,
        )
        n_updated += 1
        logger.debug("Updated snapshot | id={} changes={}", character_id, changes)

    logger.info(
        "Batch {} | +{} new, {} updated, {} skipped (no change)",
        result.batch_chapter_start,
        n_new,
        n_updated,
        n_skipped,
    )
    return n_new, n_updated, n_skipped


# ---------------------------------------------------------------------------
# Private helpers
# ---------------------------------------------------------------------------

def _apply_patch(base: Optional[dict], patch: CharacterPatch) -> dict:
    """Apply CharacterPatch to base snapshot, returning merged field dict.

    Persistent fields: inherit from base if patch returns None.
    Transient fields: use patch value directly (None = transient ended).
    is_active: inherit unless patch explicitly sets it.
    visual_importance: inherit unless patch sets it.
    """
    merged: dict = {}

    # Defaults when no base exists
    base_level = base["level"] if base else None
    base_outfit = base["outfit"] if base else None
    base_weapon = base["weapon"] if base else None
    base_vfx = base["vfx_vibes"] if base else None
    base_vi = base["visual_importance"] if base else 5
    base_active = bool(base["is_active"]) if base else True

    # Persistent: inherit when patch is None
    merged["level"] = patch.level if patch.level is not None else base_level
    merged["outfit"] = patch.outfit if patch.outfit is not None else base_outfit
    merged["weapon"] = patch.weapon if patch.weapon is not None else base_weapon
    merged["vfx_vibes"] = patch.vfx_vibes if patch.vfx_vibes is not None else base_vfx

    # Transient: no inherit
    merged["physical_description"] = patch.physical_description

    # visual_importance: inherit when patch is None
    merged["visual_importance"] = patch.visual_importance if patch.visual_importance is not None else base_vi

    # is_active: inherit unless explicitly set
    merged["is_active"] = patch.is_active if patch.is_active is not None else base_active

    return merged


def _count_changes(base: Optional[dict], merged: dict) -> int:
    """Count how many snapshot fields differ between base and merged."""
    if base is None:
        return len(_ALL_SNAPSHOT_FIELDS)

    count = 0
    field_map = {
        "level": "level",
        "outfit": "outfit",
        "weapon": "weapon",
        "vfx_vibes": "vfx_vibes",
        "physical_description": "physical_description",
        "visual_importance": "visual_importance",
        "is_active": "is_active",
    }
    for merged_key, base_key in field_map.items():
        base_val = base.get(base_key)
        new_val = merged.get(merged_key)
        # Normalize booleans from SQLite (stored as 0/1)
        if isinstance(base_val, int) and merged_key == "is_active":
            base_val = bool(base_val)
        if base_val != new_val:
            count += 1
    return count
