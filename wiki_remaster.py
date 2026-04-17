"""wiki_remaster.py — Artifact Seed & Character Remaster CLI.

Phases:
  0: Backup DB (db/pipeline.db.bak)
  1: Init wiki_remaster_batches (idempotent, copied from wiki_batches)
  2: Build top-20 character markdown (v1 snapshots + chapter excerpts)
     + seed artifact stubs from v1 weapon field (≥3 mentions)
  3: Main extraction loop — sequential through all pending batches
     (LLM extract → merge character deltas v2 → upsert artifacts inline)
  4: Final synthesis — LLM re-synthesize wiki_characters from v2 snapshots (top 20 × 1 call)

Usage:
  uv run python3 wiki_remaster.py                    # full run phase 0→4
  uv run python3 wiki_remaster.py --from-phase 1     # skip backup
  uv run python3 wiki_remaster.py --from-phase 3     # resume extraction loop
  uv run python3 wiki_remaster.py --from-phase 4     # only final synthesis
  uv run python3 wiki_remaster.py --dry-run          # skip all DB writes
"""

import argparse
import json
import re
import shutil
import sys
from collections import Counter
from pathlib import Path
from typing import Optional

from loguru import logger

from config.settings import settings
from crawler.storage import load_chapter_content
from db.database import SQLiteDB
from models.schemas import (
    CharacterPatch,
    ExtractionResult,
    NameEntry,
)
from wiki.extractor import (
    _PASS1_SYSTEM,
    _PASS1_USER_TMPL,
    _build_character_context,
    _normalize,
    _ollama_generate,
)
from wiki.merger import merge_extraction_result, normalize_name, slugify_vi

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------

_PROJECT_ROOT = Path(__file__).parent
_RERUN_DIR = _PROJECT_ROOT / settings.data_dir / "rerun"
_CHAR_INPUT_DIR = _RERUN_DIR / "character_input"
_DB_PATH = _PROJECT_ROOT / settings.db_path
_DB_BACKUP_PATH = _PROJECT_ROOT / "db" / "pipeline.db.bak"

# ---------------------------------------------------------------------------
# Remaster prompts
# ---------------------------------------------------------------------------

_REMASTER_SYSTEM = (
    "Bạn là trợ lý wiki truyện Tiên hiệp/Đô thị huyền huyễn.\n"
    "Từ toàn bộ timeline nhân vật bên dưới, hãy tổng hợp thành 1 profile hoàn chỉnh và chi tiết nhất có thể.\n"
    "Chú trọng: mô tả ngoại hình, trang phục đặc trưng, vũ khí, trạng thái tu vi hiện tại, quan hệ nhân vật.\n"
    "Trả JSON thuần túy, không markdown, không giải thích."
)

_REMASTER_USER_TMPL = """\
Timeline nhân vật:
---
{character_md}
---
Trả về JSON với cấu trúc:
{{
  "character_id": "{character_id}",
  "name": "<tên đầy đủ>",
  "aliases": ["<biệt danh>"],
  "gender": "male|female|unknown",
  "faction": "<môn phái / phe phái hoặc null>",
  "visual_anchor": "<đặc điểm ngoại hình cố định bằng tiếng Anh, đủ để nhận dạng qua các chương>",
  "personality": "<tính cách ngắn gọn>",
  "relations": [
    {{"target_id": "<character_id>", "relation_type": "<loại quan hệ>", "note": "<ghi chú>"}}
  ],
  "peak_snapshot": {{
    "level": "<cảnh giới cao nhất đạt được>",
    "outfit": "<trang phục đặc trưng nhất>",
    "weapon": "<vũ khí chính>",
    "vfx_vibes": "<mô tả hiệu ứng hình ảnh đặc trưng>",
    "visual_importance": <1-10>
  }}
}}"""

_REMASTER_PASS2_SYSTEM = (
    "Bạn là trợ lý xây dựng wiki nhân vật. Nhiệm vụ: trích xuất sự thay đổi trạng thái nhân vật "
    "và các pháp khí từ đoạn truyện mới. Chỉ trả JSON theo schema quy định, không giải thích, không markdown."
)

_REMASTER_PASS2_TMPL = """\
Đoạn truyện mới (Chương {start}-{end}):
---
{text}
---

Danh sách nhân vật liên quan (context):
{character_context}

Danh sách pháp khí cần theo dõi (context):
{artifact_context}

Trả về JSON với cấu trúc:
{{
  "new_characters": [
    {{
      "character": {{
        "character_id": "<slug tiếng Việt không dấu, dùng underscore>",
        "name": "<tên đầy đủ>",
        "name_normalized": "<lowercase, no diacritics>",
        "aliases": [],
        "traits": ["<tính cách>"],
        "relations": [{{"related_name": "...", "description": "...", "chapter_start": {start}}}],
        "visual_anchor": "<đặc điểm ngoại hình cố định hoặc null>"
      }},
      "snapshot": {{
        "chapter_start": {start},
        "is_active": true,
        "level": "<cảnh giới hoặc null>",
        "outfit": "<trang phục hoặc null>",
        "weapon": "<vũ khí hoặc null>",
        "vfx_vibes": "<hiệu ứng hình ảnh hoặc null>",
        "physical_description": "<trạng thái thể chất tạm thời hoặc null>",
        "visual_importance": <1-10>
      }}
    }}
  ],
  "updated_characters": [
    {{
      "character_id": "<id nhân vật cũ>",
      "level": null,
      "outfit": null,
      "weapon": null,
      "vfx_vibes": null,
      "physical_description": null,
      "visual_importance": null,
      "is_active": null,
      "aliases": null
    }}
  ],
  "artifact_updates": [
    {{
      "artifact_id": "<id pháp khí — slug không dấu>",
      "name": "<tên đầy đủ nếu biết>",
      "material": "<chất liệu chính nếu đề cập — null nếu không biết>",
      "visual_anchor": "<mô tả ngoại hình tiếng Anh đủ để render 3D — null nếu chưa rõ>",
      "rarity": "<mức độ quý hiếm nếu đề cập — null nếu không biết>",
      "owner_id": "<character_id đang cầm hoặc null>",
      "normal_state": "<mô tả trạng thái bình thường nếu đề cập — null nếu không biết>",
      "active_state": "<khi phát huy sức mạnh — null nếu không biết>",
      "condition": "intact|active|damaged|evolved",
      "vfx_color": "<màu hiệu ứng chủ đạo hoặc null>"
    }}
  ]
}}

Quy tắc:
- updated_characters: chỉ ghi fields thực sự thay đổi, null nếu không đổi
- artifact_updates: ghi mọi pháp khí xuất hiện trong đoạn này (dù chỉ được nhắc đến)
- artifact material/visual_anchor: quan trọng cho render 3D — cố gắng trích xuất từ mô tả
- Không nhắc đến nhân vật/pháp khí không xuất hiện trong đoạn này"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_batch_text(chapter_start: int, chapter_end: int, db: SQLiteDB) -> str:
    parts = []
    for ch_num in range(chapter_start, chapter_end + 1):
        content = load_chapter_content(ch_num, db)
        if content:
            parts.append(f"\n--- Chương {ch_num} ---\n{content}")
        else:
            logger.warning("Missing chapter content in DB | ch={}", ch_num)
    return "\n".join(parts)


def _pick_representative_chapters(chapter_nums: list[int], max_count: int) -> list[int]:
    """Pick evenly spaced chapters (always includes first + last)."""
    if not chapter_nums:
        return []
    if len(chapter_nums) <= max_count:
        return chapter_nums
    step = (len(chapter_nums) - 1) / (max_count - 1)
    indices = sorted({round(i * step) for i in range(max_count)})
    return [chapter_nums[i] for i in indices if i < len(chapter_nums)]


def _build_artifact_context(db: SQLiteDB, chapter_start: int) -> str:
    """Build compact artifact context JSON for the current chapter position."""
    artifacts = db.get_all_artifacts()
    if not artifacts:
        return "[]"
    context = []
    for art in artifacts:
        snap = db.get_artifact_snapshot_at(art["artifact_id"], chapter_start)
        context.append({
            "artifact_id": art["artifact_id"],
            "name": art["name"],
            "material": art.get("material"),
            "visual_anchor": art.get("visual_anchor"),
            "latest_condition": snap["condition"] if snap else "intact",
            "latest_owner": snap["owner_id"] if snap else None,
        })
    return json.dumps(context, ensure_ascii=False)


def _normalize_artifact_names(db: SQLiteDB, min_mentions: int = 3) -> list[str]:
    """Extract artifact names from v1 weapon field, normalize, return those with >= min_mentions.

    Weapon field may contain comma-separated values like "Kiếm A, Đao B".
    Returns deduplicated list sorted by frequency descending.
    """
    weapon_strings = db.get_v1_weapon_strings()

    counter: Counter = Counter()
    for raw in weapon_strings:
        # Split on comma or dấu chấm phẩy
        parts = re.split(r"[,;]", raw)
        for part in parts:
            name = part.strip()
            if len(name) >= 3:  # skip single-char noise
                counter[name] += 1

    result = [name for name, cnt in counter.most_common() if cnt >= min_mentions]
    logger.info(
        "_normalize_artifact_names | {} names with >= {} mentions",
        len(result),
        min_mentions,
    )
    return result


def _seed_artifact_stubs(db: SQLiteDB, artifact_names: list[str], dry_run: bool = False) -> None:
    """Insert stub wiki_artifacts records for pre-known artifact names.

    These stubs give _build_artifact_context something to return from batch 1.
    Later, _merge_artifact_updates enriches them with material/visual_anchor via LLM.
    """
    if dry_run:
        logger.info("dry_run: skip artifact stub seeding ({} names)", len(artifact_names))
        return
    seeded = 0
    for name in artifact_names:
        artifact_id = slugify_vi(name)
        if not artifact_id:
            continue
        name_norm = normalize_name(name)
        db.upsert_artifact(
            artifact_id=artifact_id,
            name=name,
            name_normalized=name_norm,
        )
        seeded += 1
    logger.info("_seed_artifact_stubs | {} stub records upserted", seeded)


def _build_character_markdown(
    char: dict, snapshots: list[dict], db: SQLiteDB, artifact_names: Optional[list[str]] = None
) -> str:
    """Render character identity + milestone snapshot table + optional artifact seed list."""
    aliases_raw = char.get("aliases_json") or "[]"
    try:
        aliases = json.loads(aliases_raw)
    except Exception:
        aliases = []

    lines = []
    lines.append(f"## {char['name']} [{char['character_id']}]")
    if char.get("visual_anchor"):
        lines.append(f"visual_anchor: {char['visual_anchor']}")
    if aliases:
        lines.append(f"aliases: {', '.join(aliases)}")
    lines.append("")

    lines.append("| Ch start | Level | Outfit | Weapon | VFX vibes | Physical state |")
    lines.append("|---|---|---|---|---|---|")

    def _cell(val: Optional[str]) -> str:
        return val.replace("|", "/") if val else "—"

    prev: Optional[dict] = None
    milestone_fields = ("level", "outfit", "weapon", "vfx_vibes", "physical_description")
    milestone_chapters: list[int] = []
    for snap in snapshots:
        is_milestone = False
        if prev is None:
            is_milestone = True
        else:
            gap = snap["chapter_start"] - prev["chapter_start"]
            if gap >= 20:
                is_milestone = True
            else:
                for f in milestone_fields:
                    if snap.get(f) != prev.get(f):
                        is_milestone = True
                        break
        if is_milestone:
            ch = snap["chapter_start"]
            lines.append(
                f"| {ch} "
                f"| {_cell(snap.get('level'))} "
                f"| {_cell(snap.get('outfit'))} "
                f"| {_cell(snap.get('weapon'))} "
                f"| {_cell(snap.get('vfx_vibes'))} "
                f"| {_cell(snap.get('physical_description'))} |"
            )
            milestone_chapters.append(ch)
            prev = snap

    # Raw chapter excerpts — so LLM can find details v1 missed
    _EXCERPT_CHARS = 500
    selected = _pick_representative_chapters(milestone_chapters, max_count=8)
    if selected:
        lines.append("")
        lines.append("=== Trích đoạn chương nguồn ===")
        for ch_num in selected:
            content = load_chapter_content(ch_num, db)
            if content:
                excerpt = content[:_EXCERPT_CHARS].replace("\n", " ")
                lines.append(f"\n[Chương {ch_num}]: {excerpt}...")

    # Artifact seed list — vocabulary for LLM to recognize known artifacts
    if artifact_names:
        lines.append("")
        lines.append("=== Pháp khí cần theo dõi ===")
        for name in artifact_names:
            lines.append(f"- {name}")

    return "\n".join(lines)


def _backup_db(dry_run: bool = False) -> None:
    if dry_run:
        logger.info("dry_run: skip DB backup")
        return
    shutil.copy(str(_DB_PATH), str(_DB_BACKUP_PATH))
    logger.info("DB backed up to {}", _DB_BACKUP_PATH)


# ---------------------------------------------------------------------------
# Phase 0 — Backup DB
# ---------------------------------------------------------------------------

def phase0_backup(dry_run: bool = False) -> None:
    logger.info("=== Phase 0: Backup DB ===")
    _backup_db(dry_run)


# ---------------------------------------------------------------------------
# Phase 1 — Init wiki_remaster_batches
# ---------------------------------------------------------------------------

def phase1_init_batches(db: SQLiteDB, dry_run: bool = False) -> None:
    logger.info("=== Phase 1: Init wiki_remaster_batches ===")
    if dry_run:
        logger.info("dry_run: skip init_remaster_batches")
        return
    count = db.init_remaster_batches()
    logger.info("Phase 1 done | registered {} remaster batches", count)


# ---------------------------------------------------------------------------
# Phase 2 — Build character markdown + seed artifact stubs
# ---------------------------------------------------------------------------

def phase2_build_input(db: SQLiteDB, dry_run: bool = False) -> list[dict]:
    """Build top-20 character markdown files and seed artifact stubs from v1 data.

    Markdown includes: v1 milestone snapshots + raw chapter excerpts + artifact seed list.
    Artifact stubs are inserted into wiki_artifacts so Phase 3 loop can use them from batch 1.
    """
    logger.info("=== Phase 2: Build character input + seed artifact stubs ===")
    top_chars = db.get_top_characters_by_snapshot(limit=20)
    if not top_chars:
        logger.warning("No characters found in DB. Run main pipeline first.")
        return []

    # Extract artifact names from v1 weapon field (≥3 mentions)
    artifact_names = _normalize_artifact_names(db, min_mentions=3)
    logger.info("Phase 2 | {} artifact seed names (≥3 mentions in v1)", len(artifact_names))

    # Seed stub records into wiki_artifacts so _build_artifact_context works from batch 1
    _seed_artifact_stubs(db, artifact_names, dry_run)

    _CHAR_INPUT_DIR.mkdir(parents=True, exist_ok=True)
    for char in top_chars:
        cid = char["character_id"]
        snapshots_v1 = [
            s for s in db.get_all_snapshots(cid)
            if s.get("extraction_version", 1) == 1
        ]
        md = _build_character_markdown(char, snapshots_v1, db=db, artifact_names=artifact_names)
        out_path = _CHAR_INPUT_DIR / f"{cid}.md"
        if not dry_run:
            out_path.write_text(md, encoding="utf-8")
        logger.info(
            "Phase 2 | {} | {} v1 snaps → {}",
            cid, len(snapshots_v1), out_path.name,
        )

    logger.info(
        "Phase 2 done | {} markdown files | {} artifact stubs",
        len(top_chars), len(artifact_names),
    )
    return top_chars


# ---------------------------------------------------------------------------
# Phase 3 — Main extraction loop (phases 4/5/6 inline)
# ---------------------------------------------------------------------------

def _remaster_pass1(
    batch_text: str, chapter_start: int, chapter_end: int
) -> list[NameEntry]:
    prompt = _PASS1_USER_TMPL.format(
        start=chapter_start, end=chapter_end, text=batch_text[:8000]
    )
    try:
        raw = _ollama_generate(prompt, _PASS1_SYSTEM, settings.wiki_extract_model)
        data = json.loads(raw)
        if isinstance(data, dict):
            data = data.get("characters", data.get("names", []))
        return [NameEntry.model_validate(item) for item in data if isinstance(item, dict)]
    except Exception as exc:
        logger.warning("Pass1 fail | batch={}-{} error={}", chapter_start, chapter_end, exc)
        return []


def _remaster_pass2(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    candidate_chars: list[dict],
    artifact_context: str,
) -> tuple[ExtractionResult, list[dict]]:
    """Run Pass 2: extract character deltas + artifact updates for this batch."""
    context_str = _build_character_context(candidate_chars)
    prompt = _REMASTER_PASS2_TMPL.format(
        start=chapter_start,
        end=chapter_end,
        text=batch_text[:12000],
        character_context=context_str,
        artifact_context=artifact_context,
    )
    raw = _ollama_generate(prompt, _REMASTER_PASS2_SYSTEM, settings.wiki_extract_model)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"Pass2 JSON parse failed | batch={chapter_start}-{chapter_end} | {exc}"
        ) from exc

    updated_raw = data.get("updated_characters", [])
    updated = [CharacterPatch.model_validate(p) for p in updated_raw if isinstance(p, dict)]
    artifact_updates = data.get("artifact_updates", [])

    result = ExtractionResult(
        batch_chapter_start=chapter_start,
        batch_chapter_end=chapter_end,
        new_characters=data.get("new_characters", []),
        updated_characters=updated,
    )
    return result, artifact_updates


def _merge_artifact_updates(
    db: SQLiteDB,
    artifact_updates: list[dict],
    chapter_start: int,
) -> int:
    """Phase 6 inline: upsert artifact metadata + insert artifact snapshots.

    Returns number of artifact snapshots written.
    """
    written = 0
    for upd in artifact_updates:
        raw_id = upd.get("artifact_id", "")
        artifact_id = slugify_vi(raw_id) if raw_id else ""
        if not artifact_id:
            continue

        # Upsert artifact (create stub or enrich existing record with new metadata)
        name = upd.get("name") or raw_id.replace("_", " ").title()
        db.upsert_artifact(
            artifact_id=artifact_id,
            name=name,
            name_normalized=normalize_name(name),
            rarity=upd.get("rarity"),
            material=upd.get("material"),
            visual_anchor=upd.get("visual_anchor"),
        )

        # Insert snapshot only if there's meaningful state to record
        new_owner = upd.get("owner_id")
        new_condition = upd.get("condition", "intact")
        has_state = any([
            new_owner,
            upd.get("normal_state"),
            upd.get("active_state"),
            new_condition != "intact",
            upd.get("vfx_color"),
        ])
        if not has_state:
            continue  # artifact mentioned but no state info — skip snapshot

        prev = db.get_latest_artifact_snapshot(artifact_id)
        is_key = bool(
            prev is None
            or prev.get("owner_id") != new_owner
            or prev.get("condition") != new_condition
        )

        db.add_artifact_snapshot(
            artifact_id=artifact_id,
            chapter_start=chapter_start,
            owner_id=new_owner,
            normal_state=upd.get("normal_state"),
            active_state=upd.get("active_state"),
            condition=new_condition,
            vfx_color=upd.get("vfx_color"),
            is_key_event=is_key,
            extraction_version=2,
        )
        written += 1
    return written


def phase3_extraction_loop(db: SQLiteDB, dry_run: bool = False) -> None:
    """Main extraction loop — sequential through all pending remaster batches.

    Inline phases:
      4: LLM extract (Pass 1 name scan + Pass 2 delta extract with artifact context)
      5: Merge character deltas → wiki_snapshots v2 (append-only)
      6: Upsert artifact metadata + artifact snapshots
    """
    logger.info("=== Phase 3: Remaster extraction loop ===")

    all_characters_fn = db.get_all_characters
    chars_by_names_fn = db.get_characters_by_names

    pending = db.get_remaster_pending_batches()
    total = db.count_remaster_total()
    merged = db.count_remaster_merged()

    if not pending:
        logger.info("Phase 3 | No pending remaster batches.")
        return

    logger.info("Phase 3 | {}/{} batches pending", len(pending), total)
    consecutive_fail = 0

    for batch in pending:
        batch_id = batch["batch_id"]
        chapter_start = batch["chapter_start"]
        chapter_end = batch["chapter_end"]
        pct = (merged / total * 100) if total else 0

        logger.info(
            "[Remaster {}/{}  | {:.1f}%] Ch {}-{}",
            batch_id, total, pct, chapter_start, chapter_end,
        )

        # Load raw text from DB
        batch_text = _load_batch_text(chapter_start, chapter_end, db)
        if not batch_text.strip():
            logger.warning("Empty batch text | batch={} — skip", batch_id)
            if not dry_run:
                db.set_remaster_batch_status(batch_id, "MERGED")
            merged += 1
            continue

        # Build character context with before_chapter cutoff
        def _get_snap(cid: str) -> Optional[dict]:
            return db.get_latest_snapshot(cid, before_chapter=chapter_start)

        artifact_ctx = _build_artifact_context(db, chapter_start)

        try:
            # [Phase 4 inline] Pass 1 — optional name scan
            all_chars = all_characters_fn()
            if len(all_chars) < settings.wiki_context_threshold:
                candidate_chars = all_chars
            else:
                name_entries = _remaster_pass1(batch_text, chapter_start, chapter_end)
                if not name_entries:
                    candidate_chars = all_chars
                else:
                    names_norm = [_normalize(e.name) for e in name_entries] + [
                        _normalize(a) for e in name_entries for a in e.aliases
                    ]
                    candidate_chars = chars_by_names_fn(names_norm)

            for char in candidate_chars:
                char["_latest_snapshot"] = _get_snap(char["character_id"])

            # [Phase 4 inline] Pass 2 — extract deltas + artifacts
            extraction_result, artifact_updates = _remaster_pass2(
                batch_text, chapter_start, chapter_end, candidate_chars, artifact_ctx
            )
            consecutive_fail = 0

        except Exception as exc:
            consecutive_fail += 1
            logger.warning(
                "Extraction fail #{} | batch={}-{} error={}",
                consecutive_fail, chapter_start, chapter_end, exc,
            )
            if consecutive_fail > settings.wiki_max_consecutive_fail:
                logger.error("Too many consecutive failures. Stopping.")
                break
            # Skip broken batch — mark MERGED to not block resume
            if not dry_run:
                db.set_remaster_batch_status(batch_id, "MERGED")
            merged += 1
            continue

        if not dry_run:
            # [Phase 5 inline] Merge character deltas → wiki_snapshots v2
            merge_extraction_result(extraction_result, db, extraction_version=2)

            # [Phase 6 inline] Upsert artifact metadata + snapshots
            art_written = _merge_artifact_updates(db, artifact_updates, chapter_start)

            # Mark MERGED only after all writes succeed (so PENDING is re-tried cleanly on partial failure)
            db.set_remaster_batch_status(batch_id, "MERGED")
            merged += 1
            logger.info(
                "[Remaster {}/{}  | {:.1f}%] MERGED | new={} updated={} art={}",
                batch_id, total, (merged / total * 100),
                len(extraction_result.new_characters),
                len(extraction_result.updated_characters),
                art_written,
            )
        else:
            logger.info(
                "[Remaster {}] dry_run | new={} updated={} art_upd={}",
                batch_id,
                len(extraction_result.new_characters),
                len(extraction_result.updated_characters),
                len(artifact_updates),
            )

    logger.info(
        "Phase 3 done | merged={}/{} remaster batches",
        db.count_remaster_merged(), total,
    )


# ---------------------------------------------------------------------------
# Phase 4 — Final synthesis from v2 snapshots
# ---------------------------------------------------------------------------

def phase4_final_synthesis(db: SQLiteDB, dry_run: bool = False) -> None:
    """Re-synthesize top-20 wiki_characters from v2 snapshots created by Phase 3.

    Phase 3 ran batch-by-batch and updated wiki_characters with delta merges.
    This phase does a single LLM call per character to consolidate v2 data into
    a clean canonical identity — the final and most accurate wiki_characters update.
    """
    logger.info("=== Phase 4: Final synthesis from v2 snapshots ===")
    top_chars = db.get_top_characters_by_snapshot(limit=20)
    if not top_chars:
        logger.warning("No characters found in DB.")
        return

    updated = 0
    skipped = 0
    for char in top_chars:
        cid = char["character_id"]

        # Resume guard: skip if already synthesized in this phase
        if char.get("remaster_version", 1) >= 2:
            logger.info("Phase 4 | {} | already synthesized (remaster_version=2), skip", cid)
            skipped += 1
            continue

        v2_snaps = [
            s for s in db.get_all_snapshots(cid)
            if s.get("extraction_version", 1) == 2
        ]
        if not v2_snaps:
            logger.info("Phase 4 | {} | no v2 snapshots yet (Phase 3 not run?), skip", cid)
            continue

        md = _build_character_markdown(char, v2_snaps, db=db)
        prompt = _REMASTER_USER_TMPL.format(character_md=md, character_id=cid)

        logger.info("Phase 4 | {} | {} v2 snaps → LLM synthesis...", cid, len(v2_snaps))
        try:
            raw = _ollama_generate(prompt, _REMASTER_SYSTEM, settings.wiki_extract_model)
            data = json.loads(raw)
        except Exception as exc:
            logger.error("Phase 4 | {} | LLM/parse fail | error={}", cid, exc)
            continue

        if dry_run:
            logger.info("Phase 4 dry_run | {} | profile received (not saved)", cid)
            continue

        db.update_character_identity(
            character_id=cid,
            visual_anchor=data.get("visual_anchor"),
            faction=data.get("faction"),
            gender=data.get("gender"),
            aliases=data.get("aliases"),
            personality=data.get("personality"),
            remaster_version=2,  # resume marker — skip on restart
        )
        for rel in data.get("relations", []):
            target_id = rel.get("target_id", "")
            if target_id:
                db.add_relation(
                    character_id=cid,
                    related_name=target_id,
                    description=rel.get("note") or rel.get("relation_type"),
                    chapter_start=0,
                )
        updated += 1
        logger.info("Phase 4 | {} | wiki_characters updated from v2 data", cid)

    logger.info(
        "Phase 4 done | {}/{} synthesized | {} skipped (already done)",
        updated, len(top_chars), skipped,
    )




# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def _setup_logging() -> None:
    from pathlib import Path
    log_dir = _PROJECT_ROOT / settings.logs_dir
    log_dir.mkdir(parents=True, exist_ok=True)
    logger.remove()
    logger.add(sys.stderr, level="INFO", colorize=True)
    logger.add(
        str(log_dir / "wiki_remaster.log"),
        level="DEBUG",
        rotation="50 MB",
        retention="7 days",
        encoding="utf-8",
    )


def main() -> None:
    _setup_logging()

    parser = argparse.ArgumentParser(
        description="wiki_remaster.py — Character & Artifact Remaster",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Phases:
  0  Backup DB (always safe to re-run)
  1  Init wiki_remaster_batches (idempotent)
  2  Build top-20 character markdown + seed artifact stubs from v1 data
  3  Main extraction loop (batch-by-batch, sequential, resume-safe)
  4  Final synthesis — re-synthesize wiki_characters from v2 snapshots
        """,
    )
    parser.add_argument(
        "--from-phase",
        type=int,
        default=0,
        metavar="N",
        help="Start from phase N (0-4). Default: 0 (full run)",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip all DB writes and file writes",
    )
    args = parser.parse_args()
    from_phase: int = args.from_phase
    dry_run: bool = args.dry_run

    if from_phase < 0 or from_phase > 4:
        logger.error("--from-phase must be between 0 and 4 (got {})", from_phase)
        sys.exit(1)

    db = SQLiteDB(settings.db_path)
    logger.info(
        "wiki_remaster starting | from_phase={} dry_run={}",
        from_phase,
        dry_run,
    )

    try:
        # Phase 0: Backup DB
        if from_phase <= 0:
            phase0_backup(dry_run)

        # Phase 1: Init remaster batches
        if from_phase <= 1:
            phase1_init_batches(db, dry_run)

        # Phase 2: Build character markdown + seed artifact stubs
        if from_phase <= 2:
            phase2_build_input(db, dry_run)

        # Phase 3: Main extraction loop (phases 4/5/6 inline — sequential, resume-safe)
        if from_phase <= 3:
            phase3_extraction_loop(db, dry_run)

        # Phase 4: Final synthesis from v2 snapshots
        if from_phase <= 4:
            phase4_final_synthesis(db, dry_run)

    except KeyboardInterrupt:
        logger.warning("Interrupted by user — progress saved, safe to resume")
        sys.exit(0)
    except Exception as exc:
        logger.error("wiki_remaster failed | error={}", exc)
        raise
    finally:
        db.close()

    logger.info("wiki_remaster complete.")


if __name__ == "__main__":
    main()
