"""wiki_remaster.py — Artifact Seed & Character Remaster CLI.

Phases:
  1: Init wiki_remaster_batches (separate table, idempotent)
  2: Build character markdown input files (top 20 by snapshot count)
  3: LLM extract artifacts per character → data/rerun/wiki_seed.json
  4: Remaster top 20 characters → UPDATE wiki_characters + INSERT wiki_snapshots v2
  5: Seed artifacts into DB (wiki_artifacts + wiki_artifact_snapshots)
  6: Verify setup (schema + artifact count)
  7: Remaster extraction loop (all batches, extraction_version=2)

Usage:
  uv run python3 wiki_remaster.py                    # full run phase 1→7
  uv run python3 wiki_remaster.py --from-phase 3     # resume from phase 3
  uv run python3 wiki_remaster.py --from-phase 7     # only run extraction loop
  uv run python3 wiki_remaster.py --dry-run          # skip all DB writes
"""

import argparse
import json
import shutil
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from loguru import logger

from config.settings import settings
from crawler.storage import load_chapter_content
from db.database import SQLiteDB
from models.schemas import (
    ArtifactSeedEntry,
    CharacterPatch,
    ExtractionResult,
    NameEntry,
    WikiSeedCharacter,
)
from wiki.extractor import (
    _PASS1_SYSTEM,
    _PASS1_USER_TMPL,
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
_WIKI_SEED_PATH = _RERUN_DIR / "wiki_seed.json"
_DB_PATH = _PROJECT_ROOT / settings.db_path
_DB_BACKUP_PATH = _PROJECT_ROOT / "db" / "pipeline.db.bak"

# ---------------------------------------------------------------------------
# Remaster prompts
# ---------------------------------------------------------------------------

_ARTIFACT_SYSTEM = (
    "Bạn là trợ lý phân tích truyện Tiên hiệp/Đô thị huyền huyễn.\n"
    "Từ timeline nhân vật, trích xuất các PHÁP KHÍ QUAN TRỌNG mà nhân vật sở hữu hoặc thường xuyên sử dụng.\n"
    "Chỉ lấy pháp khí có sức mạnh đặc biệt hoặc xuất hiện thường xuyên. Bỏ qua đồ dùng 1-2 lần.\n"
    "Với mỗi pháp khí, đặc biệt chú ý trích xuất trường 'material' (chất liệu: đồng, ngọc, gỗ đào, xương yêu...) "
    "— đây là yếu tố quyết định texture khi render 3D.\n"
    "Trả JSON thuần túy, không markdown, không giải thích."
)

_ARTIFACT_USER_TMPL = """\
Timeline nhân vật:
---
{character_md}
---
Trả về JSON với cấu trúc:
{{
  "character_id": "{character_id}",
  "artifacts": [
    {{
      "artifact_id": "<slug không dấu, underscore>",
      "name": "<tên đầy đủ>",
      "rarity": "<mức độ quý hiếm hoặc null>",
      "material": "<chất liệu chính hoặc null>",
      "visual_anchor": "<mô tả ngoại hình bằng tiếng Anh, đủ để render 3D hoặc null>",
      "snapshots": [
        {{
          "chapter_start": <int>,
          "owner_id": "<character_id hoặc null>",
          "normal_state": "<trạng thái bình thường>",
          "active_state": "<khi phát huy sức mạnh hoặc null>",
          "condition": "intact|active|damaged|evolved"
        }}
      ]
    }}
  ]
}}"""

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
  "faction": "<môn phái / phe phái hiện tại hoặc null>",
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
    "từ đoạn truyện mới. Chỉ trả JSON theo schema quy định, không giải thích, không markdown."
)

_REMASTER_PASS2_TMPL = """\
Đoạn truyện mới (Chương {start}-{end}):
---
{text}
---

Danh sách nhân vật liên quan (context):
{character_context}

Danh sách pháp khí đã biết (context):
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
      "artifact_id": "<id pháp khí>",
      "owner_id": "<character_id đang cầm hoặc null>",
      "condition": "intact|active|damaged|evolved",
      "vfx_color": "<hex hoặc tên màu hoặc null>",
      "note": "<mô tả thay đổi hoặc null>"
    }}
  ]
}}

Quy tắc:
- Persistent fields (level, outfit, weapon, vfx_vibes): null nếu không thay đổi
- Transient (physical_description): null nếu trạng thái kết thúc hoặc không nhắc
- artifact_updates: chỉ ghi khi owner_id hoặc condition thay đổi; bỏ qua pháp khí không xuất hiện
- Không nhắc đến nhân vật không xuất hiện trong đoạn này"""


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _load_wiki_seed() -> dict[str, WikiSeedCharacter]:
    """Load wiki_seed.json → dict[character_id, WikiSeedCharacter]."""
    if not _WIKI_SEED_PATH.exists():
        return {}
    try:
        data = json.loads(_WIKI_SEED_PATH.read_text(encoding="utf-8"))
        result = {}
        for entry in data:
            obj = WikiSeedCharacter.model_validate(entry)
            result[obj.character_id] = obj
        return result
    except Exception as exc:
        logger.warning("Failed to load wiki_seed.json | error={}", exc)
        return {}


def _save_wiki_seed(seed: dict[str, WikiSeedCharacter]) -> None:
    _WIKI_SEED_PATH.parent.mkdir(parents=True, exist_ok=True)
    data = [v.model_dump() for v in seed.values()]
    _WIKI_SEED_PATH.write_text(
        json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8"
    )


def _load_batch_text(chapter_start: int, chapter_end: int) -> str:
    parts = []
    for ch_num in range(chapter_start, chapter_end + 1):
        content = load_chapter_content(ch_num)
        if content:
            parts.append(f"\n--- Chương {ch_num} ---\n{content}")
        else:
            logger.warning("Missing chapter file | ch={}", ch_num)
    return "\n".join(parts)


def _build_artifact_context(db: SQLiteDB, chapter_start: int) -> str:
    """Build compact artifact context JSON for current chapter range."""
    artifacts = db.get_all_artifacts()
    if not artifacts:
        return "[]"
    context = []
    for art in artifacts:
        snap = db.get_artifact_snapshot_at(art["artifact_id"], chapter_start)
        context.append({
            "artifact_id": art["artifact_id"],
            "name": art["name"],
            "visual_anchor": art.get("visual_anchor"),
            "latest_condition": snap["condition"] if snap else "intact",
            "latest_owner": snap["owner_id"] if snap else None,
        })
    return json.dumps(context, ensure_ascii=False)


def _pick_representative_chapters(chapter_nums: list[int], max_count: int) -> list[int]:
    """Pick evenly spaced chapters from a list (always includes first + last)."""
    if not chapter_nums:
        return []
    if len(chapter_nums) <= max_count:
        return chapter_nums
    step = (len(chapter_nums) - 1) / (max_count - 1)
    indices = sorted({round(i * step) for i in range(max_count)})
    return [chapter_nums[i] for i in indices if i < len(chapter_nums)]


def _backup_db(dry_run: bool = False) -> None:
    """Backup pipeline.db before first write phase."""
    if dry_run:
        logger.info("dry_run: skip DB backup")
        return
    shutil.copy(str(_DB_PATH), str(_DB_BACKUP_PATH))
    logger.info("DB backed up to {}", _DB_BACKUP_PATH)


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
# Phase 2 — Build character markdown input
# ---------------------------------------------------------------------------

def _build_character_markdown(char: dict, snapshots: list[dict]) -> str:
    """Render top-level identity + deduplicated snapshot table as markdown."""
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

    # Table header
    lines.append("| Ch start | Level | Outfit | Weapon | VFX vibes | Physical state |")
    lines.append("|---|---|---|---|---|---|")

    def _cell(val: Optional[str]) -> str:
        return val.replace("|", "/") if val else "—"

    # Dedup: only keep milestone rows (field changed vs previous, or ≥20ch gap)
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

    # Append raw chapter text excerpts so LLM can find details v1 missed
    _EXCERPT_CHARS = 500
    selected = _pick_representative_chapters(milestone_chapters, max_count=8)
    if selected:
        lines.append("")
        lines.append("=== Trích đoạn chương nguồn ===")
        for ch_num in selected:
            content = load_chapter_content(ch_num)
            if content:
                excerpt = content[:_EXCERPT_CHARS].replace("\n", " ")
                lines.append(f"\n[Chương {ch_num}]: {excerpt}...")

    return "\n".join(lines)


def phase2_build_character_input(db: SQLiteDB, dry_run: bool = False) -> list[dict]:
    logger.info("=== Phase 2: Build character markdown input ===")
    top_chars = db.get_top_characters_by_snapshot(limit=20)
    if not top_chars:
        logger.warning("No characters found in DB. Run main pipeline first.")
        return []

    logger.info("Top {} characters by snapshot count", len(top_chars))
    _CHAR_INPUT_DIR.mkdir(parents=True, exist_ok=True)

    for char in top_chars:
        snapshots = db.get_all_snapshots(char["character_id"])
        # Only v1 snapshots for input (before remaster)
        snapshots_v1 = [s for s in snapshots if s.get("extraction_version", 1) == 1]
        md = _build_character_markdown(char, snapshots_v1)
        out_path = _CHAR_INPUT_DIR / f"{char['character_id']}.md"
        if not dry_run:
            out_path.write_text(md, encoding="utf-8")
        logger.info(
            "Phase 2 | {} | {} v1 snapshots → {}",
            char["character_id"],
            len(snapshots_v1),
            out_path.name,
        )

    logger.info("Phase 2 done | {} files written to {}", len(top_chars), _CHAR_INPUT_DIR)
    return top_chars


# ---------------------------------------------------------------------------
# Phase 3 — LLM extract artifacts → wiki_seed.json
# ---------------------------------------------------------------------------

def phase3_extract_artifacts(
    top_chars: list[dict], dry_run: bool = False
) -> dict[str, WikiSeedCharacter]:
    logger.info("=== Phase 3: LLM extract artifacts ===")
    seed = _load_wiki_seed()
    logger.info("Loaded {} existing entries from wiki_seed.json", len(seed))

    for char in top_chars:
        cid = char["character_id"]
        if cid in seed:
            logger.info("Phase 3 | {} | already in wiki_seed.json, skip", cid)
            continue

        md_path = _CHAR_INPUT_DIR / f"{cid}.md"
        if not md_path.exists():
            logger.warning("Phase 3 | {} | missing markdown file, skip", cid)
            continue

        character_md = md_path.read_text(encoding="utf-8")
        prompt = _ARTIFACT_USER_TMPL.format(
            character_md=character_md,
            character_id=cid,
        )

        logger.info("Phase 3 | {} | calling LLM...", cid)
        try:
            raw = _ollama_generate(prompt, _ARTIFACT_SYSTEM, settings.wiki_extract_model)
            data = json.loads(raw)
            obj = WikiSeedCharacter.model_validate(data)
        except Exception as exc:
            logger.error("Phase 3 | {} | LLM/parse fail | error={}", cid, exc)
            continue

        if not dry_run:
            seed[cid] = obj
            _save_wiki_seed(seed)
            logger.info(
                "Phase 3 | {} | {} artifacts extracted and saved",
                cid,
                len(obj.artifacts),
            )
        else:
            logger.info(
                "Phase 3 dry_run | {} | {} artifacts (not saved)",
                cid,
                len(obj.artifacts),
            )

    logger.info("Phase 3 done | {} total entries in wiki_seed.json", len(seed))
    return seed


# ---------------------------------------------------------------------------
# Phase 4 — Remaster top 20 characters
# ---------------------------------------------------------------------------

def phase4_remaster_characters(
    db: SQLiteDB, top_chars: list[dict], dry_run: bool = False
) -> None:
    logger.info("=== Phase 4: Remaster top 20 characters ===")

    for char in top_chars:
        cid = char["character_id"]

        # Resumable: skip if v2 snapshot already exists
        existing_v2 = db._conn.execute(
            "SELECT 1 FROM wiki_snapshots WHERE character_id=? AND extraction_version=2 LIMIT 1",
            (cid,),
        ).fetchone()
        if existing_v2:
            logger.info("Phase 4 | {} | v2 snapshot already exists, skip", cid)
            continue

        md_path = _CHAR_INPUT_DIR / f"{cid}.md"
        if not md_path.exists():
            logger.warning("Phase 4 | {} | missing markdown file, skip", cid)
            continue

        character_md = md_path.read_text(encoding="utf-8")
        prompt = _REMASTER_USER_TMPL.format(
            character_md=character_md,
            character_id=cid,
        )

        logger.info("Phase 4 | {} | calling LLM for canonical profile...", cid)
        try:
            raw = _ollama_generate(prompt, _REMASTER_SYSTEM, settings.wiki_extract_model)
            data = json.loads(raw)
        except Exception as exc:
            logger.error("Phase 4 | {} | LLM/parse fail | error={}", cid, exc)
            continue

        if dry_run:
            logger.info("Phase 4 dry_run | {} | profile received (not saved)", cid)
            continue

        # Step 1: UPDATE wiki_characters with enriched identity fields
        db.update_character_identity(
            character_id=cid,
            visual_anchor=data.get("visual_anchor"),
            faction=data.get("faction"),
            gender=data.get("gender"),
            aliases=data.get("aliases"),
            personality=data.get("personality"),
        )
        # Upsert relations
        for rel in data.get("relations", []):
            target_id = rel.get("target_id", "")
            if target_id:
                db.add_relation(
                    character_id=cid,
                    related_name=target_id,
                    description=rel.get("note") or rel.get("relation_type"),
                    chapter_start=0,  # synthetic — covers full arc
                )

        # Step 2: INSERT wiki_snapshots v2 (canonical peak state)
        peak = data.get("peak_snapshot", {})
        # chapter_start = max chapter_start seen in v1 snapshots
        row = db._conn.execute(
            "SELECT MAX(chapter_start) as mx FROM wiki_snapshots WHERE character_id=? AND extraction_version=1",
            (cid,),
        ).fetchone()
        max_chapter = row["mx"] if row and row["mx"] else 1

        db.add_snapshot(
            character_id=cid,
            chapter_start=max_chapter,
            is_active=True,
            level=peak.get("level"),
            outfit=peak.get("outfit"),
            weapon=peak.get("weapon"),
            vfx_vibes=peak.get("vfx_vibes"),
            physical_description=None,
            visual_importance=peak.get("visual_importance", 8),
            extraction_version=2,
        )
        logger.info("Phase 4 | {} | identity updated + v2 snapshot inserted (ch {})", cid, max_chapter)

    logger.info("Phase 4 done")


# ---------------------------------------------------------------------------
# Phase 5 — Seed artifacts into DB
# ---------------------------------------------------------------------------

def phase5_seed_artifacts(
    db: SQLiteDB, dry_run: bool = False
) -> None:
    logger.info("=== Phase 5: Seed artifacts into DB ===")

    # 5.1 Load wiki_seed.json
    seed = _load_wiki_seed()
    if not seed:
        logger.warning("wiki_seed.json empty or missing — run Phase 3 first")
        return

    total_artifacts = 0
    total_snapshots = 0

    for cid, char_seed in seed.items():
        for art in char_seed.artifacts:
            artifact_id = slugify_vi(art.artifact_id)
            name_norm = normalize_name(art.name)

            if not dry_run:
                db.upsert_artifact(
                    artifact_id=artifact_id,
                    name=art.name,
                    name_normalized=name_norm,
                    rarity=art.rarity,
                    material=art.material,
                    visual_anchor=art.visual_anchor,
                )
                # Insert snapshots (skip if same artifact+chapter already exists)
                for snap in art.snapshots:
                    existing = db._conn.execute(
                        "SELECT 1 FROM wiki_artifact_snapshots WHERE artifact_id=? AND chapter_start=? LIMIT 1",
                        (artifact_id, snap.chapter_start),
                    ).fetchone()
                    if not existing:
                        db.add_artifact_snapshot(
                            artifact_id=artifact_id,
                            chapter_start=snap.chapter_start,
                            owner_id=snap.owner_id,
                            normal_state=snap.normal_state,
                            active_state=snap.active_state,
                            condition=snap.condition,
                            extraction_version=2,
                        )
                        total_snapshots += 1
            total_artifacts += 1

    logger.info(
        "Phase 5 done | {} artifacts, {} snapshots seeded",
        total_artifacts,
        total_snapshots,
    )


# ---------------------------------------------------------------------------
# Phase 6 — Verify setup
# ---------------------------------------------------------------------------

def phase6_verify(db: SQLiteDB) -> None:
    logger.info("=== Phase 6: Verify setup ===")
    artifact_count = db._scalar("SELECT COUNT(*) FROM wiki_artifacts")
    remaster_batch_count = db._scalar("SELECT COUNT(*) FROM wiki_remaster_batches")
    # Check columns exist
    cols = {
        row[1]
        for row in db._conn.execute("PRAGMA table_info(wiki_characters)").fetchall()
    }
    missing = {"faction", "gender", "personality"} - cols
    if missing:
        logger.error("Phase 6 | Missing columns in wiki_characters: {}", missing)
        sys.exit(1)
    logger.info(
        "Phase 6 | artifacts={} | remaster_batches={} | identity_cols=OK",
        artifact_count,
        remaster_batch_count,
    )


# ---------------------------------------------------------------------------
# Phase 7 — Remaster extraction loop
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
        logger.warning("Remaster Pass 1 fail | batch={}-{} error={}", chapter_start, chapter_end, exc)
        return []


def _remaster_pass2(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    candidate_chars: list[dict],
    artifact_context: str,
) -> tuple[ExtractionResult, list[dict]]:
    """Run Pass 2 with artifact context. Returns (ExtractionResult, artifact_updates)."""
    from wiki.extractor import _build_character_context
    context_str = _build_character_context(candidate_chars)
    prompt = _REMASTER_PASS2_TMPL.format(
        start=chapter_start,
        end=chapter_end,
        text=batch_text[:12000],
        character_context=context_str,
        artifact_context=artifact_context,
    )
    raw = _ollama_generate(prompt, _REMASTER_PASS2_SYSTEM, settings.wiki_extract_model)
    data = json.loads(raw)

    new_chars = data.get("new_characters", [])
    updated_raw = data.get("updated_characters", [])
    updated = [CharacterPatch.model_validate(p) for p in updated_raw if isinstance(p, dict)]
    artifact_updates = data.get("artifact_updates", [])

    result = ExtractionResult(
        batch_chapter_start=chapter_start,
        batch_chapter_end=chapter_end,
        new_characters=new_chars,
        updated_characters=updated,
    )
    return result, artifact_updates


def _merge_artifact_updates(
    db: SQLiteDB,
    artifact_updates: list[dict],
    chapter_start: int,
) -> None:
    """Insert wiki_artifact_snapshots for changed artifacts."""
    for upd in artifact_updates:
        artifact_id = slugify_vi(upd.get("artifact_id", ""))
        if not artifact_id:
            continue
        art = db._conn.execute(
            "SELECT 1 FROM wiki_artifacts WHERE artifact_id=?", (artifact_id,)
        ).fetchone()
        if not art:
            logger.debug("Artifact not in DB, skip | id={}", artifact_id)
            continue

        prev = db.get_latest_artifact_snapshot(artifact_id)
        new_owner = upd.get("owner_id")
        new_condition = upd.get("condition", "intact")
        is_key = bool(
            prev is None
            or prev.get("owner_id") != new_owner
            or prev.get("condition") != new_condition
        )
        if not is_key:
            continue  # no change, skip

        db.add_artifact_snapshot(
            artifact_id=artifact_id,
            chapter_start=chapter_start,
            owner_id=new_owner,
            normal_state=None,
            active_state=None,
            condition=new_condition,
            vfx_color=upd.get("vfx_color"),
            is_key_event=is_key,
            extraction_version=2,
        )


def phase7_extraction_loop(db: SQLiteDB, dry_run: bool = False) -> None:
    logger.info("=== Phase 7: Remaster extraction loop ===")

    all_characters = db.get_all_characters
    chars_by_names = db.get_characters_by_names

    pending = db.get_remaster_pending_batches()
    total = db.count_remaster_total()
    merged = db.count_remaster_merged()

    if not pending:
        logger.info("Phase 7 | No pending remaster batches.")
        return

    logger.info(
        "Phase 7 | {}/{} batches pending",
        len(pending),
        total,
    )

    consecutive_fail = [0]

    for batch in pending:
        batch_id = batch["batch_id"]
        chapter_start = batch["chapter_start"]
        chapter_end = batch["chapter_end"]

        pct = (merged / total * 100) if total else 0
        logger.info(
            "[Remaster {}/{}  | {:.1f}%] Ch {}-{}",
            batch_id,
            total,
            pct,
            chapter_start,
            chapter_end,
        )

        # Load text
        batch_text = _load_batch_text(chapter_start, chapter_end)
        if not batch_text.strip():
            logger.warning("Empty batch text | batch={}", batch_id)
            db.set_remaster_batch_status(batch_id, "MERGED")
            merged += 1
            continue

        # Build context: use before_chapter cutoff
        def _get_snap(cid: str) -> Optional[dict]:
            return db.get_latest_snapshot(cid, before_chapter=chapter_start)

        artifact_ctx = _build_artifact_context(db, chapter_start)

        # Pass 1 — name scan
        try:
            all_chars = all_characters()
            wiki_size = len(all_chars)

            if wiki_size < settings.wiki_context_threshold:
                candidate_chars = all_chars
            else:
                name_entries = _remaster_pass1(batch_text, chapter_start, chapter_end)
                if not name_entries:
                    candidate_chars = all_chars
                else:
                    names_norm = [_normalize(e.name) for e in name_entries] + [
                        _normalize(a) for e in name_entries for a in e.aliases
                    ]
                    candidate_chars = chars_by_names(names_norm)

            for char in candidate_chars:
                char["_latest_snapshot"] = _get_snap(char["character_id"])

            # Pass 2 — delta extract
            extraction_result, artifact_updates = _remaster_pass2(
                batch_text, chapter_start, chapter_end, candidate_chars, artifact_ctx
            )
            consecutive_fail[0] = 0

        except Exception as exc:
            consecutive_fail[0] += 1
            logger.warning(
                "Remaster extraction fail #{} | batch={}-{} error={}",
                consecutive_fail[0],
                chapter_start,
                chapter_end,
                exc,
            )
            if consecutive_fail[0] > settings.wiki_max_consecutive_fail:
                logger.error("Too many consecutive failures. Stopping.")
                break
            db.set_remaster_batch_status(batch_id, "MERGED")  # skip broken batch
            merged += 1
            continue

        if not dry_run:
            db.set_remaster_batch_status(batch_id, "EXTRACTED")

            # Merge character updates (v2)
            merge_extraction_result(extraction_result, db, extraction_version=2)

            # Merge artifact updates
            _merge_artifact_updates(db, artifact_updates, chapter_start)

            db.set_remaster_batch_status(batch_id, "MERGED")
            merged += 1
            logger.info(
                "[Remaster {}/{}  | {:.1f}%] MERGED | new={} updated={} art_upd={}",
                batch_id,
                total,
                (merged / total * 100),
                len(extraction_result.new_characters),
                len(extraction_result.updated_characters),
                len(artifact_updates),
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
        "Phase 7 done | merged={}/{} remaster batches",
        db.count_remaster_merged(),
        total,
    )


# ---------------------------------------------------------------------------
# Phase 8 — Final character remaster from v2 snapshots
# ---------------------------------------------------------------------------

def phase8_final_remaster(db: SQLiteDB, dry_run: bool = False) -> None:
    """Re-synthesize top 20 character profiles from v2 snapshots (created by Phase 7).

    Phase 4 ran BEFORE Phase 7 — it synthesized wiki_characters from v1 snapshot data.
    This phase runs AFTER Phase 7 to produce the final, accurate wiki_characters update
    using data the LLM actually extracted from re-reading the raw chapter text.
    """
    logger.info("=== Phase 8: Final character remaster from v2 snapshots ===")
    top_chars = db.get_top_characters_by_snapshot(limit=20)
    if not top_chars:
        logger.warning("No characters found in DB.")
        return

    updated = 0
    for char in top_chars:
        cid = char["character_id"]

        # Only proceed if Phase 7 created v2 snapshots for this character
        v2_snaps = [
            s for s in db.get_all_snapshots(cid)
            if s.get("extraction_version", 1) == 2
        ]
        if not v2_snaps:
            logger.info("Phase 8 | {} | no v2 snapshots yet (run Phase 7 first), skip", cid)
            continue

        # Build markdown from v2 snapshots (includes chapter text excerpts)
        md = _build_character_markdown(char, v2_snaps)
        prompt = _REMASTER_USER_TMPL.format(
            character_md=md,
            character_id=cid,
        )

        logger.info("Phase 8 | {} | {} v2 snapshots → LLM resynthesis...", cid, len(v2_snaps))
        try:
            raw = _ollama_generate(prompt, _REMASTER_SYSTEM, settings.wiki_extract_model)
            data = json.loads(raw)
        except Exception as exc:
            logger.error("Phase 8 | {} | LLM/parse fail | error={}", cid, exc)
            continue

        if dry_run:
            logger.info("Phase 8 dry_run | {} | profile received (not saved)", cid)
            continue

        db.update_character_identity(
            character_id=cid,
            visual_anchor=data.get("visual_anchor"),
            faction=data.get("faction"),
            gender=data.get("gender"),
            aliases=data.get("aliases"),
            personality=data.get("personality"),
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
        logger.info("Phase 8 | {} | wiki_characters updated from v2 data", cid)

    logger.info(
        "Phase 8 done | {}/{} characters updated from v2 snapshots",
        updated,
        len(top_chars),
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

    parser = argparse.ArgumentParser(description="wiki_remaster.py — Character & Artifact Remaster")
    parser.add_argument(
        "--from-phase",
        type=int,
        default=1,
        metavar="N",
        help="Start from phase N (1-8). Default: 1",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Skip all DB writes and file writes",
    )
    args = parser.parse_args()
    from_phase: int = args.from_phase
    dry_run: bool = args.dry_run

    if from_phase < 1 or from_phase > 8:
        logger.error("--from-phase must be between 1 and 8")
        sys.exit(1)

    db = SQLiteDB(settings.db_path)
    logger.info(
        "wiki_remaster starting | from_phase={} dry_run={}",
        from_phase,
        dry_run,
    )

    try:
        if from_phase <= 1:
            phase1_init_batches(db, dry_run)

        top_chars: list[dict] = []
        if from_phase <= 2:
            top_chars = phase2_build_character_input(db, dry_run)
        else:
            # Need top_chars for phases 3 & 4 even when resuming
            top_chars = db.get_top_characters_by_snapshot(limit=20)

        if from_phase <= 3:
            phase3_extract_artifacts(top_chars, dry_run)

        # Backup DB before first write (Phase 4+)
        if from_phase <= 4:
            _backup_db(dry_run)
            phase4_remaster_characters(db, top_chars, dry_run)

        if from_phase <= 5:
            phase5_seed_artifacts(db, dry_run)

        if from_phase <= 6:
            phase6_verify(db)

        if from_phase <= 7:
            phase7_extraction_loop(db, dry_run)

        if from_phase <= 8:
            phase8_final_remaster(db, dry_run)

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
