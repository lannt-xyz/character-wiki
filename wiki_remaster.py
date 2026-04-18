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
import time
from collections import Counter
from datetime import datetime
from pathlib import Path
from typing import Optional

from loguru import logger

from config.settings import settings
from crawler.storage import load_chapter_content
from db.database import SQLiteDB
from models.schemas import (
    CharBatchSnapshot,
    CharPassResult,
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
_LLM_REQUEST_DIR = _PROJECT_ROOT / settings.data_dir / "llm_requests"
_TRACE_DIR = _RERUN_DIR / "trace"
_DB_PATH = _PROJECT_ROOT / settings.db_path


def _build_db_backup_path() -> Path:
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    # Example: mao-son-troc-quy-nhan_20260417_123456.db.bak
    return _DB_PATH.with_name(f"{_DB_PATH.stem}_{ts}{_DB_PATH.suffix}.bak")

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
        artifact_id = db.upsert_artifact(
  "gender": "male|female|unknown",
  "faction": "<môn phái / phe phái hoặc null>",
  "visual_anchor": "<đặc điểm ngoại hình cố định bằng tiếng Anh, đủ để nhận dạng qua các chương>",
  "personality": "<tính cách ngắn gọn>",
  "relations": [
    {{"target_id": "<character_id>", "relation_type": "<loại quan hệ>", "note": "<ghi chú>"}}
  ],
  "peak_snapshot": {{
    "level": "<cảnh giới cao nhất đạt được>",
        db.upsert_artifact(
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


_REMASTER_CHAR_PASS_SYSTEM = (
    "Bạn là trợ lý wiki nhân vật. Nhiệm vụ: từ các đoạn truyện được cung cấp, "
    "trích xuất sự thay đổi trạng thái của MỘT nhân vật duy nhất theo từng mốc chương. "
    "Chỉ trả JSON theo schema quy định, không giải thích, không markdown."
)

_REMASTER_CHAR_PASS_TMPL = """\
Nhân vật focus: {name} [{character_id}]

Context seed (thông tin đã biết về nhân vật):
---
{seed_context}
---

Pháp khí liên quan:
{artifact_context}

Đoạn truyện (chỉ chứa các đoạn nhắc đến nhân vật này):
---
{spans_text}
---

Trả về JSON với cấu trúc:
{{
  "character_id": "{character_id}",
  "snapshots": [
    {{
      "chapter_start": <số chương đầu của mốc thay đổi>,
      "is_active": true,
      "level": "<cảnh giới hoặc null>",
      "outfit": "<trang phục hoặc null>",
      "weapon": "<vũ khí hoặc null>",
      "vfx_vibes": "<hiệu ứng hình ảnh hoặc null>",
      "physical_description": "<trạng thái thể chất tạm thời hoặc null>",
      "visual_importance": <1-10>
    }}
  ],
  "artifact_updates": [
    {{
      "artifact_id": "<id pháp khí — slug không dấu>",
      "name": "<tên đầy đủ>",
      "material": "<chất liệu chính — null nếu không biết>",
      "visual_anchor": "<mô tả ngoại hình tiếng Anh đủ để render 3D — null nếu chưa rõ>",
      "rarity": "<mức độ quý hiếm — null nếu không biết>",
      "owner_id": "{character_id}",
      "normal_state": "<trạng thái bình thường — null nếu không biết>",
      "active_state": "<khi phát huy sức mạnh — null nếu không biết>",
      "condition": "intact|active|damaged|evolved",
      "vfx_color": "<màu hiệu ứng chủ đạo — null nếu không biết>"
    }}
  ],
  "new_aliases": ["<alias mới phát hiện trong đoạn này, nếu có>"]
}}

Quy tắc:
- snapshots: mỗi entry = 1 mốc chương có thay đổi đáng kể về trạng thái nhân vật
- Chỉ ghi fields thực sự thay đổi, null nếu không đổi
- artifact_updates: chỉ pháp khí của nhân vật focus này
- Nếu không có thay đổi nào, trả về snapshots=[], artifact_updates=[], new_aliases=[]
"""


def _save_llm_request(batch_num: int, chapter_start: int, chapter_end: int, prompt: str, system: str) -> None:
    """Save full LLM request as markdown for inspection."""
    _LLM_REQUEST_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"pass2_ch{chapter_start:03d}-{chapter_end:03d}_batch{batch_num}.md"
    filepath = _LLM_REQUEST_DIR / filename

    content = f"""# LLM Request

- Batch: {batch_num}
- Chapters: {chapter_start}-{chapter_end}
- Timestamp: {datetime.now().isoformat()}

## System Prompt

{system}

## User Prompt

{prompt}
"""
    filepath.write_text(content, encoding="utf-8")
    logger.debug("Saved LLM request to {}", filepath)


def _save_llm_response(batch_num: int, chapter_start: int, chapter_end: int, raw: str) -> None:
    """Save raw LLM response as JSON for inspection."""
    _LLM_REQUEST_DIR.mkdir(parents=True, exist_ok=True)
    filename = f"pass2_ch{chapter_start:03d}-{chapter_end:03d}_batch{batch_num}.json"
    filepath = _LLM_REQUEST_DIR / filename

    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        payload = {
            "_parse_error": str(exc),
            "_raw": raw,
        }

    filepath.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    logger.debug("Saved LLM response to {}", filepath)


def _clear_llm_trace_dir() -> None:
    """Remove old prompt/response trace files before a fresh run."""
    if not _LLM_REQUEST_DIR.exists():
        return
    removed = 0
    for path in _LLM_REQUEST_DIR.glob("pass2_ch*.*"):
        if path.is_file():
            path.unlink()
            removed += 1
    if removed:
        logger.info("Cleared {} old LLM trace files from {}", removed, _LLM_REQUEST_DIR)
    # Also clear per-character trace dir
    if _TRACE_DIR.exists():
        shutil.rmtree(_TRACE_DIR)
        logger.info("Cleared trace dir {}", _TRACE_DIR)


def _save_char_llm_request(
    character_id: str, batch_id: int, segment_start: int, segment_end: int,
    prompt: str, system: str
) -> None:
    """Save per-character LLM request to data/rerun/trace/{character_id}/."""
    trace_dir = _TRACE_DIR / character_id
    trace_dir.mkdir(parents=True, exist_ok=True)
    filepath = trace_dir / f"batch{batch_id:05d}_ch{segment_start}-{segment_end}_request.md"
    content = (
        f"# CharPass LLM Request\n\n"
        f"- batch_id: {batch_id}\n"
        f"- character_id: {character_id}\n"
        f"- chapters: {segment_start}-{segment_end}\n"
        f"- timestamp: {datetime.now().isoformat()}\n\n"
        f"## System\n\n{system}\n\n"
        f"## User\n\n{prompt}\n"
    )
    filepath.write_text(content, encoding="utf-8")
    logger.debug("Saved char request to {}", filepath)


def _save_char_llm_response(
    character_id: str, batch_id: int, segment_start: int, segment_end: int, raw: str
) -> None:
    """Save per-character LLM response to data/rerun/trace/{character_id}/."""
    trace_dir = _TRACE_DIR / character_id
    trace_dir.mkdir(parents=True, exist_ok=True)
    filepath = trace_dir / f"batch{batch_id:05d}_ch{segment_start}-{segment_end}_response.json"
    try:
        payload = json.loads(raw)
    except json.JSONDecodeError as exc:
        payload = {"_parse_error": str(exc), "_raw": raw}
    filepath.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8"
    )
    logger.debug("Saved char response to {}", filepath)


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


def _build_balanced_batch_excerpt(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    max_chars: int = 12000,
) -> str:
    """Build a chapter-balanced excerpt so all chapters get represented.

    Old behavior sliced from the beginning only, which could include just the first
    1-2 chapters in long batches. This function allocates text budget per chapter.
    """
    if not batch_text.strip() or max_chars <= 0:
        return ""

    chapter_count = max(1, chapter_end - chapter_start + 1)
    markers = list(re.finditer(r"\n--- Chương\s+(\d+)\s+---\n", batch_text))
    if not markers:
        return batch_text[:max_chars]

    # Keep some room for separators and avoid very tiny per-chapter slices.
    per_chapter_budget = max(300, max_chars // chapter_count)
    chunks: list[str] = []

    for idx, marker in enumerate(markers):
        block_start = marker.start()
        block_end = markers[idx + 1].start() if idx + 1 < len(markers) else len(batch_text)
        block = batch_text[block_start:block_end]
        chunks.append(block[:per_chapter_budget])

    excerpt = "\n".join(chunks)
    if len(excerpt) > max_chars:
        excerpt = excerpt[:max_chars]
    return excerpt


def _text_has_phrase(batch_text: str, phrase: Optional[str]) -> bool:
    """Return True when a normalized phrase appears in normalized batch text."""
    if not phrase:
        return False
    needle = _normalize(phrase)
    if not needle:
        return False
    # Skip overly generic single-token needles like "kiem" / "phu".
    if " " not in needle and len(needle) < 5:
        return False
    haystack = f" {_normalize(batch_text)} "
    return f" {needle} " in haystack


def _pick_representative_chapters(chapter_nums: list[int], max_count: int) -> list[int]:
    """Pick evenly spaced chapters (always includes first + last)."""
    if not chapter_nums:
        return []
    if len(chapter_nums) <= max_count:
        return chapter_nums
    step = (len(chapter_nums) - 1) / (max_count - 1)
    indices = sorted({round(i * step) for i in range(max_count)})
    return [chapter_nums[i] for i in indices if i < len(chapter_nums)]


def _build_artifact_context(artifacts: list[dict]) -> str:
    """Build markdown artifact context for the current chapter position."""
    if not artifacts:
        return "- Không có pháp khí context"
    lines: list[str] = []
    for art in artifacts:
        lines.append(f"### {art['name']} [{art['artifact_id']}]")
        if art.get("rarity"):
            lines.append(f"- Rarity: {art['rarity']}")
        if art.get("material"):
            lines.append(f"- Material: {art['material']}")
        if art.get("visual_anchor"):
            lines.append(f"- Visual anchor: {art['visual_anchor']}")
        lines.append("")
    return "\n".join(lines).strip()


def _dedup_characters_for_context(characters: list[dict]) -> list[dict]:
    """Deduplicate context rows by tolerant normalized name.

    Keeps the first occurrence so caller can control priority order.
    """
    deduped: list[dict] = []
    seen: set[str] = set()
    for char in characters:
        name_key = _normalize(char.get("name_normalized") or char.get("name") or char["character_id"])
        if not name_key:
            name_key = char["character_id"]
        if name_key in seen:
            continue
        seen.add(name_key)
        deduped.append(char)
    return deduped


def _select_candidate_characters(batch_text: str, all_chars: list[dict], max_total: int = 12) -> list[dict]:
    """Select characters whose name or aliases appear directly in the batch text."""
    matched: list[dict] = []
    for char in all_chars:
        aliases_raw = char.get("aliases_json") or "[]"
        try:
            aliases = json.loads(aliases_raw) if isinstance(aliases_raw, str) else (aliases_raw or [])
        except Exception:
            aliases = []
        phrases = [char.get("name"), *aliases]
        if any(_text_has_phrase(batch_text, phrase) for phrase in phrases):
            matched.append(char)
    return _dedup_characters_for_context(matched)[:max_total]


def _select_candidate_artifacts(batch_text: str, all_artifacts: list[dict], max_total: int = 20) -> list[dict]:
    """Select artifacts whose display name appears directly in the batch text."""
    matched: list[dict] = []
    for art in all_artifacts:
        if _text_has_phrase(batch_text, art.get("name")):
            matched.append(art)
    return matched[:max_total]


def _filter_artifacts_against_character_names(
    artifacts: list[dict],
    characters: list[dict],
) -> list[dict]:
    """Drop artifact rows whose names collide with character names in the same batch context."""
    char_name_keys = {
        _normalize(char.get("name_normalized") or char.get("name") or "")
        for char in characters
        if char.get("name_normalized") or char.get("name")
    }
    result: list[dict] = []
    for art in artifacts:
        art_name_key = _normalize(art.get("name") or art.get("artifact_id") or "")
        if art_name_key and art_name_key in char_name_keys:
            continue
        result.append(art)
    return result


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


# ---------------------------------------------------------------------------
# Per-character helpers (Phase 1 & Phase 3)
# ---------------------------------------------------------------------------

def _build_mention_index(db: SQLiteDB, all_chars: list[dict]) -> None:
    """Build wiki_mention_index for all characters.

    Strategy: pre-load + pre-normalize all chapters once (17MB RAM),
    then scan 600 chars against the normalized texts — avoids 2.1M DB round-trips.
    """
    chapter_pairs = db.get_all_chapter_contents()
    # {chapter_num: " <normalized text> "} — outer spaces for whole-word match
    chapter_norm: dict[int, str] = {
        num: f" {_normalize(text)} " for num, text in chapter_pairs if text
    }
    total = len(all_chars)
    logger.info(
        "_build_mention_index | {} chapters loaded | scanning {} chars...",
        len(chapter_norm), total,
    )

    for i, char in enumerate(all_chars, 1):
        cid = char["character_id"]
        aliases_raw = char.get("aliases_json") or "[]"
        try:
            aliases = json.loads(aliases_raw) if isinstance(aliases_raw, str) else (aliases_raw or [])
        except Exception:
            aliases = []

        # Build normalized needle list (same logic as _text_has_phrase)
        phrases = [char.get("name"), *aliases]
        needles: list[str] = []
        for phrase in phrases:
            if not phrase:
                continue
            n = _normalize(phrase)
            if n and not (" " not in n and len(n) < 5):
                needles.append(f" {n} ")

        if not needles:
            continue

        chapter_nums = [
            num for num, norm_text in chapter_norm.items()
            if any(needle in norm_text for needle in needles)
        ]
        if chapter_nums:
            db.build_mention_index(cid, chapter_nums)

        if i % 50 == 0:
            logger.info("_build_mention_index | {}/{} chars done", i, total)

    logger.info("_build_mention_index | done | {} chars processed", total)


def _group_char_segments(
    chapter_nums: list[int], segment_size: int, gap_threshold: int
) -> list[tuple[int, int]]:
    """Group mention chapter numbers into (segment_start, segment_end) tuples.

    Step 1: Split into independent groups when consecutive gap > gap_threshold.
    Step 2: Within each group, create windows of segment_size chapter range.
    The (start, end) are actual chapter numbers used in DB range queries.
    """
    if not chapter_nums:
        return []
    sorted_nums = sorted(set(chapter_nums))

    # Step 1: split by gap
    groups: list[list[int]] = []
    current: list[int] = [sorted_nums[0]]
    for num in sorted_nums[1:]:
        if num - current[-1] > gap_threshold:
            groups.append(current)
            current = [num]
        else:
            current.append(num)
    groups.append(current)

    # Step 2: split each group into segment_size chapter-range windows
    segments: list[tuple[int, int]] = []
    for group in groups:
        window_start = group[0]
        while True:
            window_end = window_start + segment_size - 1
            in_window = [n for n in group if window_start <= n <= window_end]
            if not in_window:
                break
            segments.append((in_window[0], in_window[-1]))
            remaining = [n for n in group if n > window_end]
            if not remaining:
                break
            window_start = remaining[0]

    return segments


def _load_chapters_by_range(db: SQLiteDB, start: int, end: int) -> dict[int, str]:
    """Return {chapter_num: content} for chapters in [start, end]."""
    rows = db._conn.execute(
        "SELECT chapter_num, content FROM chapters WHERE chapter_num BETWEEN ? AND ? AND content IS NOT NULL",
        (start, end),
    ).fetchall()
    return {row[0]: row[1] for row in rows if row[1]}


def _load_character_seed_context(character_id: str, char_row: dict, db: SQLiteDB) -> str:
    """Return seed context markdown for the character.

    Priority: data/rerun/character_input/{id}.md → fallback to built from v1 snapshots.
    """
    seed_file = _CHAR_INPUT_DIR / f"{character_id}.md"
    if seed_file.exists():
        return seed_file.read_text(encoding="utf-8")

    # Fallback: build from v1 snapshots in DB
    snapshots_v1 = [
        s for s in db.get_all_snapshots(character_id)
        if s.get("extraction_version", 1) == 1
    ]
    return _build_character_markdown(char_row, snapshots_v1, db=db)


def _extract_character_spans(
    chapter_texts: dict[int, str],
    character_id: str,
    char_row: dict,
    budget: Optional[int] = None,
) -> str:
    """Extract paragraphs mentioning the character from chapter texts.

    Includes the paragraph immediately before each matching paragraph
    (context window — author often describes appearance before naming the char).
    Returns empty string if no paragraphs match.
    """
    if budget is None:
        budget = settings.char_span_budget

    aliases_raw = char_row.get("aliases_json") or "[]"
    try:
        aliases = json.loads(aliases_raw) if isinstance(aliases_raw, str) else (aliases_raw or [])
    except Exception:
        aliases = []

    phrases = [char_row.get("name"), *aliases]
    valid_phrases = [p for p in phrases if p and _normalize(p) and not (
        " " not in _normalize(p) and len(_normalize(p)) < 5
    )]

    if not valid_phrases:
        return ""

    total_chars = 0
    result_parts: list[str] = []

    for chap_num in sorted(chapter_texts.keys()):
        text = chapter_texts.get(chap_num) or ""
        paragraphs = [p.strip() for p in text.split("\n\n") if p.strip()]
        selected: list[str] = []

        for idx, para in enumerate(paragraphs):
            if any(_text_has_phrase(para, phrase) for phrase in valid_phrases):
                # Context window: include predecessor paragraph if present and not already added
                if idx > 0:
                    prev = paragraphs[idx - 1]
                    if not selected or selected[-1] != prev:
                        selected.append(prev)
                if not selected or selected[-1] != para:
                    selected.append(para)

        if not selected:
            continue

        chapter_block = f"--- Chương {chap_num} ---\n" + "\n\n".join(selected)
        block_len = len(chapter_block)

        if total_chars + block_len > budget:
            remaining = budget - total_chars
            if remaining > 100:
                result_parts.append(chapter_block[:remaining])
            break

        result_parts.append(chapter_block)
        total_chars += block_len

    return "\n\n".join(result_parts)


def _deduplicate_characters_phase2(db: SQLiteDB, dry_run: bool = False) -> int:
    """Merge duplicate character rows by canonicalized display name.

    Canonical pick priority:
    1) character_id == slugify_vi(name)
    2) highest snapshot count
    3) lexicographically smaller character_id
    """
    chars = db.get_all_characters(include_deleted=False)
    groups: dict[str, list[dict]] = {}
    for char in chars:
        key = slugify_vi(char.get("name") or "")
        if not key:
            continue
        groups.setdefault(key, []).append(char)

    merged_total = 0
    for key, members in groups.items():
        if len(members) <= 1:
            continue

        scored = []
        for c in members:
            cid = c["character_id"]
            score = (
                1 if cid == key else 0,
                db.get_character_snapshot_count(cid),
                -len(cid),
            )
            scored.append((score, cid))
        scored.sort(reverse=True)
        canonical_id = scored[0][1]
        duplicate_ids = [c["character_id"] for c in members if c["character_id"] != canonical_id]

        logger.warning(
            "Phase 2 dedup | name_key={} canonical={} duplicates={}",
            key,
            canonical_id,
            duplicate_ids,
        )
        if dry_run:
            continue
        merged_total += db.merge_character_records(
            canonical_id=canonical_id,
            duplicate_ids=duplicate_ids,
            reason="phase2_name_dedup",
        )

    if merged_total:
        logger.info("Phase 2 dedup done | {} duplicate rows merged", merged_total)
    else:
        logger.info("Phase 2 dedup done | no duplicates found")
    return merged_total


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
    backup_path = _build_db_backup_path()
    shutil.copy(str(_DB_PATH), str(backup_path))
    logger.info("DB backed up to {}", backup_path)


# ---------------------------------------------------------------------------
# Phase 0 — Backup DB
# ---------------------------------------------------------------------------

def phase0_backup(dry_run: bool = False) -> None:
    logger.info("=== Phase 0: Backup DB ===")
    _backup_db(dry_run)


# ---------------------------------------------------------------------------
# Phase 1 — Init mention index + char batches
# ---------------------------------------------------------------------------

def phase1_init_batches(db: SQLiteDB, dry_run: bool = False) -> None:
    """Full reset + rebuild mention index + char batches for all active characters."""
    logger.info("=== Phase 1: Init mention index + char batches ===")

    # Full reset: purge all v2 data, clear mention_index + char_batches
    deleted, _ = db.reset_remaster_v2()
    if deleted:
        logger.warning("Phase 1 | Purged {} stale v2 data", deleted)

    all_chars = db.get_top_chars_by_v1_importance(limit=settings.char_top_limit)
    total_active = len(db.get_all_active_characters())
    logger.info(
        "Phase 1 | processing top {} / {} active chars (char_top_limit={})",
        len(all_chars), total_active, settings.char_top_limit,
    )

    if dry_run:
        logger.info("dry_run: skip mention index build and char_batches insert")
        return

    _clear_llm_trace_dir()

    # Build mention index (pre-loads all chapters into RAM for efficiency)
    _build_mention_index(db, all_chars)

    # Group into char_batches by segment
    batch_rows: list[dict] = []
    for char in all_chars:
        cid = char["character_id"]
        chapter_nums = db.get_mention_chapters(cid)
        if not chapter_nums:
            continue
        segments = _group_char_segments(
            chapter_nums,
            segment_size=settings.char_segment_size,
            gap_threshold=settings.char_gap_threshold,
        )
        for seg_start, seg_end in segments:
            batch_rows.append({
                "character_id": cid,
                "segment_start": seg_start,
                "segment_end": seg_end,
            })

    db.build_char_batches(batch_rows)
    total_batches = db.count_char_batches_total()
    logger.info(
        "Phase 1 done | {} chars | {} char_batches",
        len(all_chars), total_batches,
    )


# ---------------------------------------------------------------------------
# Phase 2 — Build character markdown + seed artifact stubs
# ---------------------------------------------------------------------------

def phase2_build_input(db: SQLiteDB, dry_run: bool = False) -> list[dict]:
    """Build top-20 character markdown files and seed artifact stubs from v1 data.

    Markdown includes: v1 milestone snapshots + raw chapter excerpts + artifact seed list.
    Artifact stubs are inserted into wiki_artifacts so Phase 3 loop can use them from batch 1.
    """
    logger.info("=== Phase 2: Build character input + seed artifact stubs ===")

    # Ensure one active row per character before generating markdown files.
    _deduplicate_characters_phase2(db, dry_run)

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
# Per-character LLM pass + merge (Phase 3 v2 core)
# ---------------------------------------------------------------------------

def _remaster_char_pass(
    batch_id: int,
    character_id: str,
    segment_start: int,
    segment_end: int,
    char_context: str,
    artifact_context: str,
    spans_text: str,
    char_name: str,
) -> CharPassResult:
    """Call LLM for one character × one segment. Returns validated CharPassResult."""
    prompt = _REMASTER_CHAR_PASS_TMPL.format(
        name=char_name,
        character_id=character_id,
        seed_context=char_context,
        artifact_context=artifact_context,
        spans_text=spans_text,
    )
    _save_char_llm_request(character_id, batch_id, segment_start, segment_end, prompt, _REMASTER_CHAR_PASS_SYSTEM)
    raw = _ollama_generate(prompt, _REMASTER_CHAR_PASS_SYSTEM, settings.wiki_extract_model)
    _save_char_llm_response(character_id, batch_id, segment_start, segment_end, raw)
    try:
        data = json.loads(raw)
    except json.JSONDecodeError as exc:
        raise ValueError(
            f"CharPass JSON parse failed | char={character_id} batch={batch_id} | {exc}"
        ) from exc
    # Ensure character_id is correct even if LLM returns wrong value
    data["character_id"] = character_id
    return CharPassResult.model_validate(data)


def _merge_char_pass_result(
    result: CharPassResult,
    char_row: dict,
    db: SQLiteDB,
) -> int:
    """Merge CharPassResult into DB. Returns number of snapshots written."""
    character_id = result.character_id
    written = 0

    for snap in result.snapshots:
        if db.snapshot_exists(character_id, snap.chapter_start, extraction_version=2):
            continue
        db.add_snapshot(
            character_id=character_id,
            chapter_start=snap.chapter_start,
            is_active=snap.is_active,
            level=snap.level,
            outfit=snap.outfit,
            weapon=snap.weapon,
            vfx_vibes=snap.vfx_vibes,
            physical_description=snap.physical_description,
            visual_importance=snap.visual_importance,
            extraction_version=2,
        )
        written += 1

    # Dedup-append new_aliases
    if result.new_aliases:
        existing_raw = char_row.get("aliases_json") or "[]"
        try:
            existing_aliases: list[str] = json.loads(existing_raw) if isinstance(existing_raw, str) else []
        except Exception:
            existing_aliases = []
        existing_normalized = {_normalize(a) for a in existing_aliases}
        truly_new: list[str] = []
        for alias in result.new_aliases:
            norm = _normalize(alias)
            if norm and norm not in existing_normalized:
                truly_new.append(alias)
                logger.warning(
                    "New alias discovered | char={} alias={} — may need index rebuild",
                    character_id, alias,
                )
        if truly_new:
            db.merge_aliases(character_id, truly_new)

    # Artifact updates (use first snapshot chapter_start, or segment's start)
    if result.artifact_updates:
        chapter_ref = result.snapshots[0].chapter_start if result.snapshots else 0
        _merge_artifact_updates(db, result.artifact_updates, chapter_ref)

    return written


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
    batch_id: int,
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    candidate_chars: list[dict],
    artifact_context: str,
) -> tuple[ExtractionResult, list[dict]]:
    """Run Pass 2: extract character deltas + artifact updates for this batch."""
    context_str = _build_character_context(candidate_chars)
    batch_excerpt = _build_balanced_batch_excerpt(
        batch_text=batch_text,
        chapter_start=chapter_start,
        chapter_end=chapter_end,
        max_chars=12000,
    )
    
    # Log context being sent to LLM for inspection
    logger.debug(
        "Pass 2 context | batch={}-{} | chars={} | {}...",
        chapter_start, chapter_end, len(candidate_chars),
        context_str[:200] if context_str else "(empty)"
    )
    
    prompt = _REMASTER_PASS2_TMPL.format(
        start=chapter_start,
        end=chapter_end,
        text=batch_excerpt,
        character_context=context_str,
        artifact_context=artifact_context,
    )
    
    # Save full request to file for inspection
    _save_llm_request(batch_id, chapter_start, chapter_end, prompt, _REMASTER_PASS2_SYSTEM)
    
    raw = _ollama_generate(prompt, _REMASTER_PASS2_SYSTEM, settings.wiki_extract_model)
    _save_llm_response(batch_id, chapter_start, chapter_end, raw)
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
        artifact_id = db.upsert_artifact(
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


def _phase3_legacy_loop(db: SQLiteDB, dry_run: bool = False) -> None:
    # LEGACY — replaced by phase3_char_extraction_loop
    """Original batch-by-chapter extraction loop. Kept for reference only."""
    logger.info("=== Phase 3: Remaster extraction loop ===")

    all_characters_fn = db.get_all_characters
    all_artifacts_fn = db.get_all_artifacts

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

        try:
            all_chars = all_characters_fn()
            all_artifacts = all_artifacts_fn()
            candidate_chars = _select_candidate_characters(batch_text, all_chars)
            candidate_artifacts = _select_candidate_artifacts(batch_text, all_artifacts)
            candidate_artifacts = _filter_artifacts_against_character_names(
                candidate_artifacts,
                candidate_chars,
            )
            artifact_ctx = _build_artifact_context(candidate_artifacts)

            logger.debug(
                "Candidate chars | batch={}-{} | ids={}",
                chapter_start,
                chapter_end,
                [c["character_id"] for c in candidate_chars],
            )
            logger.debug(
                "Candidate artifacts | batch={}-{} | ids={}",
                chapter_start,
                chapter_end,
                [a["artifact_id"] for a in candidate_artifacts],
            )

            # [Phase 4 inline] Pass 2 — extract deltas + artifacts
            extraction_result, artifact_updates = _remaster_pass2(
                batch_id,
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
# Phase 3 (v2) — Per-character extraction loop
# ---------------------------------------------------------------------------

def phase3_char_extraction_loop(
    db: SQLiteDB, dry_run: bool = False, max_batches: Optional[int] = None
) -> None:
    """Per-character salient-span extraction loop.

    Sequential — 1 LLM call per char_batch. Resume-safe: restarts from first PENDING batch.
    Each call: load chapter texts → extract spans → LLM → merge v2 snapshots.
    """
    logger.info("=== Phase 3 (v2): Per-character extraction loop ===")

    pending = db.get_pending_char_batches()
    total = db.count_char_batches_total()
    merged = db.count_char_batches_merged()

    if not pending:
        logger.info("Phase 3 | No pending char batches.")
        return

    logger.info("Phase 3 | {}/{} char batches pending", len(pending), total)
    all_artifacts_fn = db.get_all_artifacts
    consecutive_fail = 0
    processed = 0

    for batch in pending:
        if max_batches is not None and processed >= max_batches:
            logger.info("Phase 3 | Reached max_batches={}, stopping.", max_batches)
            break

        batch_id = batch["batch_id"]
        character_id = batch["character_id"]
        segment_start = batch["segment_start"]
        segment_end = batch["segment_end"]
        pct = (merged / total * 100) if total else 0

        logger.info(
            "[CharBatch {}/{}  | {:.1f}%] char={} ch={}-{}",
            batch_id, total, pct, character_id, segment_start, segment_end,
        )
        _t_batch_start = time.monotonic()

        # Fetch character row
        char_row = db.get_character(character_id)
        if not char_row:
            logger.warning("CharBatch {} | character not found: {} — skip", batch_id, character_id)
            if not dry_run:
                db.set_char_batch_status(batch_id, "MERGED")
            merged += 1
            processed += 1
            continue

        # Load chapter texts for this segment
        chapter_texts = _load_chapters_by_range(db, segment_start, segment_end)

        # Extract salient spans for this character
        spans_text = _extract_character_spans(chapter_texts, character_id, char_row)
        if not spans_text:
            logger.info(
                "CharBatch {} | no spans for char={} ch={}-{} — skip LLM",
                batch_id, character_id, segment_start, segment_end,
            )
            if not dry_run:
                db.set_char_batch_status(batch_id, "MERGED")
            merged += 1
            processed += 1
            continue

        # Seed context
        seed_context = _load_character_seed_context(character_id, char_row, db)

        # Candidate artifacts from the combined segment text
        combined_text = "\n".join(chapter_texts.values())
        all_artifacts = all_artifacts_fn()
        candidate_artifacts = _select_candidate_artifacts(combined_text, all_artifacts)
        candidate_artifacts = _filter_artifacts_against_character_names(candidate_artifacts, [char_row])
        artifact_ctx = _build_artifact_context(candidate_artifacts)

        try:
            _t_llm_start = time.monotonic()
            result = _remaster_char_pass(
                batch_id=batch_id,
                character_id=character_id,
                segment_start=segment_start,
                segment_end=segment_end,
                char_context=seed_context,
                artifact_context=artifact_ctx,
                spans_text=spans_text,
                char_name=char_row.get("name", character_id),
            )
            _t_llm_end = time.monotonic()
            logger.debug(
                "LLM call done | char={} ch={}-{} | llm={:.1f}s",
                character_id, segment_start, segment_end, _t_llm_end - _t_llm_start,
            )
            consecutive_fail = 0
        except Exception as exc:
            consecutive_fail += 1
            logger.warning(
                "CharBatch fail #{} | batch={} char={} error={}",
                consecutive_fail, batch_id, character_id, exc,
            )
            if consecutive_fail > settings.wiki_max_consecutive_fail:
                logger.error("Too many consecutive failures. Stopping.")
                break
            if not dry_run:
                db.set_char_batch_status(batch_id, "MERGED")
            merged += 1
            processed += 1
            continue

        if not dry_run:
            written = _merge_char_pass_result(result, char_row, db)
            db.set_char_batch_status(batch_id, "MERGED")
            merged += 1
            _t_total = time.monotonic() - _t_batch_start
            logger.info(
                "[CharBatch {}/{}  | {:.1f}%] MERGED | char={} snaps={} | total={:.1f}s",
                batch_id, total, (merged / total * 100), character_id, written, _t_total,
            )
        else:
            _t_total = time.monotonic() - _t_batch_start
            logger.info(
                "[CharBatch {}] dry_run | char={} snaps={} art_upd={} | total={:.1f}s",
                batch_id, character_id,
                len(result.snapshots), len(result.artifact_updates), _t_total,
            )

        processed += 1

    logger.info(
        "Phase 3 done | merged={}/{} char batches",
        db.count_char_batches_merged(), total,
    )


# ---------------------------------------------------------------------------
# Phase 4 — Final synthesis from v2 snapshots
# ---------------------------------------------------------------------------

def phase4_final_synthesis(db: SQLiteDB, dry_run: bool = False) -> None:
    """Re-synthesize all active wiki_characters from v2 snapshots.

    Processes chars ordered by max v2 visual_importance desc
    (visual_importance >= 6 first so Video Pipeline has data ASAP).
    Resume-safe: skips chars already at remaster_version=2.
    """
    logger.info("=== Phase 4: Final synthesis from v2 snapshots ===")
    all_chars = db.get_all_chars_ordered_for_synthesis()
    if not all_chars:
        logger.warning("No characters found in DB.")
        return

    updated = 0
    skipped = 0
    for char in all_chars:
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
        updated, len(all_chars), skipped,
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
  1  Init mention index + char batches for all active characters (full reset)
  2  Build character markdown + seed artifact stubs from v1 data
  3  Per-character extraction loop (sequential, resume-safe)
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
    parser.add_argument(
        "--max-batches",
        type=int,
        default=None,
        metavar="N",
        help="Limit Phase 3 to N char batches (for testing)",
    )
    parser.add_argument(
        "--stats",
        action="store_true",
        help="Print pipeline progress stats and exit",
    )
    args = parser.parse_args()
    from_phase: int = args.from_phase
    dry_run: bool = args.dry_run
    max_batches: Optional[int] = args.max_batches

    db = SQLiteDB(settings.db_path)

    if args.stats:
        total = db.count_char_batches_total()
        merged = db.count_char_batches_merged()
        pct = (merged / total * 100) if total else 0.0
        active_chars = len(db.get_all_active_characters())
        print(
            f"wiki_remaster stats\n"
            f"  char_batches : {merged}/{total} merged ({pct:.1f}%)\n"
            f"  active chars : {active_chars}\n"
            f"  v2 snapshots : {db._scalar('SELECT COUNT(*) FROM wiki_snapshots WHERE extraction_version=2')}"
        )
        db.close()
        return

    if from_phase < 0 or from_phase > 4:
        logger.error("--from-phase must be between 0 and 4 (got {})", from_phase)
        db.close()
        sys.exit(1)

    logger.info(
        "wiki_remaster starting | from_phase={} dry_run={} max_batches={}",
        from_phase, dry_run, max_batches,
    )

    try:
        # Phase 0: Backup DB
        if from_phase <= 0:
            phase0_backup(dry_run)

        # Phase 1: Build mention index + char batches (full reset)
        if from_phase <= 1:
            phase1_init_batches(db, dry_run)

        # Phase 2: Build character markdown + seed artifact stubs
        if from_phase <= 2:
            phase2_build_input(db, dry_run)

        # Phase 3: Per-character extraction loop (sequential, resume-safe)
        if from_phase <= 3:
            phase3_char_extraction_loop(db, dry_run, max_batches=max_batches)

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
