"""wiki/extractor.py — Two-pass LLM extraction via Ollama REST.

Pass 1 — Name Scan (small request):
    Send raw batch text + short prompt, ask LLM to list characters + aliases.
    Skip Pass 1 when total wiki size < wiki_context_threshold characters.

Pass 2 — Delta Extract:
    Normalize names from Pass 1, lookup existing characters, send compact context
    + batch text, return ExtractionResult (new_characters + updated_characters).
"""

import json
import re
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

import httpx
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import settings
from models.schemas import CharacterPatch, ExtractionResult, NameEntry

_DATA_DIR = Path(settings.data_dir)
_LLM_REQ_DIR = _DATA_DIR / "llm_request"
_LLM_RESP_DIR = _DATA_DIR / "llm_response"


def _trace_request(tag: str, chapter_start: int, chapter_end: int, system: str, prompt: str) -> None:
    """Write LLM prompt to data/llm_request/<tag>_ch{start}-{end}_<ts>.txt."""
    try:
        _LLM_REQ_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%H%M%S")
        fname = f"{tag}_ch{chapter_start}-{chapter_end}_{ts}.txt"
        (_LLM_REQ_DIR / fname).write_text(
            f"[SYSTEM]\n{system}\n\n[USER]\n{prompt}", encoding="utf-8"
        )
    except Exception as exc:
        logger.debug("trace_request write failed | {}", exc)


def _trace_response(tag: str, chapter_start: int, chapter_end: int, raw: str) -> None:
    """Write LLM raw response to data/llm_response/<tag>_ch{start}-{end}_<ts>.txt."""
    try:
        _LLM_RESP_DIR.mkdir(parents=True, exist_ok=True)
        ts = datetime.now(timezone.utc).strftime("%H%M%S")
        fname = f"{tag}_ch{chapter_start}-{chapter_end}_{ts}.txt"
        (_LLM_RESP_DIR / fname).write_text(raw, encoding="utf-8")
    except Exception as exc:
        logger.debug("trace_response write failed | {}", exc)


class ExtractionFatalError(Exception):
    """Raised when consecutive fail counter exceeds wiki_max_consecutive_fail."""


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_PASS1_SYSTEM = (
    "Bạn là trợ lý phân tích truyện. Nhiệm vụ: liệt kê TẤT CẢ tên nhân vật xuất hiện trong "
    "đoạn text dưới đây — bao gồm cả nhân vật phụ, người chỉ nhắc thoáng qua, nhân vật được "
    "gọi bằng danh hiệu/vai vế/biệt danh/chức danh (vd: 'chị Thảo', 'thầy giáo', 'người phụ nữ'). "
    "Kèm biệt danh/cách gọi khác. Chỉ trả JSON array, không giải thích thêm."
)

_PASS1_USER_TMPL = """\
Đoạn truyện (Chương {start}-{end}):
---
{text}
---
Trả về JSON array với cấu trúc: [{{"name": "...", "aliases": ["...", "..."]}}]
Chỉ tên nhân vật cụ thể, không tên địa danh hay tên sự vật."""

_PASS2_SYSTEM = (
    "Bạn là trợ lý xây dựng wiki nhân vật. Nhiệm vụ: trích xuất đầy đủ thông tin nhân vật "
    "từ đoạn truyện — ưu tiên ngoại hình, trang phục, tính cách, trạng thái thể chất. "
    "Với nhân vật mới: điền visual_anchor từ MỌI mô tả ngoại hình có trong text, "
    "không bỏ sót dù là chi tiết nhỏ (dáng người, tóc, mắt, giọng nói, cử chỉ đặc trưng). "
    "Chỉ trả JSON theo schema quy định, không giải thích, không markdown."
)

_PASS2_USER_TMPL = """\
Đoạn truyện mới (Chương {start}-{end}):
---
{text}
---

Danh sách nhân vật liên quan (context):
{character_context}

Trả về JSON với cấu trúc:
{{
  "new_characters": [
    {{
      "character": {{
        "character_id": "<slug tiếng Việt không dấu, dùng underscore, vd: diep_dai_bao>",
        "name": "<tên đầy đủ>",
        "name_normalized": "<lowercase, no diacritics removed>",
        "aliases": [],
        "gender": "<nam/nữ/không rõ>",
        "faction": "<phe/tổ chức/nghề nghiệp hoặc null>",
        "traits": ["<tính cách và cử chỉ đặc trưng, vd: bình tĩnh, dịu dàng, hay cười khẽ>"],
        "relations": [{{"related_name": "...", "description": "...", "chapter_start": {start}}}],
        "visual_anchor": "<mô tả ngoại hình cố định: vóc dáng, mái tóc, màu mắt, làn da, giọng nói, cử chỉ đặc trưng. Tổng hợp MỌI chi tiết ngoại hình cố định từ text, không chỉ sẹo/dị tật. null nếu không có thông tin>",
        "age": "<tuổi hoặc mô tả độ tuổi (vd: 20, khoảng 15, trẻ em) hoặc null>",
        "personality": "<tóm tắt tính cách đặc trưng của nhân vật dựa trên hành động/lời nói trong text hoặc null>"
      }},
      "snapshot": {{
        "chapter_start": {start},
        "is_active": true,
        "level": "<cảnh giới hoặc null (null nếu truyện hiện đại/không có tu tiên)>",
        "age": "<tuổi hoặc mô tả độ tuổi (vd: 20, khoảng 15, trẻ em) hoặc null>",
        "outfit": "<mô tả đầy đủ trang phục: kiểu, màu, chi tiết nổi bật. null nếu không đề cập>",
        "weapon": "<vũ khí hoặc null>",
        "vfx_vibes": "<mô tả hiệu ứng hình ảnh/không khí hoặc null>",
        "physical_description": "<trạng thái thể chất tạm thời (vết thương, mệt mỏi, cảm xúc trên gương mặt) hoặc null>",
        "visual_importance": <1-10>
      }}
    }}
  ],
  "updated_characters": [
    {{
      "character_id": "<id nhân vật cũ>",
      "level": "<cảnh giới mới hoặc null nếu không đổi>",
      "age": "<tuổi mới hoặc null nếu không đổi>",
      "personality": "<tính cách mới/thay đổi hoặc null>",
      "outfit": "<trang phục mới hoặc null>",
      "weapon": "<vũ khí mới hoặc null>",
      "vfx_vibes": "<hiệu ứng mới hoặc null>",
      "physical_description": "<trạng thái thể chất tạm thời; null nếu trạng thái cũ đã kết thúc hoặc không nhắc>",
      "visual_importance": <int hoặc null>,
      "is_active": <true/false hoặc null>,
      "aliases": ["<biệt danh mới>"] hoặc null
    }}
  ]
}}

Quy tắc quan trọng:
- Persistent fields (level, outfit, weapon, vfx_vibes): trả null nếu không thay đổi
- Transient field (physical_description): trả null nếu trạng thái đó kết thúc hoặc không nhắc
- Không nhắc đến nhân vật không xuất hiện trong đoạn này
- updated_characters chỉ chứa nhân vật CŨ (đã có trong context), không chứa nhân vật mới
- Nhân vật kể chuyện theo ngôi thứ nhất (xưng 'tôi', 'mình') PHẢI đưa vào new_characters nếu chưa có trong context, dù chỉ xuất hiện qua đại từ"""


# ---------------------------------------------------------------------------
# Ollama REST helpers
# ---------------------------------------------------------------------------

@retry(
    stop=stop_after_attempt(settings.llm_max_retries),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
    reraise=True,
)
def _ollama_generate(prompt: str, system: str, model: str) -> str:
    """Call Ollama /api/generate (sync). Returns the response content string."""
    payload = {
        "model": model,
        "prompt": prompt,
        "system": system,
        "stream": False,
        "format": "json",
    }
    t0 = time.monotonic()
    with httpx.Client(timeout=settings.llm_timeout) as client:
        resp = client.post(f"{settings.ollama_url}/api/generate", json=payload)
        resp.raise_for_status()
    elapsed = time.monotonic() - t0
    logger.debug("Ollama request done | model={} elapsed={:.1f}s", model, elapsed)
    return resp.json()["response"]


def offload_ollama(model: Optional[str] = None) -> None:
    """Unload the model from VRAM by setting keep_alive=0.

    Safe to call even if Ollama is not running — logs warning and returns.
    """
    target = model or settings.wiki_extract_model
    try:
        with httpx.Client(timeout=10) as client:
            resp = client.post(
                f"{settings.ollama_url}/api/generate",
                json={"model": target, "keep_alive": 0},
            )
            resp.raise_for_status()
        logger.info("Ollama model offloaded from VRAM | model={}", target)
    except Exception as exc:
        logger.warning("Could not offload Ollama | model={} error={}", target, exc)


# ---------------------------------------------------------------------------
# Greedy chapter packing
# ---------------------------------------------------------------------------

def _pack_chapters_within_budget(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    max_chars: int,
) -> str:
    """Greedily pack full chapters within the char budget.

    Iterates chapters in order and stops at the first chapter that would
    exceed max_chars — included chapters are always complete and contiguous,
    never truncated or reordered. Falls back to head-truncation when chapter
    markers are absent. Token estimate: ~4 chars/token (Vietnamese mixed text).
    """
    if not batch_text.strip() or max_chars <= 0:
        return ""

    markers = list(re.finditer(r"\n--- Chương\s+\d+\s+---\n", batch_text))
    if not markers:
        return batch_text[:max_chars]

    included: list[str] = []
    used = 0
    for idx, marker in enumerate(markers):
        block_start = marker.start()
        block_end = markers[idx + 1].start() if idx + 1 < len(markers) else len(batch_text)
        chapter_text = batch_text[block_start:block_end]
        if used + len(chapter_text) > max_chars:
            if not included:
                # Always include at least the first chapter even if it exceeds
                # the budget (better to send slightly over than to send nothing).
                included.append(chapter_text)
            break  # stop — chapters must be contiguous, no skipping
        included.append(chapter_text)
        used += len(chapter_text)

    return "\n".join(included)


# ---------------------------------------------------------------------------
# Pass 1 — Name Scan
# ---------------------------------------------------------------------------

def _pass1_name_scan(batch_text: str, chapter_start: int, chapter_end: int) -> list[NameEntry]:
    """Return list of NameEntry from the batch text. Empty list on parse failure."""
    excerpt = _pack_chapters_within_budget(
        batch_text, chapter_start, chapter_end, settings.wiki_pass1_budget
    )
    prompt = _PASS1_USER_TMPL.format(
        start=chapter_start, end=chapter_end, text=excerpt
    )
    _trace_request("pass1", chapter_start, chapter_end, _PASS1_SYSTEM, prompt)
    try:
        raw = _ollama_generate(prompt, _PASS1_SYSTEM, settings.wiki_extract_model)
        _trace_response("pass1", chapter_start, chapter_end, raw)
        data = json.loads(raw)
        # Support both wrapped {"characters": [...]} and bare [...]
        if isinstance(data, dict):
            data = data.get("characters", data.get("names", []))
        entries = [NameEntry.model_validate(item) for item in data if isinstance(item, dict)]
        logger.debug("Pass 1 found {} names | batch={}-{}", len(entries), chapter_start, chapter_end)
        return entries
    except Exception as exc:
        logger.warning("Pass 1 parse fail | batch={}-{} error={}", chapter_start, chapter_end, exc)
        return []


# ---------------------------------------------------------------------------
# Pass 2 — Delta Extract
# ---------------------------------------------------------------------------

def _build_character_context(characters: list[dict]) -> str:
    """Build markdown context for Pass 2 prompt."""
    if not characters:
        return "- Không có nhân vật context"

    lines: list[str] = []
    for char in characters:
        aliases_raw = char.get("aliases_json", "[]")
        if isinstance(aliases_raw, str):
            try:
                aliases = json.loads(aliases_raw)
            except Exception:
                aliases = []
        else:
            aliases = aliases_raw or []

        traits_raw = char.get("traits_json", "[]")
        if isinstance(traits_raw, str):
            try:
                traits = json.loads(traits_raw)
            except Exception:
                traits = []
        else:
            traits = traits_raw or []

        lines.append(f"### {char['name']} [{char['character_id']}]")
        if char.get("gender"):
            lines.append(f"- Giới tính: {char['gender']}")
        if char.get("faction"):
            lines.append(f"- Phe/Nghề: {char['faction']}")
        if char.get("age"):
            lines.append(f"- Tuổi: {char['age']}")
        if char.get("personality"):
            lines.append(f"- Tính cách: {char['personality']}")
        if aliases:
            lines.append(f"- Aliases: {', '.join(aliases)}")
        if char.get("visual_anchor"):
            lines.append(f"- Ngoại hình: {char['visual_anchor']}")
        if traits:
            lines.append(f"- Traits: {', '.join(traits)}")
        if char.get("personality"):
            lines.append(f"- Tính cách: {char['personality']}")

        snap = char.get("_latest_snapshot") or {}
        if snap:
            if snap.get("level"):
                lines.append(f"- Cảnh giới: {snap['level']}")
            if snap.get("age"):
                lines.append(f"- Tuổi: {snap['age']}")
            if snap.get("outfit"):
                lines.append(f"- Trang phục: {snap['outfit']}")
            if snap.get("weapon"):
                lines.append(f"- Vũ khí: {snap['weapon']}")
            if snap.get("vfx_vibes"):
                lines.append(f"- VFX: {snap['vfx_vibes']}")
            if snap.get("physical_description"):
                lines.append(f"- Trạng thái: {snap['physical_description']}")

        lines.append("")

    return "\n".join(lines).strip()


def _pass2_delta_extract(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    character_context_rows: list[dict],
) -> ExtractionResult:
    """Run Pass 2 and return ExtractionResult. Raises on parse failure."""
    context_str = _build_character_context(character_context_rows)
    excerpt = _pack_chapters_within_budget(
        batch_text, chapter_start, chapter_end, settings.wiki_pass2_budget
    )
    prompt = _PASS2_USER_TMPL.format(
        start=chapter_start,
        end=chapter_end,
        text=excerpt,
        character_context=context_str,
    )
    _trace_request("pass2", chapter_start, chapter_end, _PASS2_SYSTEM, prompt)
    raw = _ollama_generate(prompt, _PASS2_SYSTEM, settings.wiki_extract_model)
    _trace_response("pass2", chapter_start, chapter_end, raw)
    data = json.loads(raw)

    new_chars = data.get("new_characters", [])
    updated_raw = data.get("updated_characters", [])
    updated = [CharacterPatch.model_validate(p) for p in updated_raw if isinstance(p, dict)]

    return ExtractionResult(
        batch_chapter_start=chapter_start,
        batch_chapter_end=chapter_end,
        new_characters=new_chars,
        updated_characters=updated,
    )


# ---------------------------------------------------------------------------
# Main extraction entry point
# ---------------------------------------------------------------------------

def extract_batch(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    get_characters_by_names_fn,  # callable: (list[str]) -> list[dict]
    get_all_characters_fn,       # callable: () -> list[dict]
    get_latest_snapshot_fn,      # callable: (character_id: str) -> Optional[dict]
    consecutive_fail_counter: list,  # mutable list[int] with one element, e.g. [0]
) -> ExtractionResult:
    """Full two-pass extraction for one batch.

    consecutive_fail_counter is a mutable list[int] so the caller can track
    failures across invocations without global state.

    Raises ExtractionFatalError if consecutive failures exceed the threshold.
    """
    all_chars = get_all_characters_fn()
    wiki_size = len(all_chars)

    try:
        # Decide whether to run Pass 1 or send all characters
        if wiki_size < settings.wiki_context_threshold:
            # Small wiki: skip Pass 1, send all characters directly
            candidate_chars = all_chars
        else:
            # Pass 1 — Name Scan
            name_entries = _pass1_name_scan(batch_text, chapter_start, chapter_end)
            if not name_entries:
                # Fallback: send all characters when Pass 1 returns nothing
                candidate_chars = all_chars
            else:
                names_normalized = [
                    _normalize(entry.name) for entry in name_entries
                ] + [
                    _normalize(alias)
                    for entry in name_entries
                    for alias in entry.aliases
                ]
                candidate_chars = get_characters_by_names_fn(names_normalized)

        # Attach latest_snapshot to each candidate for Pass 2 context
        for char in candidate_chars:
            snap = get_latest_snapshot_fn(char["character_id"])
            char["_latest_snapshot"] = snap

        # Pass 2 — Delta Extract
        result = _pass2_delta_extract(
            batch_text, chapter_start, chapter_end, candidate_chars
        )
        consecutive_fail_counter[0] = 0  # reset on success
        return result

    except ExtractionFatalError:
        raise
    except Exception as exc:
        consecutive_fail_counter[0] += 1
        logger.warning(
            "Extraction fail #{} | batch={}-{} error={}",
            consecutive_fail_counter[0],
            chapter_start,
            chapter_end,
            exc,
        )
        if consecutive_fail_counter[0] > settings.wiki_max_consecutive_fail:
            raise ExtractionFatalError(
                f"Consecutive extraction failures exceeded {settings.wiki_max_consecutive_fail}. "
                f"Last batch: {chapter_start}-{chapter_end}. Manual check required."
            ) from exc
        return ExtractionResult(
            batch_chapter_start=chapter_start,
            batch_chapter_end=chapter_end,
        )


def _normalize(name: str) -> str:
    import unicodedata
    import re

    name = name.replace("đ", "d").replace("Đ", "D")
    name = unicodedata.normalize("NFD", name)
    name = "".join(c for c in name if unicodedata.category(c) != "Mn")
    name = name.lower().strip()
    name = re.sub(r"[^a-z0-9]+", " ", name)
    return re.sub(r"\s+", " ", name).strip()
