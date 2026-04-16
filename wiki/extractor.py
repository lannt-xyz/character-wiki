"""wiki/extractor.py — Two-pass LLM extraction via Ollama REST.

Pass 1 — Name Scan (small request):
    Send raw batch text + short prompt, ask LLM to list characters + aliases.
    Skip Pass 1 when total wiki size < wiki_context_threshold characters.

Pass 2 — Delta Extract:
    Normalize names from Pass 1, lookup existing characters, send compact context
    + batch text, return ExtractionResult (new_characters + updated_characters).
"""

import json
from typing import Optional

import httpx
from loguru import logger
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential

from config.settings import settings
from models.schemas import CharacterPatch, ExtractionResult, NameEntry


class ExtractionFatalError(Exception):
    """Raised when consecutive fail counter exceeds wiki_max_consecutive_fail."""


# ---------------------------------------------------------------------------
# Prompts
# ---------------------------------------------------------------------------

_PASS1_SYSTEM = (
    "Bạn là trợ lý phân tích truyện. Nhiệm vụ: liệt kê tên nhân vật xuất hiện trong đoạn text "
    "dưới đây kèm biệt danh/cách gọi khác. Chỉ trả JSON array, không giải thích thêm."
)

_PASS1_USER_TMPL = """\
Đoạn truyện (Chương {start}-{end}):
---
{text}
---
Trả về JSON array với cấu trúc: [{{"name": "...", "aliases": ["...", "..."]}}]
Chỉ tên nhân vật cụ thể, không tên địa danh hay tên sự vật."""

_PASS2_SYSTEM = (
    "Bạn là trợ lý xây dựng wiki nhân vật. Nhiệm vụ: trích xuất sự thay đổi trạng thái nhân vật "
    "từ đoạn truyện mới. Chỉ trả JSON theo schema quy định, không giải thích, không markdown."
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
        "traits": ["<tính cách>"],
        "relations": [{{"related_name": "...", "description": "...", "chapter_start": {start}}}],
        "visual_anchor": "<đặc điểm ngoại hình cố định: sẹo, dị tật, đặc trưng>  hoặc null"
      }},
      "snapshot": {{
        "chapter_start": {start},
        "is_active": true,
        "level": "<cảnh giới hoặc null>",
        "outfit": "<trang phục hoặc null>",
        "weapon": "<vũ khí hoặc null>",
        "vfx_vibes": "<mô tả hiệu ứng hình ảnh hoặc null>",
        "physical_description": "<trạng thái thể chất tạm thời hoặc null>",
        "visual_importance": <1-10>
      }}
    }}
  ],
  "updated_characters": [
    {{
      "character_id": "<id nhân vật cũ>",
      "level": "<cảnh giới mới hoặc null nếu không đổi>",
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
- updated_characters chỉ chứa nhân vật CŨ (đã có trong context), không chứa nhân vật mới"""


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
    with httpx.Client(timeout=settings.llm_timeout) as client:
        resp = client.post(f"{settings.ollama_url}/api/generate", json=payload)
        resp.raise_for_status()
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
# Pass 1 — Name Scan
# ---------------------------------------------------------------------------

def _pass1_name_scan(batch_text: str, chapter_start: int, chapter_end: int) -> list[NameEntry]:
    """Return list of NameEntry from the batch text. Empty list on parse failure."""
    prompt = _PASS1_USER_TMPL.format(
        start=chapter_start, end=chapter_end, text=batch_text[:8000]
    )
    try:
        raw = _ollama_generate(prompt, _PASS1_SYSTEM, settings.wiki_extract_model)
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
    """Build compact context JSON for Pass 2 prompt."""
    compact = []
    for char in characters:
        compact.append({
            "character_id": char["character_id"],
            "name": char["name"],
            "aliases": json.loads(char.get("aliases_json", "[]")),
            "visual_anchor": char.get("visual_anchor"),
            "latest_snapshot": char.get("_latest_snapshot"),
        })
    return json.dumps(compact, ensure_ascii=False, indent=None)


def _pass2_delta_extract(
    batch_text: str,
    chapter_start: int,
    chapter_end: int,
    character_context_rows: list[dict],
) -> ExtractionResult:
    """Run Pass 2 and return ExtractionResult. Raises on parse failure."""
    context_str = _build_character_context(character_context_rows)
    prompt = _PASS2_USER_TMPL.format(
        start=chapter_start,
        end=chapter_end,
        text=batch_text[:12000],
        character_context=context_str,
    )
    raw = _ollama_generate(prompt, _PASS2_SYSTEM, settings.wiki_extract_model)
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
    return unicodedata.normalize("NFC", name).lower().strip()
