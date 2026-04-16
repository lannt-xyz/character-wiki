## Plan: Story Crawl + Wiki Extraction System

**TL;DR:** Crawl từng batch 5 chương → gửi sang LLM trích xuất nhân vật/thuộc tính → merge stateful vào SQLite wiki. Toàn bộ chạy tuần tự, resume-safe, có temporal history theo chapter range.

---

### Phase 1 — Foundation (unblock crawler hiện tại)

**1. Tạo `models/schemas.py`** — unblock scraper.py và storage.py đang import thiếu:
- `ChapterMeta` — chapter_num, title, url, content, status, error_msg
- `CharacterSnapshot` — chapters_range (e.g. `"1-50"`), level, outfit, weapon, vfx_vibes
- `Character` — character_id, name, aliases, traits, relations, status_history: `list[CharacterSnapshot]`
- `ExtractionResult` — new_characters: `list[Character]`, updated_characters: `list[dict]` (partial update), raw_json
- `WikiBatchState` — batch_id, chapter_start, chapter_end, status (PENDING/CRAWLED/EXTRACTED/MERGED)

**2. Tạo `db/database.py`** — SQLite wrapper dùng stdlib `sqlite3`:
- Table `chapters` — chapter_num PK, title, url, file_path, status, crawled_at, error_msg
- Table `wiki_characters` — character_id PK, name_normalized, data JSON (full `Character` object), updated_at
- Table `wiki_batches` — batch_id PK, chapter_start, chapter_end, status, extracted_at, merged_at
- Methods: `upsert_chapter()`, `set_chapter_status()`, `upsert_character()`, `get_character_by_name()`, `get_all_characters()`, `upsert_batch()`, `get_pending_batches()`

**3. Update settings.yaml + settings.py** — thêm:
- `wiki_batch_size: 5` (đổi từ `chapters_per_episode`)
- `wiki_extract_model: "gemma4-32k:latest"` (có thể khác với video pipeline model)
- `wiki_inherit_fields: true` (flag bật/tắt inherit logic)
- `crawler_rate_limit: 1.0` (rps, thêm vào settings.py hiện tại đang thiếu)

---

### Phase 2 — LLM Extraction

**4. Tạo `wiki/extractor.py`** — gọi Ollama qua `httpx` (không dùng SDK):

**Prompt strategy** — gửi kèm existing data để tránh hallucination:
```
Dưới đây là {n} chương mới (Ch {start}-{end}).
Danh sách nhân vật hiện tại: {existing_names_json}
Data cũ để tham chiếu: {existing_characters_json}

Hãy trả về JSON với cấu trúc sau:
- new_characters: nhân vật xuất hiện lần đầu
- updated_characters: nhân vật cũ có thay đổi (chỉ trả field thay đổi + chapter_range mới)
```

- Input: list `ChapterMeta` + `list[Character]` hiện tại từ DB
- Retry với `tenacity` (stop_after_attempt=3, wait_exponential)
- Parse response → validate qua `ExtractionResult` Pydantic model
- Nếu LLM trả non-JSON → log warning + return empty result (không fail toàn batch)

---

### Phase 3 — Wiki Merge

**5. Tạo `wiki/merger.py`** — stateful merge với temporal tracking:

**Upsert logic:**
- Character mới → insert với `status_history[0]` = snapshot này
- Character cũ có thay đổi → append snapshot mới vào `status_history`, đóng chapter_end của snapshot trước
- **Inherit fields**: nếu LLM không trả `outfit` thì copy từ snapshot cuối — logic này ở merger, không phải trong prompt

**Chapter range tracking:**
```python
# snapshot cũ: chapters_range = "1-50"
# batch mới (Ch 51-55) → đóng lại "1-50", mở snapshot mới "51-..."
```

- Normalize tên nhân vật: lowercase + strip dấu câu để dedup aliases
- Conflict resolution: newest wins + log conflict với loguru WARNING

---

### Phase 4 — Orchestrator + CLI

**6. Tạo `wiki/orchestrator.py`** — vòng lặp chính:
```
for batch in pending_batches:
    1. crawl_chapters(batch.start, batch.end)  → dùng crawler/scraper.py
    2. save_chapters(chapters, db)             → dùng crawler/storage.py
    3. mark_batch(CRAWLED)
    4. extract(chapters, existing_characters)  → wiki/extractor.py
    5. mark_batch(EXTRACTED)
    6. merge(extraction_result, db)            → wiki/merger.py
    7. mark_batch(MERGED)
```
- Resume-safe: skip batch đã MERGED khi restart
- Phát hiện batch CRAWLED nhưng chưa EXTRACTED (crash recovery)
- Rate limit giữa batches: sleep sau mỗi crawl (tránh ban IP)

**7. Tạo `main_wiki.py`** — CLI entrypoint:
- `python main_wiki.py` — chạy toàn bộ từ đầu đến cuối (resume từ batch cuối)
- `python main_wiki.py --from-batch 100` — resume từ batch cụ thể
- `python main_wiki.py --dry-run` — crawl + extract, không merge vào DB
- `python main_wiki.py --export` — xuất toàn bộ wiki ra `data/wiki/` (JSON per character)

---

### Relevant Files

- models/schemas.py — **tạo mới** (PhaseChapterMeta + wiki schemas)
- db/database.py — **tạo mới** (SQLite wrapper)
- settings.yaml — thêm wiki fields
- settings.py — thêm wiki fields + `crawler_rate_limit`
- scraper.py — không cần sửa (đã dùng ChapterMeta)
- storage.py — không cần sửa
- wiki/extractor.py — **tạo mới**
- wiki/merger.py — **tạo mới**
- wiki/orchestrator.py — **tạo mới**
- main_wiki.py — **tạo mới**

---

### Verification

1. `python -c "from models.schemas import ChapterMeta, Character; print('OK')"` — schema import OK
2. `python -c "from db.database import SQLiteDB; db = SQLiteDB('db/test.db'); print(db.get_pending_batches())"` — DB init OK
3. `python main_wiki.py --from-batch 1 --dry-run` — crawl Ch 1-5, extract, log output, không write DB
4. `python main_wiki.py --from-batch 1` — chạy Ch 1-5 thật, kiểm tra `data/wiki/` có JSON nhân vật
5. Restart `python main_wiki.py` — xác nhận batch 1 bị skip (MERGED), tiếp tục từ batch 2
6. Sau ~50 chapters: kiểm tra `status_history` của main character có đúng chapter ranges không

---

### Decisions

- **Wiki storage**: SQLite JSON column thay vì graph DB — đủ dùng cho query theo chapter, không over-engineer
- **Batch size 5**: giữ nguyên `chapters_per_episode: 5` trong config, wiki dùng cùng giá trị
- **Inherit logic**: ở merger layer, không nhét vào prompt — giảm token + dễ debug hơn
- **Conflict resolution**: newest wins + WARNING log, không block pipeline
- **Scope excluded**: video pipeline (ComfyUI, TTS, FFmpeg) — wiki chạy độc lập, video pipeline query wiki sau

---

### Further Considerations

1. **LLM chọn model**: `gemma4-32k:latest` (32k context = đủ cho 5 chương ~10k token + existing wiki JSON). Nếu wiki JSON lớn dần (1000+ nhân vật) → cần giới hạn chỉ gửi nhân vật *xuất hiện* trong batch đó thay vì toàn bộ.
2. **Export format cho Video Pipeline**: wiki character JSON có thể dùng trực tiếp làm context cho image gen (IPAdapter character reference). Nên thống nhất schema `Character.status_history[n].vfx_vibes` format với ComfyUI prompt builder từ sớm.
