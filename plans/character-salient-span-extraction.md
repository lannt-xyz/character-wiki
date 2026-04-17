## Plan: Per-Character Salient-Span Extraction

**TL;DR:** Thay thế Phase 3 batch-by-chapter bằng per-character loop. Xây một **global mention index** qua toàn bộ chapters một lần → nhóm mentions thành segments 20ch → mỗi LLM call = 1 nhân vật × 1 segment × extracted spans (chỉ đoạn text đề cập nhân vật đó). Context seed từ file `.md` thay vì DB fragments. Kết quả overwrite `extraction_version=2`.

**Scope constraints (confirmed):**
- DB hiện có ~600 characters → xử lý toàn bộ `is_deleted=0`, không giới hạn top-N
- Phase 1 luôn **rebuild từ đầu** (full reset, không dùng incremental resume)
- Phase 3 chạy **sequential** (1 LLM call tại một thời điểm, Ollama single GPU)
- `wiki_remaster.py` chỉ dùng cho remaster — Phase 3 cũ được replace hoàn toàn

---

### Phase 1 — Init: Schema + Mention Index + Char Batches

**Step 1 — DB schema** (`database.py` → `_create_tables()`)
Thêm 2 bảng mới:
- `wiki_mention_index (id INTEGER PK AUTOINCREMENT, character_id TEXT, chapter_num INTEGER)` — UNIQUE(character_id, chapter_num)
- `wiki_char_batches (batch_id INTEGER PK AUTOINCREMENT, character_id TEXT, segment_start INTEGER, segment_end INTEGER, remaster_version INTEGER DEFAULT 2, status TEXT DEFAULT 'PENDING', extracted_at TEXT, merged_at TEXT)`

**Step 2 — DB methods** (`database.py`) — *parallel với Step 5*
- `build_mention_index(character_id, chapter_nums: list[int])` — bulk INSERT OR IGNORE
- `get_mention_chapters(character_id) -> list[int]`
- `clear_mention_index()` — DELETE all rows
- `build_char_batches(batches: list[dict])` — bulk INSERT; fields: character_id, segment_start, segment_end
- `clear_char_batches()` — DELETE all rows
- `get_pending_char_batches() -> list[dict]`
- `set_char_batch_status(batch_id, status)`
- `count_char_batches_merged() -> int`, `count_char_batches_total() -> int`
- `get_character(character_id) -> dict | None` — single row fetch
- `get_all_active_characters() -> list[dict]` — WHERE is_deleted=0, no limit
- Extend `reset_remaster_v2()` → thêm `clear_char_batches()` + `clear_mention_index()`

**Step 3 — Helper functions** (`wiki_remaster.py`) — *depends on Step 1*
- `_build_mention_index(db, all_chars)`:
  1. **Pre-load** toàn bộ chapter content vào `dict[int, str]` bằng 1 DB query (`SELECT chapter_num, content FROM chapters`)
  2. **Pre-normalize** mỗi chapter text một lần duy nhất → `dict[int, str]` normalized
  3. Loop 600 chars × check normalized chapter texts → collect `chapter_nums` per char
  4. `db.build_mention_index(character_id, chapter_nums)` — bulk INSERT mỗi char
  5. Log progress mỗi 50 chars: `"_build_mention_index | {i}/{total} chars done"`
- `_group_char_segments(chapter_nums: list[int], segment_size: int, gap_threshold: int) -> list[tuple[int, int]]` — nhóm chapters liên tục thành `(start, end)` tuples, tách khi gap > gap_threshold

**Step 4 — Replace `phase1_init_batches()`** (`wiki_remaster.py`) — *depends on Steps 2, 3*
1. `reset_remaster_v2()` → purge all v2 data + clear index + clear char_batches (full reset)
2. `all_chars = db.get_all_active_characters()` — tất cả 600 chars, không giới hạn
3. `_build_mention_index(db, all_chars)`
4. For each char: `get_mention_chapters()` → `_group_char_segments(segment_size=settings.char_segment_size, gap_threshold=settings.char_gap_threshold)` → `build_char_batches()`
5. `_clear_llm_trace_dir()`
6. Log summary: `"Phase 1 done | {len(all_chars)} chars | {total_batches} char_batches"`

**Step 4b — New settings keys** (`config/settings.yaml`)
```yaml
char_segment_size: 20       # chapters per char batch
char_gap_threshold: 50      # chapter gap before splitting a new segment
char_span_budget: 20000     # max chars of extracted spans sent to LLM (leaves room for seed context)
```

---

### Phase 3 — Per-Character Extraction Loop

**Step 5 — New Pydantic models** (`models/schemas.py`) — *parallel với Step 2*
- `CharBatchSnapshot` — chapter_start + all temporal fields (level, outfit, weapon, vfx_vibes, physical_description, visual_importance, is_active)
- `CharPassResult` — character_id: str, snapshots: list[CharBatchSnapshot], artifact_updates: list[dict], new_aliases: list[str]

**Step 6 — New helper functions** (`wiki_remaster.py`) — *depends on Step 3*
- `_load_chapters_by_range(db, start, end) -> dict[int, str]` — `SELECT chapter_num, content FROM chapters WHERE chapter_num BETWEEN start AND end`, trả về `{chapter_num: content}`
- `_load_character_seed_context(character_id, char_row, db) -> str` — đọc `data/rerun/character_input/{character_id}.md` nếu tồn tại, fallback về `_build_character_markdown()` từ DB v1 snapshots
- `_extract_character_spans(chapter_texts: dict[int, str], character_id, char_row, budget: int = settings.char_span_budget) -> str`:
  - Split mỗi chapter text theo paragraph `\n\n`
  - Giữ paragraphs có mention tên/alias của nhân vật (dùng `_text_has_phrase()`)
  - **Context window**: cũng giữ paragraph đứng ngay trước paragraph match (tác giả thường mô tả ngoại hình ở câu trước tên nhân vật)
  - Đánh dấu `--- Chương N ---` trước mỗi chapter block
  - Truncate tổng đến `budget` chars
  - Return empty string nếu không có paragraph nào match

**Step 7 — New prompt template** (`wiki_remaster.py`) — *parallel với Step 6*
- `_REMASTER_CHAR_PASS_TMPL` — focus vào 1 nhân vật: "Nhân vật focus: {name} [{character_id}] / Context seed: {seed_context} / Pháp khí: {artifact_context} / Đoạn truyện: {spans_text}"
- `_REMASTER_CHAR_PASS_SYSTEM` — system: "Trích xuất wiki cho MỘT nhân vật duy nhất. Trả về JSON."

**Step 8 — New core function `_remaster_char_pass()`** (`wiki_remaster.py`) — *depends on Steps 5, 7*
- Signature: `(batch_id, character_id, segment_start, segment_end, char_context, artifact_context, spans_text) -> CharPassResult`
- Format prompt → `_save_llm_request()` → `_ollama_generate()` → `_save_llm_response()` → parse JSON → validate as `CharPassResult`
- LLM trace path: `data/rerun/trace/{character_id}/{batch_id}.md` (thay vì flat `data/llm_requests/`) để dễ debug per-character

**Step 9 — New merge function `_merge_char_pass_result()`** (`wiki_remaster.py`) — *depends on Step 5*
- Ghi từng snapshot vào `wiki_snapshots` với `extraction_version=2`
- Update `aliases_json`: **dedup-append** (existing aliases + new_aliases, normalize + deduplicate bằng `_normalize()`)
- **Alias drift logging**: nếu `new_aliases` trả về alias chưa có trong DB, log WARNING: `"New alias discovered | char={} alias={} — may need index rebuild"`. Không tự rebuild index (user quyết định).
- Gọi `_merge_artifact_updates()` (existing)

**Step 10 — Replace `phase3_extraction_loop()` → `phase3_char_extraction_loop()`** — *depends on Steps 6, 8, 9*

> `phase3_extraction_loop()` cũ được rename thành `_phase3_legacy_loop()` với docstring `# LEGACY — replaced by phase3_char_extraction_loop`. Không xóa để tham chiếu.

Loop `get_pending_char_batches()` (sequential):
1. `_load_chapters_by_range()` → dict chapter texts
2. `_load_character_seed_context()` → char seed markdown
3. `_extract_character_spans()` → filtered spans
   - Nếu spans rỗng → `set_char_batch_status(batch_id, 'MERGED')`, continue (không gọi LLM)
4. `_select_candidate_artifacts()` + `_filter_artifacts_against_character_names()`
5. `_remaster_char_pass()` → `CharPassResult`
6. `_merge_char_pass_result()` → DB
7. `set_char_batch_status(batch_id, 'MERGED')`

**Step 11 — Update CLI** (`wiki_remaster.py` `main()`)
- `--from-phase 3` gọi `phase3_char_extraction_loop()` thay vì `phase3_extraction_loop()`
- `--stats` dùng `count_char_batches_merged()` / `count_char_batches_total()`
- Phase 4 (`phase4_final_synthesis`) được giữ nguyên — scope mở rộng: đổi `limit=20` → dùng `get_all_active_characters()` nhưng **ưu tiên `visual_importance >= 6` trước** để Video Pipeline có dữ liệu ngay; nhân vật phụ chạy sau

---

### Relevant Files
- `wiki_remaster.py` — `phase1_init_batches` (replace), `phase3_extraction_loop` (legacy rename), thêm helpers mới
- `wiki_remaster.py` — `_build_character_markdown()` (reuse as fallback seed context)
- `wiki_remaster.py` — `_text_has_phrase()` (reuse trong `_build_mention_index` + `_extract_character_spans`)
- `database.py` — `reset_remaster_v2()` (extend), thêm methods cho mention index + char batches
- `database.py` — thêm `get_all_active_characters()`
- `models/schemas.py` — thêm `CharBatchSnapshot` + `CharPassResult`
- `config/settings.yaml` — thêm `char_segment_size`, `char_gap_threshold`, `char_span_budget`
- `config/settings.py` — thêm 3 fields tương ứng
- `data/rerun/character_input/*.md` — seed files (đã có ~20, không need thay đổi)

---

### Verification
1. `uv run python3 -m py_compile wiki_remaster.py db/database.py models/schemas.py` — pass
2. `uv run pytest tests/ -v` — existing tests pass
3. `uv run python3 wiki_remaster.py --from-phase 1 --dry-run` — log shows: 600 chars processed, mention index count, total char_batches count
4. `uv run python3 wiki_remaster.py --stats` — N char_batches total, 0 merged
5. `uv run python3 wiki_remaster.py --from-phase 3 --max-batches 3` — inspect 3 trace `.md` files: prompt chỉ có 1 nhân vật focus, spans đã filtered
6. Verify DB: `SELECT COUNT(*) FROM wiki_snapshots WHERE extraction_version=2` > 0
7. Verify empty-spans skip: `SELECT COUNT(*) FROM wiki_char_batches WHERE status='MERGED'` tăng ngay cả khi LLM không được gọi
8. Full run hoàn thành không crash qua toàn bộ char_batches

---

### Notes

- **`wiki_mention_index` build**: Pre-load tất cả chapters vào RAM trước, normalize một lần → tránh 600×3534 = 2.1M DB round-trips. Bộ nhớ ước tính ~3534 chapters × ~5KB avg = ~17MB, acceptable.
- **`wiki_char_batches` total estimate**: 600 chars × avg ~3 segments = ~1800 char_batches. Runtime với gemma4-32k sequential: ~30–90s/batch → ~15–45h full run.
- **`wiki_remaster_batches` table**: Giữ nguyên trong DB (không drop), chỉ legacy-rename function trong code.
- **Phase 4 scope**: Mở rộng từ `limit=20` lên `get_all_active_characters()`, ưu tiên `visual_importance >= 6` trước.
- **`new_aliases` merge**: Dedup-append — normalize cả existing aliases và new_aliases bằng `_normalize()`, bỏ duplicate, INSERT chỉ entries mới.
- **LLM trace structure**: `data/rerun/trace/{character_id}/{batch_id}.md` — dễ kiểm tra tại sao 1 nhân vật bị extract sai.
- **Context window cho span extraction**: Luôn include paragraph ngay trước paragraph match để không mất mô tả ngoại hình do tác giả đặt ở câu trước tên nhân vật.
- **Alias drift**: Khi Phase 3 phát hiện alias mới, log WARNING nhưng không tự rebuild index. Nếu alias đó quan trọng, user chạy lại Phase 1 để rebuild toàn bộ.

### Risks

| Risk | Mức độ | Cách xử lý |
|---|---|---|
| Token overflow | Thấp | `char_span_budget: 20000` giới hạn spans trước khi gửi LLM |
| Long runtime (45h) | Trung bình | Resume-safe: `PENDING → MERGED`, dừng/chạy lại bất kỳ lúc nào |
| Consistency drift | Thấp | Context seed v1 gửi kèm mỗi request làm anchor |
| DB lock | Thấp | Sequential loop — chỉ 1 writer tại 1 thời điểm |
| Alias drift (index miss) | Thấp | Log WARNING khi phát hiện alias mới, user quyết định rebuild |

