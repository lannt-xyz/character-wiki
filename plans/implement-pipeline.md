# Story Crawl + Wiki Extraction Pipeline — Full Implementation

## Status: Done ✅

## Mục tiêu
Implement hệ thống crawl truyện + trích xuất wiki nhân vật tự động theo batch 5 chương.
Output: Raw story store (3534 chương) + Character Wiki (JSON per nhân vật, có temporal history theo chapter range).

---

## Roadmap

### 1. Project Bootstrap
- [x] Tạo cấu trúc thư mục: `models/`, `db/`, `wiki/`, `data/chapters/`, `data/wiki/`, `logs/`
- [x] `pyproject.toml` — uv dependencies: httpx, pydantic, pydantic-settings[yaml], loguru, tenacity, beautifulsoup4, lxml
- [x] Update `config/settings.yaml` — thêm: `wiki_batch_size`, `wiki_extract_model`, `crawler_rate_limit`, `crawler_delay_sec`, `wiki_context_threshold`, `wiki_max_consecutive_fail`, `wiki_snapshot_min_change`
- [x] Update `config/settings.py` — thêm fields tương ứng + `get_chapter_url()` helper

### 2. Models
- [x] `models/schemas.py` — Pydantic BaseModel cho toàn bộ pipeline:
  - `ChapterMeta` — chapter_num, title, url, content, status, error_msg
  - `CharacterSnapshot` — chapter_start, extraction_version, is_active (bool, default True), level, outfit, weapon, vfx_vibes *(Persistent — inherit nếu null)*, physical_description *(Transient — không inherit, reset NULL nếu batch sau không nhắc)*, visual_importance *(int 1-10, dùng cho Video Pipeline ưu tiên Cref)* *(không có chapter_end — truly append-only, end suy ra bằng snapshot tiếp theo)*
  - `CharacterPatch` — character_id, level, outfit, weapon, vfx_vibes, physical_description, visual_importance, is_active, aliases *(tất cả `Optional` — LLM chỉ trả field thay đổi; `physical_description=null` = không transient nào, không phải "không có thay đổi")*
  - `Character` — character_id, name, name_normalized, aliases, traits, relations, visual_anchor *(mô tả ngoại hình gốc không đổi: sẹo, nốt ruồi, vocóc dáng — luôn gửi kèm trong Pass 2 context)*
  - `ExtractionResult` — new_characters: `list[Character+snapshot]`, updated_characters: `list[CharacterPatch]`, batch_chapter_start, batch_chapter_end
  - `WikiBatchState` — batch_id *(= chapter_start, deterministic key)*, chapter_start, chapter_end, status, extraction_version

### 3. Database Layer
- [x] `db/database.py` — SQLite wrapper; **4 normalized tables**:

  ```sql
  -- Identity-only, không chứa temporal data
  wiki_characters  (character_id PK, name, name_normalized UNIQUE, aliases_json, traits_json,
                    visual_anchor TEXT,   -- ngoại hình gốc không đổi, luôn gửi kèm Pass 2
                    created_at, updated_at)

  -- Truly append-only — không có chapter_end, không bao giờ UPDATE row cũ
  -- "End" của snapshot = chapter_start của snapshot kế tiếp cùng character (suy ra ngầm)
  wiki_snapshots   (id PK AUTOINCREMENT, character_id FK, chapter_start INT,
                    is_active BOOLEAN DEFAULT 1,   -- 0 khi nhân vật chết/biến mất
                    level, outfit, weapon, vfx_vibes,         -- Persistent: inherit nếu null
                    physical_description,                      -- Transient: reset NULL mỗi batch
                    visual_importance INT, extraction_version INT, created_at)

  -- Append-only relations
  wiki_relations   (id PK AUTOINCREMENT, character_id FK, related_name, description, chapter_start INT)

  -- Pipeline state
  chapters         (chapter_num PK, title, url, file_path, status, crawled_at, error_msg)
  wiki_batches     (batch_id INT PK,   -- = chapter_start, deterministic
                    chapter_start INT, chapter_end INT, status, extraction_version INT, extracted_at, merged_at)
  ```

  - **Index**: `CREATE INDEX idx_snap_char_ch ON wiki_snapshots(character_id, chapter_start DESC)` ← bắt buộc cho time-travel query
  - `upsert_chapter()`, `set_chapter_status()`
  - `upsert_character()` — insert or update identity fields only
  - `add_snapshot()` — INSERT only, tuyệt đối không UPDATE
  - `get_snapshot_at(character_id, chapter_num)` — time-travel: `WHERE character_id=? AND chapter_start<=? ORDER BY chapter_start DESC LIMIT 1`
  - `get_latest_snapshot(character_id)` — snapshot mới nhất: `ORDER BY chapter_start DESC LIMIT 1`
  - `get_characters_by_names(names: list[str])` — bulk fetch theo `name_normalized` list
  - `get_character_by_name(name_normalized)`, `get_all_characters()`
  - `upsert_batch()`, `get_pending_batches()`

### 4. Crawler (đã có skeleton)
- [x] Đảm bảo `crawler/scraper.py` import OK sau khi `models/schemas.py` tồn tại
- [x] Đảm bảo `crawler/storage.py` nhận `db: SQLiteDB` instance đúng

### 5. Wiki Extraction Module
- [x] `wiki/extractor.py` — Ollama REST call via httpx (không dùng SDK); dùng `format: "json"` trong API call để force JSON output; **Two-pass extraction**:
  - **Pass 1 — Name Scan** *(request nhỏ)*: gửi raw text batch + prompt ngắn "Liệt kê tên nhân vật được nhắc đến kèm biệt danh/cách gọi khác, trả JSON array of `{name, aliases[]}`". Output: `list[{name, aliases}]` tên thô.
  - **Pass 2 — Delta Extract**: normalize tên + aliases từ Pass 1 → `get_characters_by_names()` (lookup cả trong `aliases_json`) → load `get_latest_snapshot()` của từng candidate → gửi compact context JSON + text → trả `ExtractionResult`
  - Khi wiki < `wiki_context_threshold` characters (default 50): skip Pass 1, gửi tất cả trực tiếp
  - tenacity retry max 3, wait_exponential cho mỗi pass
  - Parse fail → log WARNING, return empty result; **consecutive fail counter**: nếu > `wiki_max_consecutive_fail` (default 5) liên tiếp → raise `ExtractionFatalError`, dừng pipeline — không im lặng skip vô hạn
- [x] `wiki/merger.py` — merge ExtractionResult vào DB, **append-only tuyệt đối** (không UPDATE bao giờ):
  - Character mới → `upsert_character()` + `add_snapshot(chapter_start=batch_start, extraction_version=V)`
  - Character cũ có thay đổi → `get_latest_snapshot()` làm base → **Inherit có chọn lọc**:
    - **Persistent fields** (level, outfit, weapon, vfx_vibes): copy từ base nếu `CharacterPatch` trả `None`
    - **Transient fields** (physical_description): **không inherit** — nếu `CharacterPatch` không trả → lưu `NULL` (trạng thái tạm thời kết thúc)
    - `is_active`: inherit, chỉ thay đổi khi LLM explicit trả `is_active: false`
    - so sánh merged values với base: nếu số field khác < `wiki_snapshot_min_change` (default 1) → **bỏ qua, không tạo snapshot rác** → nếu đủ thay đổi → `add_snapshot()` với merged values
  - Character cũ không thay đổi → **không làm gì** — snapshot cũ vẫn valid
  - **Alias dedup**: khi character mới xuất hiện, merge `aliases` vào `aliases_json` của `wiki_characters`; khi lookup Pass 1 output → check cả `aliases_json` để map đúng `character_id` thay vì tạo duplicate
  - Normalize tên: `unicodedata.normalize('NFC', name).lower().strip()` để dedup aliases
  - Log mỗi batch: `logger.info("Batch {} | +{} new, {} updated, {} skipped (no change)", batch_id, n_new, n_updated, n_skipped)`
- [x] `wiki/orchestrator.py` — vòng lặp chính batch-by-batch:
  - Resume-safe: skip MERGED batch; crash recovery: re-extract CRAWLED batch
  - **Progress & ETA**: track `batch_start_time`, rolling average 10 batch gần nhất → ETA; log mỗi batch: `[Batch 45/707 | 6.4% | ETA ~5h32m] MERGED | +2 new, 5 updated`
  - `settings.crawler_delay_sec` sleep sau mỗi crawl — configurable
  - Consecutive fail safeguard: catch `ExtractionFatalError` → log batch cần manual check → dừng

### 6. Entry Point
- [x] `main_wiki.py` — CLI argparse; logger setup (loguru, rotate 100MB); delegate to orchestrator
  - `python main_wiki.py` — chạy full từ batch đầu tiên chưa MERGED
  - `python main_wiki.py --from-batch 100` — resume từ batch có `chapter_start=100` (deterministic)
  - `python main_wiki.py --dry-run` — crawl + extract, không write wiki vào DB
  - `python main_wiki.py --export` — xuất wiki ra `data/wiki/{name_normalized}.json` (filename safe)
  - `python main_wiki.py --stats` — in tiến độ: `X/707 batches MERGED (Y%), Z characters, W snapshots`; không chạy pipeline

### 7. Validation
- [x] `wiki/validator.py` — post-merge per-batch sanity check: mỗi character có ít nhất 1 snapshot, không có duplicate `chapter_start` cho cùng character, `extraction_version` consistent trong batch

### Testing & Verification
- [x] `tests/test_schemas.py` — validate ChapterMeta, Character, ExtractionResult, CharacterPatch schema
- [x] `tests/test_merger.py` — test inherit logic, append-only behavior (không UPDATE), dedup, extraction_version, alias dedup (không tạo duplicate character), snapshot_min_change threshold
- [x] `tests/test_extractor.py` — mock Ollama two-pass response, test parse fail → empty result, consecutive fail counter
- [x] `tests/test_db.py` — test `get_snapshot_at()` time-travel, test index với `EXPLAIN QUERY PLAN`
- [x] Smoke test: `python main_wiki.py --stats` chạy không crash

---

## Acceptance Criteria
- [ ] `python main_wiki.py --dry-run` chạy không crash, crawl Ch 1-5, log extraction output
- [ ] `python main_wiki.py --from-batch 1` tạo ít nhất 1 character JSON trong `data/wiki/`
- [ ] Restart `python main_wiki.py` — batch 1 bị skip (MERGED), tiếp batch 2
- [ ] `python main_wiki.py --stats` in tiến độ đúng, không chạy pipeline
- [ ] Sau 50 chương: `get_snapshot_at(char_id, 120)` trả đúng snapshot cho chương 120
- [ ] DB verify: `SELECT COUNT(*) FROM wiki_snapshots` chỉ tăng, không có UPDATE — append-only
- [ ] 5 batch LLM fail liên tiếp → pipeline dừng với error rõ ràng, không im lặng skip
- [ ] ETA log hiển thị mỗi batch: `[Batch X/707 | Y% | ETA ~Zh Wm]`
- [ ] `pytest tests/ -v` tất cả pass
- [ ] Không có hardcoded story slug, URL, hoặc model name trong code
- [ ] Inherit logic: outfit không thay đổi → `CharacterPatch.outfit=None` → merger copy từ latest snapshot
- [ ] Export filename: `lâm-phong.json` (name_normalized, space→hyphen, no unsafe chars)

## Architecture Decisions
- **Wiki storage**: SQLite normalized tables — `wiki_snapshots` truly append-only, không có `chapter_end` column, time-travel query bằng `ORDER BY chapter_start DESC LIMIT 1 WHERE chapter_start <= N`
- **Tại sao không JSON blob**: Blob rewrite toàn bộ object mỗi update; không query bằng SQL được; không diff; không scale khi có 1000+ nhân vật × 707 batches
- **Snapshot lifecycle — truly append-only**: Không bao giờ UPDATE/DELETE row cũ. "End" của snapshot suy ra ngầm = `chapter_start` của snapshot kế tiếp cùng character. Không cần lưu `chapter_end` trực tiếp.
- **System prompt change recovery**: `extraction_version INT` trong `wiki_snapshots` và `wiki_batches`. Khi đổi prompt → bump version → re-mark các batch bị ảnh hưởng về `CRAWLED` → re-extract → INSERT snapshot mới với version mới. Snapshot cũ vẫn còn nguyên trong DB.
- **LLM context — Two-pass để tránh context overflow**: Pass 1 nhỏ (chỉ lấy tên), Pass 2 chỉ gửi latest snapshot của những tên được nhắc. Dứt khoát không gửi toàn bộ wiki → vô nghĩa từ chapter 200 trở đi.
- **LLM JSON reliability**: `format: "json"` trong Ollama API; consecutive fail counter dừng sau `wiki_max_consecutive_fail` lần liên tiếp thay vì im lặng skip.
- **batch_id = chapter_start**: deterministic key, không thay đổi kể cả khi DB bị xóa tạo lại.
- **Inherit logic**: ở `merger.py` — LLM chỉ trả `CharacterPatch` (delta), merger load latest snapshot làm base, override field không None.
- **Persistent vs Transient fields**: Persistent (level, outfit, weapon, vfx_vibes) — inherit khi null. Transient (physical_description) — không inherit, reset NULL mỗi batch nếu không được nhắc. Tránh lỗi "nhân vật vẫn bị thương" 200 chương sau khi đã lành.
- **visual_anchor**: field cố định trong `wiki_characters` (sẹo, cóc dáng, dị tật...). Luôn gửi kèm trong Pass 2 context thành phần `character_identity` — giúp LLM không “mù lòa” nhân vật qua hàng ngàn chương.
- **is_active flag**: trong snapshot, default True. LLM set False khi nhân vật chết/biến mất. Video Pipeline lọc `WHERE is_active = 1` để không render nhân vật đã chết.
- **Batch size**: `wiki_batch_size: 5` trong `settings.yaml`
- **Stateful batch tracking**: `PENDING → CRAWLED → EXTRACTED → MERGED` trong `wiki_batches` table

## DB Schema (tham khảo)

```sql
-- Static identity (visual_anchor không đổi suốt truyện)
INSERT INTO wiki_characters VALUES ('main_001', 'Lâm Phong', 'lâm phong', '["Lâm sư huynh"]', '["Kiên trì","Quyết đoán"]',
  'Nam, cao gầy, tóc đen, vết sẹo dọc cánh tay phải', ...);

-- Batch 1 (Ch 1-5): xuất hiện lần đầu
INSERT INTO wiki_snapshots(character_id, chapter_start, is_active, level, outfit, weapon, vfx_vibes, physical_description, visual_importance, extraction_version)
VALUES ('main_001', 1, 1, 'Luyện Khí tầng 1', 'Vải thô xám', 'Không', 'Ánh sáng trắng mờ', 'Đang bị thương cánh tay trái', 8, 1);

-- Batch 3 (Ch 11-15): vết thương lành, không nhắc nữa → physical_description = NULL (transient reset)
INSERT INTO wiki_snapshots(character_id, chapter_start, is_active, level, outfit, weapon, vfx_vibes, physical_description, visual_importance, extraction_version)
VALUES ('main_001', 11, 1, 'Luyện Khí tầng 1', 'Vải thô xám', 'Không', 'Ánh sáng trắng mờ', NULL, 8, 1);

-- Batch 11 (Ch 51-55): level thay đổi → chỉ INSERT, không UPDATE gì cả
INSERT INTO wiki_snapshots(character_id, chapter_start, is_active, level, outfit, weapon, vfx_vibes, physical_description, visual_importance, extraction_version)
VALUES ('main_001', 51, 1, 'Trúc Cơ', 'Trường bào xanh', 'Thanh Phong Kiếm', 'Kiếm khí sắc bén', NULL, 8, 1);

-- Time-travel query: nhân vật ở chương 120 mặc gì?
SELECT s.*, c.visual_anchor FROM wiki_snapshots s
JOIN wiki_characters c ON c.character_id = s.character_id
WHERE s.character_id = 'main_001' AND s.chapter_start <= 120
ORDER BY s.chapter_start DESC LIMIT 1;
-- → row chapter_start=51, outfit='Trường bào xanh', visual_anchor='Nam, cao gầy...' ✓

-- System prompt đổi → bump version, re-extract, INSERT snapshot mới cùng character
INSERT INTO wiki_snapshots(character_id, chapter_start, ..., extraction_version)
VALUES ('main_001', 51, ..., 2);  -- snapshot cũ version=1 vẫn còn, không bị xóa
-- Query mặc định lọc theo version hiện tại của wiki_batches
```

## Export Schema (data/wiki/{name}.json)
```json
{
  "character_id": "main_001",
  "name": "Lâm Phong",
  "aliases": [],
  "traits": ["Kiên trì", "Quyết đoán"],
  "relations": [{"related_name": "Vân Lam", "description": "Hôn thê", "chapter_start": 1, "chapter_end": 10}],
  "snapshots": [
    {"chapter_start": 1,  "chapter_end": 10,  "is_active": true,  "level": "Luyện Khí tầng 1", "outfit": "Vải thô xám",   "weapon": "Không",            "vfx_vibes": "Ánh sáng trắng mờ",  "physical_description": "Đang bị thương cánh tay trái", "visual_importance": 8},
    {"chapter_start": 11, "chapter_end": 50,  "is_active": true,  "level": "Luyện Khí tầng 1", "outfit": "Vải thô xám",   "weapon": "Không",            "vfx_vibes": "Ánh sáng trắng mờ",  "physical_description": null, "visual_importance": 8},
    {"chapter_start": 51, "chapter_end": null, "is_active": true,  "level": "Trúc Cơ",         "outfit": "Trường bào xanh", "weapon": "Thanh Phong Kiếm", "vfx_vibes": "Kiếm khí sắc bén", "physical_description": null, "visual_importance": 8}
  ]
}
```

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| LLM trả non-JSON dù có `format:json` | Medium | Medium | consecutive fail counter; few-shot example trong prompt |
| Site thay đổi HTML structure → scraper fail | Medium | High | `_CONTENT_SELECTORS` fallback chain đã có; alert khi content < 100 chars |
| DB corrupt giữa chừng | Low | High | Rollback: xóa `wiki_snapshots` + `wiki_characters` có `created_at > T`, reset batch status về CRAWLED |
| Wiki quá lớn → Pass 1 miss tên viết tắt/biệt danh | Medium | Medium | Pass 1 trả kèm `aliases[]`; merger merge vào `aliases_json`; lookup dùng cả aliases |
| Snapshot rác do LLM lặp mô tả cũ | Medium | Low | `wiki_snapshot_min_change` threshold; merger skip nếu không đủ field thay đổi |
| Extraction version drift | Low | Medium | `extraction_version` column; re-extract chỉ cần reset batch status |

## Rollback Plan

```sql
-- Rollback về trước batch N (giả sử batch_id = chapter_start):
DELETE FROM wiki_snapshots WHERE created_at > '<timestamp_of_batch_N>';
DELETE FROM wiki_relations WHERE created_at > '<timestamp_of_batch_N>';
UPDATE wiki_batches SET status = 'CRAWLED', extracted_at = NULL, merged_at = NULL
  WHERE chapter_start >= N;
-- Re-run: python main_wiki.py --from-batch N
```

## Notes
- LLM calls là sync (serial, không concurrent với ComfyUI)
- Crawler là async (httpx AsyncClient, rate-limited `crawler_delay_sec` configurable)
- `wiki/` module độc lập với video pipeline — video pipeline query wiki sau khi có đủ data
- story_slug abstract: swap story = chỉ sửa settings.yaml
- `ExtractionResult` validate LLM JSON output trực tiếp qua Pydantic — không raw dict
- Export `data/wiki/{name_normalized}.json` (filename safe) là input cho Video Pipeline (IPAdapter character reference)
- **Pass 2 system prompt template** (diff-focused, tiết kiệm token):
  ```
  Character identity (permanent): {visual_anchor}
  Current state (chapter {last_chapter}): {latest_snapshot_json}
  New chapters {start}-{end} below. Return ONLY changed fields as JSON.
  Set null for unchanged fields. Set physical_description=null if injury/condition resolved.
  Set is_active=false if character dies or permanently disappears.
  {raw_chapter_text}
  ```
