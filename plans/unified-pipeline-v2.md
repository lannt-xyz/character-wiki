# Unified Pipeline v2 — Per-Chapter + Vector DB

## Mục tiêu

Thay thế hai-pipeline hiện tại (`main_wiki.py` + `wiki_remaster.py`) bằng một pipeline thống nhất:

- Xử lý **per-chapter** thay vì per-batch
- Extraction **nhân vật + pháp khí inline** (không remaster riêng)
- **Append-only snapshots** mỗi chương (không LLM merge)
- **Vector DB (sqlite-vec)** được cập nhật sau mỗi chương — dùng cho RAG content generation

---

## Tổng quan Flow

```
PENDING → CRAWLED → EXTRACTED → EMBEDDED
   (1)        (2)        (3)         (4)
```

```
1. Crawl chapter  → chapters table (status=CRAWLED)
2. LLM Extract    → wiki_snapshots + wiki_artifact_snapshots (status=EXTRACTED)
3. Embed & Upsert → vec_characters + vec_artifacts (status=EMBEDDED)
```

**Nguyên tắc thiết kế:**
- Snapshot là **append-only** — không bao giờ UPDATE
- LLM chỉ dùng ở bước extraction, không dùng ở merge/embed
- Vector DB lưu **latest aggregated profile** (join từ snapshots gần nhất)
- Resume-safe: restart từ bất kỳ trạng thái nào
- `set_chapter_status()` là method duy nhất để đổi chapter status — không dùng `upsert_chapter()` sau bước CRAWLED

---

## Thay đổi State Machine

### Cũ (batch-level):
```
wiki_batches: PENDING → CRAWLED → EXTRACTED → MERGED
chapters: chỉ track crawl status
```

### Mới (chapter-level):
```
chapters: PENDING → CRAWLED → EXTRACTED → EMBEDDED
```

Không còn `wiki_batches` cho pipeline mới. Bảng `wiki_batches` giữ nguyên để không breaking v1 data.

---

## Schema Changes

### Bảng mới: `vec_characters`

```sql
CREATE TABLE IF NOT EXISTS vec_characters (
    character_id  TEXT PRIMARY KEY,
    chapter_num   INTEGER NOT NULL,     -- chapter khi embedding được update lần cuối
    embedding     BLOB NOT NULL,        -- float32[768] nomic-embed-text
    profile_text  TEXT NOT NULL,        -- text đã dùng để embed (for debug/regen)
    updated_at    TEXT NOT NULL
);
```

> Dùng `sqlite-vec` virtual table để query ANN:
> ```sql
> CREATE VIRTUAL TABLE IF NOT EXISTS vec_char_index USING vec0(
>     character_id TEXT,
>     embedding float[768]
> );
> ```

### Bảng mới: `vec_artifacts`

```sql
CREATE TABLE IF NOT EXISTS vec_artifacts (
    artifact_id  TEXT PRIMARY KEY,
    chapter_num  INTEGER NOT NULL,
    embedding    BLOB NOT NULL,
    profile_text TEXT NOT NULL,
    updated_at   TEXT NOT NULL
);

CREATE VIRTUAL TABLE IF NOT EXISTS vec_artifact_index USING vec0(
    artifact_id TEXT,
    embedding float[768]
);
```

### Thay đổi bảng `chapters`

Thêm status values mới: `EXTRACTED`, `EMBEDDED` (backward-compatible — cũ chỉ dùng `CRAWLED`/`ERROR`).

Không thêm column mới — status đã là TEXT, chỉ extend giá trị hợp lệ.

> ⚠️ **Lưu ý quan trọng:** `crawler/storage.py:save_chapter()` hiện gọi `upsert_chapter(..., status="CRAWLED")` hardcode — nếu chapter đã ở `EXTRACTED`/`EMBEDDED` mà crawl lại, `upsert_chapter` sẽ overwrite status về `CRAWLED` (do `ON CONFLICT DO UPDATE SET status=excluded.status`). Orchestrator_v2 **không được** gọi `save_chapter()` cho chapter đã qua bước CRAWLED. Chỉ gọi `set_chapter_status()` (đã có trong `database.py`) để advance status sau bước extract và embed.
>
> **Fix thêm trong `upsert_chapter`:** Sửa câu UPDATE để không bao giờ hạ cấp status:
> ```sql
> ON CONFLICT(chapter_num) DO UPDATE SET
>     title=excluded.title,
>     url=excluded.url,
>     status=CASE
>         WHEN wiki_chapters.status IN ('EXTRACTED','EMBEDDED') THEN wiki_chapters.status
>         ELSE excluded.status
>     END,
>     crawled_at=excluded.crawled_at,
>     content=excluded.content
> ```
> Điều này bảo vệ status ngay tại DB layer — defense-in-depth, không phụ thuộc vào caller discipline.

### Không thay đổi:
- `wiki_characters` — giữ nguyên
- `wiki_snapshots` — giữ nguyên (append-only)
- `wiki_artifacts` — giữ nguyên
- `wiki_artifact_snapshots` — giữ nguyên
- `wiki_batches` — giữ nguyên (v1 data không bị ảnh hưởng)

---

## Modules Mới / Sửa

### 1. `wiki/embedder.py` (MỚI)

**Responsibility:** Embed + upsert vào sqlite-vec.

```python
# Interface chính:
def embed_text(text: str) -> list[float]:
    """Call Ollama /api/embeddings với nomic-embed-text:latest.
    Returns float[768]. Raise nếu response invalid.
    Validate len(result) == settings.embed_dim khi startup."""

def build_character_profile_text(character_id: str, db: SQLiteDB) -> str:
    """Aggregate: wiki_characters + latest snapshot → plain text để embed.
    
    Format:
    Tên: {name} | Aliases: {aliases}
    Cố định: {visual_anchor}
    Trạng thái hiện tại: {physical_description}
    Cảnh giới: {level}
    Trang phục: {outfit}
    Vũ khí: {weapon}
    Hiệu ứng: {vfx_vibes}
    
    visual_anchor là từ wiki_characters (không thay đổi theo chương).
    Các field còn lại từ latest wiki_snapshots.
    """

def build_artifact_profile_text(artifact_id: str, db: SQLiteDB) -> str:
    """Aggregate: wiki_artifacts + latest artifact_snapshot → plain text."""

def upsert_character_embedding(character_id: str, chapter_num: int, db: SQLiteDB) -> None:
    """Build profile text → embed → upsert vec_characters + vec_char_index.
    
    Guard: skip nếu character_id không tồn tại trong wiki_characters (ghost character).
    Log warning và return sớm để không làm bẩn Vector DB.
    
    Upsert strategy cho vec0 (không hỗ trợ ON CONFLICT):
        Bọc toàn bộ trong một SQL Transaction:
            INSERT OR REPLACE INTO vec_characters ...
            DELETE FROM vec_char_index WHERE character_id = ?
            INSERT INTO vec_char_index(character_id, embedding) VALUES (?, ?)
        Nếu transaction fail, cả ba thao tác đều rollback → index không bị lệch.
    """

def upsert_artifact_embedding(artifact_id: str, chapter_num: int, db: SQLiteDB) -> None:
    """Build profile text → embed → upsert vec_artifacts + vec_artifact_index.
    Same transaction + ghost-check strategy như character."""
```

**Retry:** `tenacity` — 3 retries, exponential backoff, same pattern như extractor.py.

**Ollama endpoint — Batch embedding:** Dùng `POST /api/embed` (API mới hơn) thay vì `/api/embeddings` vì hỗ trợ `input` dạng list — gửi tất cả profiles của một chapter trong **một request duy nhất**:

```python
# Thay vì N requests:
#   embed_text(profile_1), embed_text(profile_2), ...embed_text(profile_N)
# Gửi 1 request batch:
payload = {
    "model": "nomic-embed-text:latest",
    "input": [profile_text_1, profile_text_2, ..., profile_text_N],
}
# Response: {"embeddings": [[...768 floats...], [...], ...]}
```

Với chapter có 20-30 nhân vật, batch request giảm N round-trips HTTP xuống còn 1. Interface `embed_text_batch(texts: list[str]) -> list[list[float]]` thay thế `embed_text(text: str)`. `upsert_character_embedding` không còn gọi embed riêng lẻ — orchestrator gom tất cả profile texts trước, gọi `embed_text_batch` một lần, rồi loop upsert.

**Distance metric:** `nomic-embed-text` dùng **cosine similarity**. Dùng `vec_distance_cosine` trong sqlite-vec query, không dùng L2.

```sql
-- ANN query mẫu:
SELECT c.character_id, c.profile_text,
       vec_distance_cosine(ci.embedding, ?) AS distance
FROM vec_char_index ci
JOIN vec_characters c USING(character_id)
ORDER BY distance ASC
LIMIT ?;
```

**VRAM Management:** Embed model (`nomic-embed-text`) và extract model (`gemma4-32k`) không được load cùng lúc. Pipeline là **sequential** (single-thread) — không có race condition với Ollama global state. Orchestrator_v2 phải:
1. Offload extract model trước khi gọi bất kỳ embed call nào: `offload_ollama(settings.wiki_v2_extract_model)`
2. Các embed calls cho chapter đó chạy liên tiếp (không offload giữa các chars trong cùng chapter)
3. Offload embed model sau khi embed xong chapter: `offload_ollama(settings.embed_model)`
4. Chapter tiếp theo: load lại extract model như thường

---

### 2. `wiki/extractor_v2.py` (MỚI — per-chapter)

Wrap/extend từ `extractor.py` — **không duplicate logic**, chỉ thêm artifact support và adjust per-chapter behavior:
- Input: **single chapter text** (không phải batch)
- Pass 1 (name scan): gửi **toàn bộ chapter text** — không dùng `_pack_chapters_within_budget()` vì single chapter thường chỉ ~3000-5000 chars, không cần truncate. Budget packing chỉ cần thiết cho multi-chapter batch.
- Pass 2 (delta extract): giữ nguyên prompt, chỉ đổi chapter range = `{n}-{n}`, bổ sung section artifact context
- Return: `ExtractionResult` với `batch_chapter_start == batch_chapter_end == chapter_num`

```python
def extract_chapter(
    chapter_text: str,
    chapter_num: int,
    get_characters_by_names_fn,
    get_all_characters_fn,
    get_latest_snapshot_fn,
    get_artifacts_by_names_fn,      # NEW — pháp khí context
    get_latest_artifact_snapshot_fn, # NEW
    consecutive_fail_counter: list[int],
) -> ExtractionResult:
```

**Điểm khác biệt với extractor.py:**
- Pass 2 prompt bổ sung section **Pháp khí hiện có** (artifact context) — tương đương logic remaster
- LLM response schema mở rộng: thêm `new_artifacts` + `updated_artifacts` vào `ExtractionResult`

---

### 3. `models/schemas.py` — Mở rộng `ExtractionResult`

```python
class ArtifactPatch(BaseModel):
    artifact_id: str
    owner_id: Optional[str] = None
    normal_state: Optional[str] = None
    active_state: Optional[str] = None
    condition: Optional[str] = None
    vfx_color: Optional[str] = None
    is_key_event: Optional[bool] = None

class ArtifactEntry(BaseModel):
    """New artifact discovered in this chapter."""
    artifact: dict   # wiki_artifacts fields
    snapshot: dict   # wiki_artifact_snapshots fields

class ExtractionResult(BaseModel):
    batch_chapter_start: int
    batch_chapter_end: int
    new_characters: list[dict] = Field(default_factory=list)
    updated_characters: list[CharacterPatch] = Field(default_factory=list)
    # NEW fields — default=[] để backward-compat với v1 merger:
    new_artifacts: list[ArtifactEntry] = Field(default_factory=list)
    updated_artifacts: list[ArtifactPatch] = Field(default_factory=list)
```

> **Backward compat note:** `new_artifacts`/`updated_artifacts` có `default_factory=list` nên `merger.py` (v1) parse bình thường và silently ignore artifact data. Chỉ `merger_v2.py` xử lý artifact fields. Đây là hành vi mong muốn.

---

### 4. `wiki/merger_v2.py` (MỚI)

Extend merger.py thêm artifact merge:

```python
def merge_extraction_result_v2(
    result: ExtractionResult,
    db: SQLiteDB,
    extraction_version: int = 2,
) -> tuple[int, int, int, int, int]:  # (n_char_new, n_char_upd, n_char_skip, n_art_new, n_art_upd)
```

Logic artifact merge:
- `new_artifacts` → `db.upsert_artifact()` + `db.add_artifact_snapshot()`
- `updated_artifacts` → load latest artifact snapshot → apply patch → `db.add_artifact_snapshot()`
- Skip artifact snapshot nếu không có field nào thay đổi
- **`owner_id=None` trong patch có nghĩa là pháp khí không còn chủ nhân** (rơi mất / bị cướp). Merger phải ghi `owner_id=NULL` vào snapshot mới — không inherit từ snapshot cũ như các persistent fields khác. `owner_id` là **transient field**.

---

### 5. `wiki/orchestrator_v2.py` (MỚI)

Pipeline loop per-chapter. Giữ nguyên `orchestrator.py` để backward-compat với `main_wiki.py`.

```python
def run_pipeline_v2(
    db: SQLiteDB,
    from_chapter: Optional[int] = None,
    dry_run: bool = False,
    max_chapters: Optional[int] = None,
) -> None:
```

**Loop logic:**

```python
for chapter in pending_chapters:
    chapter_num = chapter["chapter_num"]
    status = chapter["status"]

    # Step 1: Crawl — chỉ chạy nếu PENDING
    if status == "PENDING":
        ch = await crawl_chapter(chapter_num)  # single chapter
        if ch.status == "ERROR":
            db.set_chapter_status(chapter_num, "ERROR", ch.error_msg)
            continue
        # save_chapter() ghi content + set status=CRAWLED
        # Sau đây không gọi save_chapter/upsert_chapter nữa — chỉ dùng set_chapter_status
        db.upsert_chapter(chapter_num, ch.title, ch.url, "CRAWLED", content=ch.content)
        status = "CRAWLED"

    # Step 2: Extract — chỉ chạy nếu CRAWLED
    if status == "CRAWLED":
        text = db.get_chapter_content(chapter_num)  # load từ DB
        result = extract_chapter(text, chapter_num, ...)
        merge_extraction_result_v2(result, db)
        db.set_chapter_status(chapter_num, "EXTRACTED")  # dùng set_chapter_status, KHÔNG upsert_chapter
        status = "EXTRACTED"

    # Step 3: Embed — chỉ chạy nếu EXTRACTED
    if status == "EXTRACTED":
        affected_chars = [c["character_id"] for c in result.new_characters_merged] + \
                         [p.character_id for p in result.updated_characters]
        affected_arts  = [a.artifact["artifact_id"] for a in result.new_artifacts] + \
                         [p.artifact_id for p in result.updated_artifacts]

        # Ghost character guard: chỉ embed những char/artifact thực sự tồn tại trong master tables
        valid_chars = [c for c in affected_chars if db.get_character_by_id(c)]
        invalid_chars = set(affected_chars) - set(valid_chars)
        if invalid_chars:
            logger.warning("Ghost characters detected, skipping embed | ids={}", invalid_chars)

        if valid_chars or affected_arts:
            offload_ollama(settings.wiki_v2_extract_model)  # free VRAM trước embed
            for char_id in valid_chars:
                upsert_character_embedding(char_id, chapter_num, db)
            for art_id in affected_arts:
                upsert_artifact_embedding(art_id, chapter_num, db)
            offload_ollama(settings.embed_model)            # free embed VRAM

        db.set_chapter_status(chapter_num, "EMBEDDED")     # dùng set_chapter_status
```

**Resume-safe:** Mỗi step chỉ chạy nếu status tương ứng. `set_chapter_status()` (đã có trong `database.py`) là method duy nhất để advance status sau bước CRAWLED — không dùng `upsert_chapter()` để tránh reset status.

---

### 6. `db/database.py` — Thêm methods

```python
# Chapters — ĐÃ CÓ SẴN (không implement lại):
# set_chapter_status(chapter_num, status, error_msg=None)  ← dùng method này
# get_chapter_status(chapter_num) -> Optional[str]
# get_chapter_content(chapter_num) -> Optional[str]

# Chapters — CẦN THÊM MỚI:
def get_chapters_by_status(self, status: str) -> list[dict]
# Returns all chapters WHERE status=? ORDER BY chapter_num ASC

# Artifacts — ĐÃ CÓ SẴN (không implement lại):
# upsert_artifact(artifact_id, name, name_normalized, rarity, material, visual_anchor, description) -> str
# add_artifact_snapshot(artifact_id, chapter_start, owner_id, ...) -> None
# get_latest_artifact_snapshot(artifact_id) -> Optional[dict]
# get_all_artifacts() -> list[dict]

# Artifacts — CẦN THÊM MỚI:
def get_artifacts_by_names(self, names_normalized: list[str]) -> list[dict]
# Bulk fetch artifacts bằng name_normalized — tương đương get_characters_by_names()
# Cần cho extractor_v2 để build artifact context cho Pass 2

# Vector DB — TẤT CẢ CẦN THÊM MỚI:
def create_vec_tables(self) -> None
# Gọi sau sqlite_vec.load(conn). Tạo:
#   vec_characters (regular table — INSERT OR REPLACE)
#   vec_char_index (virtual vec0 — DELETE + INSERT)
#   vec_artifacts, vec_artifact_index

def upsert_character_vec(self, character_id: str, chapter_num: int, embedding: list[float], profile_text: str) -> None
# Wrapped trong một SQL Transaction:
#   INSERT OR REPLACE INTO vec_characters
#   DELETE FROM vec_char_index WHERE character_id=?
#   INSERT INTO vec_char_index(character_id, embedding)
# Transaction đảm bảo regular table và virtual table không bị lệch nếu crash giữa chừng

def upsert_artifact_vec(self, artifact_id: str, chapter_num: int, embedding: list[float], profile_text: str) -> None
# Same transaction pattern

def search_characters_by_embedding(self, query_embedding: list[float], top_k: int = 10) -> list[dict]
def search_artifacts_by_embedding(self, query_embedding: list[float], top_k: int = 10) -> list[dict]
# Dùng vec_distance_cosine (nomic-embed-text dùng cosine, không dùng L2):
# SELECT c.*, vec_distance_cosine(ci.embedding, ?) AS distance
# FROM vec_char_index ci JOIN vec_characters c USING(character_id)
# ORDER BY distance ASC LIMIT ?
```

> ⚠️ **sqlite-vec `vec0` không hỗ trợ `ON CONFLICT`/upsert native.** Upsert phải thực hiện bằng DELETE trước rồi INSERT. Regular table `vec_characters` dùng `INSERT OR REPLACE` bình thường.

---

### 7. `main_wiki_v2.py` (MỚI)

CLI entry point mới. `main_wiki.py` giữ nguyên.

```bash
uv run python3 main_wiki_v2.py                         # full pipeline
uv run python3 main_wiki_v2.py --from-chapter <N>      # resume từ chapter N
uv run python3 main_wiki_v2.py --max-chapters <N>      # giới hạn N chapters (test)
uv run python3 main_wiki_v2.py --dry-run               # skip DB writes
uv run python3 main_wiki_v2.py --stats                 # in tiến độ
uv run python3 main_wiki_v2.py --regen-embeddings      # re-embed tất cả chars/artifacts
```

---

## Config Changes (`config/settings.yaml`)

Thêm:
```yaml
# Unified pipeline v2
embed_model: "nomic-embed-text:latest"    # model cho embeddings
embed_dim: 768                             # nomic-embed-text output dim
wiki_v2_extract_model: "gemma4-32k:latest" # có thể khác v1
```

---

## Dependency Mới

```toml
# pyproject.toml
sqlite-vec = ">=0.1.0"   # sqlite extension, install via uv
```

Load extension trong `db/database.py`:
```python
import sqlite_vec
conn.enable_load_extension(True)
sqlite_vec.load(conn)
conn.enable_load_extension(False)
```

---

## Migration / Backward Compatibility

- `main_wiki.py` và `wiki_remaster.py` **không bị đụng** — vẫn chạy được trên v1 data
- `main_wiki_v2.py` là entry point mới, dùng DB cùng (`db/pipeline.db`)
- Chapters đã được extract v1 có `chapters.status = CRAWLED` (v1 không advance chapter status lên MERGED). Nếu chạy v2 mà không migrate, **toàn bộ 3682 chương sẽ bị re-extract** → duplicate snapshots.

### Migration Script (chạy 1 lần trước main_wiki_v2.py)

Logic: chapters nào đã có ít nhất 1 wiki_snapshot (v1) thì mark EMBEDDED để v2 skip.

```sql
-- Script: scripts/migrate_v1_to_v2_status.py
UPDATE chapters
SET status = 'EMBEDDED'
WHERE chapter_num IN (
    SELECT DISTINCT chapter_start FROM wiki_snapshots WHERE extraction_version = 1
)
AND status = 'CRAWLED';
```

Chapters chưa có snapshot v1 (crawled nhưng extraction fail) giữ nguyên `CRAWLED` → v2 sẽ pick up và extract.

> **CLI flag:** `main_wiki_v2.py --migrate-v1` để chạy migration script trước pipeline.

---

## LLM Prompt v2 — Pass 2 Additions

Pass 2 cần bổ sung section pháp khí vào prompt:

```
Pháp khí hiện có (context):
{artifact_context}

Trả về JSON với cấu trúc:
{
  "new_characters": [...],      -- giữ nguyên
  "updated_characters": [...],  -- giữ nguyên
  "new_artifacts": [
    {
      "artifact": {
        "artifact_id": "<slug>",
        "name": "<tên pháp khí>",
        "rarity": "<độ hiếm hoặc null>",
        "material": "<chất liệu hoặc null>",
        "visual_anchor": "<đặc trưng ngoại hình cố định hoặc null>",
        "description": "<mô tả ngắn>"
      },
      "snapshot": {
        "chapter_start": {n},
        "owner_id": "<character_id hoặc null>",
        "normal_state": "<trạng thái bình thường hoặc null>",
        "active_state": "<trạng thái kích hoạt hoặc null>",
        "condition": "intact|damaged|destroyed",
        "vfx_color": "<màu hiệu ứng hoặc null>",
        "is_key_event": false
      }
    }
  ],
  "updated_artifacts": [
    {
      "artifact_id": "<id>",
      "owner_id": "<new owner hoặc null nếu không đổi>",
      ...fields thay đổi...
    }
  ]
}
```

---

## Thứ Tự Implement

| Task | File | Ghi chú | Phụ thuộc |
|---|---|---|---|
| 1 | `pyproject.toml` — thêm `sqlite-vec` dep + `uv sync` | **Phải làm đầu tiên** — mọi import sqlite_vec đều fail nếu chưa install | — |
| 2 | `config/settings.yaml` — thêm embed config | — | — |
| 3 | `models/schemas.py` — thêm `ArtifactPatch`, `ArtifactEntry`, mở rộng `ExtractionResult` | Backward-compat: default_factory=list cho artifact fields | — |
| 4 | `db/database.py` — thêm `get_chapters_by_status`, `get_artifacts_by_names`, vec methods + `create_vec_tables` | Đã có sẵn: `set_chapter_status`, `upsert_artifact`, `add_artifact_snapshot`, `get_latest_artifact_snapshot` — không implement lại | Task 1, 3 |
| 5 | `wiki/embedder.py` — Ollama embed + sqlite-vec upsert (DELETE+INSERT strategy) | Include VRAM offload helpers | Task 4 |
| 6 | `wiki/extractor_v2.py` — per-chapter extract, wrap extractor.py, thêm artifact support | Pass 1 gửi full chapter text, không dùng budget packing | Task 3 |
| 7 | `wiki/merger_v2.py` — wrap merger.py, thêm artifact merge | — | Task 3, 4 |
| 8 | `wiki/orchestrator_v2.py` — per-chapter loop, VRAM offload strategy, dùng `set_chapter_status` | — | Task 4, 5, 6, 7 |
| 9 | `main_wiki_v2.py` — CLI entry point; bao gồm `ensure_migration()` chạy tự động khi startup | Migration không phải script rời — là hàm trong main, gọi trước pipeline loop | Task 8 |
| 11 | Tests — `tests/test_embedder.py`, `tests/test_extractor_v2.py`, `tests/test_merger_v2.py` | — | Task 5, 6, 7 |

---

## Acceptance Criteria

- [ ] Chạy `main_wiki_v2.py --max-chapters 10` trên DB sạch: 10 chapters đạt status `EMBEDDED`
- [ ] `SELECT COUNT(*) FROM vec_char_index` > 0 sau 10 chapters
- [ ] ANN query `search_characters_by_embedding` trả kết quả trong < 200ms trên 500 chars
- [ ] Restart giữa chừng (kill sau chapter 5): resume từ đúng chapter 6, không re-extract chapter 1-5
- [ ] `wiki_snapshots` không có duplicate (same character_id + chapter_start) sau re-run
- [ ] `main_wiki.py` vẫn chạy bình thường sau khi thêm các schema mới

---

## Rollback Plan

Vì `vec_characters`/`vec_artifacts`/`vec_char_index`/`vec_artifact_index` chỉ là cache (có thể rebuild từ snapshots), rollback an toàn:

```bash
# Nếu pipeline v2 có vấn đề:
cp db/pipeline.db.bak db/pipeline.db   # restore DB backup (Phase 0 tạo backup)
# Hoặc chỉ clear vector tables:
sqlite3 db/pipeline.db "DROP TABLE IF EXISTS vec_characters; DROP TABLE IF EXISTS vec_artifacts;"
sqlite3 db/pipeline.db "DROP TABLE IF EXISTS vec_char_index; DROP TABLE IF EXISTS vec_artifact_index;"
# Rồi regen: main_wiki_v2.py --regen-embeddings
```

`wiki_snapshots` và `wiki_characters` không bị ảnh hưởng bởi rollback vector tables.

---

## Risks & Mitigations

| Risk | Likelihood | Impact | Mitigation |
|------|-----------|--------|------------|
| `save_chapter`/`upsert_chapter` overwrite EXTRACTED→CRAWLED | High (nếu không fix) | Critical — phá resume-safety | Orchestrator_v2 không gọi `upsert_chapter` sau bước crawl; dùng `set_chapter_status` |
| VRAM OOM khi load 2 model cùng lúc | Medium | High — crash mid-chapter | Explicit `offload_ollama()` trước/sau embed step |
| sqlite-vec upsert lỗi ON CONFLICT | High (nếu không biết) | High — crash sau chapter đầu | DELETE + INSERT strategy, document rõ |
| Per-chapter extraction chất lượng kém hơn batch | Medium | Medium — miss nhân vật | Pass 1 gửi full chapter text; context từ existing snapshots bù đắp |
| Re-extract toàn bộ v1 data | High (nếu skip migration) | High — tốn giờ, duplicate data | Migration script mark EMBEDDED trước khi chạy |
| embed_dim thay đổi khi đổi model | Low | High — ANN index broken | Validate `len(embedding) == settings.embed_dim` khi startup |

---

## Notes

- Embedding khi character/artifact chưa có snapshot: embed ngay với tên + visual_anchor, không skip.
- `extractor_v2.py` wrap `extractor.py` (không copy-paste) để tránh diverge hai codebase.
- `merger_v2.py` wrap `merger.py` — gọi `merge_extraction_result()` cho character part, thêm artifact merge inline.
- sqlite-vec ANN accuracy đủ dùng cho RAG context retrieval; không cần benchmark production trừ khi dataset > 10k vectors.
