# wiki_remaster.py — Artifact Seed & Character Remaster Plan

## Status: Done

## Mục tiêu
1. **Dậm lại top 20 nhân vật** cho rõ nét hơn (lần 1 có thể bỏ sót detail)
2. **Tạo wiki_artifacts** — pháp khí quan trọng với visual description đầy đủ

## Tổng quan flow

```
[main_wiki.py chạy hết 707 batch — wiki_batches đầy MERGED]
        ↓
[wiki_remaster.py]
  Phase 1: Init wiki_remaster_batches (bảng riêng, PENDING toàn bộ)
  Phase 2: Build character markdown từ full wiki (top 20 by snapshot count)
  Phase 3: LLM extract artifacts per character → wiki_seed.json
  Phase 4: Remaster top 20 characters → 1 canonical v2 snapshot / nhân vật
  Phase 5: Seed artifacts vào DB (2 bảng mới) + backup
  Phase 6: Patch db/extractor/merger cho before_chapter + artifact context
  Phase 7: Run remaster extraction loop (dùng wiki_remaster_batches)
        ↓
[wiki_batches không bị đụng — main_wiki.py vẫn thấy MERGED hết]
```

**Nguyên tắc**:
- `main_wiki.py`, `wiki_batches` **không bị sửa**, không bị reset
- `wiki_remaster.py` có state machine riêng qua `wiki_remaster_batches`
- Reuse `SQLiteDB`, `settings`, `_ollama_generate` — không thay đổi chúng

---

## Usage

```bash
# Full run (phase 1 → 6)
uv run python3 wiki_remaster.py

# Resume từ phase cụ thể
uv run python3 wiki_remaster.py --from-phase 3
uv run python3 wiki_remaster.py --from-phase 6   # chỉ chạy lại extraction loop

# Dry-run: skip ghi DB
uv run python3 wiki_remaster.py --dry-run
```

---

## Phase 1 — Init `wiki_remaster_batches`

Tạo bảng quản lý batch riêng cho remaster, đăng ký toàn bộ từ `wiki_batches`.
`main_wiki.py` không biết gì về bảng này.

```sql
CREATE TABLE IF NOT EXISTS wiki_remaster_batches (
    batch_id          INTEGER PRIMARY KEY,  -- same key as wiki_batches.batch_id
    chapter_start     INTEGER NOT NULL,
    chapter_end       INTEGER NOT NULL,
    remaster_version  INTEGER NOT NULL DEFAULT 1,
    status            TEXT NOT NULL DEFAULT 'PENDING',  -- PENDING → EXTRACTED → MERGED
    extracted_at      TEXT,
    merged_at         TEXT
);
```

> **Không có CRAWLED** — `.txt` files đã tồn tại từ pipeline lần 1, bỏ crawl step hoàn toàn.

Init từ `wiki_batches` (idempotent — skip nếu đã có):
```sql
INSERT OR IGNORE INTO wiki_remaster_batches (batch_id, chapter_start, chapter_end)
SELECT batch_id, chapter_start, chapter_end FROM wiki_batches;
```

State machine của remaster:
```
PENDING → extract → EXTRACTED → merge → MERGED
            ↑
  .txt files đã có sẵn, đọc thẳng
```

---

## Phase 2 — Build Character Input

### Chọn nhân vật
Script chạy sau khi **toàn bộ 707 batch đã MERGED** → top 20 lúc này là đầy đủ:

```sql
SELECT character_id, name, COUNT(*) as snap_count
FROM wiki_snapshots
GROUP BY character_id
ORDER BY snap_count DESC
LIMIT 20
```

### Format output — Markdown Table

```markdown
## Diệp Thiếu Dương [diep_thieu_duong]
visual_anchor: <đặc điểm gốc nếu có>
aliases: Tiểu Dương, ...

| Ch start | Ch end | Level | Outfit | Weapon | VFX vibes | Physical state |
|---|---|---|---|---|---|---|
| 26 | 35 | — | — | Thất Tinh Long Tuyền Kiếm | — | — |
| 36 | 40 | — | — | Thất Tinh Long Tuyền Kiếm | — | không có |
...

Relations:
- Tiểu Mã: đồng hành, từ Ch 26
```

> Cell trùng lặp ghi giá trị thực hoặc `—`, không dùng ký hiệu shorthand như `"` hay `ditto` — tăng rủi ro LLM parse sai.

> **Dedup snapshots**: nếu nhân vật có > 50 snapshots, chỉ giữ các **milestone rows** — tức là những row có ít nhất 1 field thay đổi so với row liền trước, hoặc cách nhau ≥ 20 chương. Bỏ các row toàn `—` liên tiếp — LLM sẽ bỏ qua thông tin quan trọng cuối bảng nếu bị "ngộp" bởi hàng trăm dòng giống nhau.

Output: `data/rerun/character_input/{character_id}.md` (1 file / nhân vật)

---

## Phase 3 — LLM Extract Artifacts

### 1 LLM request / nhân vật (tránh vượt token limit)

**System prompt**:
```
Bạn là trợ lý phân tích truyện Tiên hiệp/Đô thị huyền huyễn.
Từ timeline nhân vật, trích xuất các PHÁP KHÍ QUAN TRỌNG mà nhân vật sở hữu hoặc thường xuyên sử dụng.
Chỉ lấy pháp khí có sức mạnh đặc biệt hoặc xuất hiện thường xuyên. Bỏ qua đồ dùng 1-2 lần.
Với mỗi pháp khí, hãy đặc biệt chú ý trích xuất "material" (chất liệu: đồng, ngọc, gỗ đào, xương yêu...) — đây là yếu tố quyết định texture khi render 3D.
Trả JSON thuần túy, không markdown, không giải thích.
```

**User prompt** = nội dung file markdown từ Phase 1.

### Schema output LLM

```json
{
  "character_id": "diep_thieu_duong",
  "artifacts": [
    {
      "artifact_id": "that_tinh_long_tuyen_kiem",
      "name": "Thất Tinh Long Tuyền Kiếm",
      "rarity": "trấn sơn chi bảo",
      "material": "đồng cổ, khảm ngọc tinh tú",
      "visual_anchor": "Ancient Chinese bronze sword, 7 silver stars inlaid on blade, dragon-shaped hilt",
      "snapshots": [
        {
          "chapter_start": 10,
          "owner_id": "diep_thieu_duong",
          "normal_state": "Thanh kiếm đồng cổ, trầm mặc, 7 ngôi sao mờ trên thân",
          "active_state": "7 sao phát sáng xanh, bóng rồng quấn quanh, tiếng rồng ngâm",
          "condition": "intact"
        },
        {
          "chapter_start": 450,
          "owner_id": "diep_thieu_duong",
          "normal_state": "Thân kiếm có vết nứt dọc, ánh sao mờ hơn",
          "active_state": "Vẫn phát sáng nhưng yếu hơn, không còn bóng rồng",
          "condition": "damaged"
        }
      ]
    }
  ]
}
```

### Enum cho `condition`
| Value | Ý nghĩa | Render hint |
|---|---|---|
| `intact` | Bình thường, nguyên vẹn | Standard render |
| `active` | Đang phát huy sức mạnh | VFX rực rỡ, hiệu ứng tối đa |
| `damaged` | Sứt mẻ, vết nứt | Render model có vết nứt/sứt |
| `evolved` | Thăng cấp, ngoại hình thay đổi hoàn toàn | Dùng visual_anchor mới từ snapshot này |

Output: `data/rerun/wiki_seed.json`
**Resumable**: nếu `wiki_seed.json` đã có entry cho `character_id`, skip — không gọi LLM lại.

---

## Phase 4 — Remaster Top 20 Characters

Dùng lại `data/rerun/character_input/{character_id}.md` từ Phase 2; gọi LLM 1 lần / nhân vật để tổng hợp toàn bộ timeline thành **1 canonical v2 snapshot**.

### System prompt
```
Bạn là trợ lý wiki truyện Tiên hiệp/Đô thị huyền huyễn.
Từ toàn bộ timeline nhân vật bên dưới, hãy tổng hợp thành 1 profile hoàn chỉnh và chi tiết nhất có thể.
Chú trọng: mô tả ngoại hình, trang phục đặc trưng, vũ khí, trạng thái tu vi hiện tại, quan hệ nhân vật.
Trả JSON thuần túy, không markdown, không giải thích.
```

### User prompt
Nội dung `data/rerun/character_input/{character_id}.md`

### Output JSON schema
```json
{
  "character_id": "diep_thieu_duong",
  "name": "Diệp Thiếu Dương",
  "aliases": ["Tiểu Dương", "Ma Đế"],
  "gender": "male",
  "faction": "Thiên Ma Tông",
  "visual_anchor": "Tall young man, black robes with silver trim, cold sharp eyes, long black hair tied loosely",
  "personality": "lạnh lùng, quyết đoán, bảo vệ người thân bằng mọi giá",
  "relations": [
    {"target_id": "tieu_ma", "relation_type": "đồng hành", "note": "từ Ch 26"}
  ],
  "peak_snapshot": {
    "level": "Hóa Thần Kỳ hậu kỳ",
    "outfit": "Hắc Ma Bào có văn khắc ngân long",
    "weapon": "Thất Tinh Long Tuyền Kiếm",
    "vfx_vibes": "black mist, silver stars, dragon shadow",
    "visual_importance": 10
  }
}
```

**Hai trường nhóm tách biệt**:
- **Root fields** (`name`, `aliases`, `gender`, `faction`, `visual_anchor`, `personality`, `relations`) → UPDATE `wiki_characters`
- **`peak_snapshot`** → INSERT `wiki_snapshots` (temporal state tại chapter cao nhất)

### Ghi vào DB

**Bước 1 — UPDATE `wiki_characters`** (ghi đè identity fields với version tốt hơn):
```python
db.update_character_identity(
    character_id=result["character_id"],
    visual_anchor=result["visual_anchor"],
    faction=result["faction"],
    gender=result["gender"],
    aliases=result["aliases"],
    personality=result["personality"],
)
# + upsert relations
```

**Bước 2 — INSERT `wiki_snapshots`** (canonical peak state):
- `chapter_start` = chapter lớn nhất nhân vật xuất hiện (query từ `wiki_snapshots` v1)
- `extraction_version = 2`
- append-only, coexist với v1

**Resumable**: kiểm tra `wiki_snapshots WHERE character_id=? AND extraction_version=2` — nếu đã có thì skip cả 2 bước.

---

## Phase 5 — Apply Seed vào DB

### 5.1 Backup DB (bắt buộc trước khi ghi)
```python
shutil.copy("db/pipeline.db", "db/pipeline.db.bak")
```

### 5.2 Schema mới — 2 bảng artifact, không sửa bảng cũ

```sql
CREATE TABLE IF NOT EXISTS wiki_artifacts (
    artifact_id      TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    name_normalized  TEXT NOT NULL UNIQUE,
    rarity           TEXT,
    visual_anchor    TEXT,
    description      TEXT,
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wiki_artifact_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id         TEXT NOT NULL REFERENCES wiki_artifacts(artifact_id),
    chapter_start       INTEGER NOT NULL,
    owner_id            TEXT,           -- character_id hoặc NULL (vô chủ)
    normal_state        TEXT,
    active_state        TEXT,
    condition           TEXT NOT NULL DEFAULT 'intact',
    vfx_color           TEXT,           -- hex hoặc tên màu chủ đạo, e.g. "#7B68EE" "gold" (dùng thẳng cho particle system)
    is_key_event        INTEGER NOT NULL DEFAULT 0,  -- 1 nếu owner_id hoặc condition thay đổi so với snapshot trước
    extraction_version  INTEGER NOT NULL DEFAULT 2,
    created_at          TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_art_snap_ch
    ON wiki_artifact_snapshots(artifact_id, chapter_start DESC);
```

> `owner_id` append-only: đổi chủ = insert row mới với owner_id mới.
> Query video pipeline: `SELECT * FROM wiki_artifact_snapshots WHERE artifact_id=? AND chapter_start <= ? ORDER BY chapter_start DESC LIMIT 1`

### 5.3 Insert từ wiki_seed.json
- `upsert_artifact()` — idempotent (ON CONFLICT DO UPDATE visual_anchor/rarity)
- `add_artifact_snapshot()` — append-only, không UPDATE

---

## Phase 6 — Patch Extractor cho Remaster

Đây là phần **thiếu trong design cũ**. Sau khi seed artifact xong, `main_wiki.py` cần biết về artifact để extraction lần 2 có nghĩa.

### 4.0 Fix `get_latest_snapshot` — thêm `before_chapter` cutoff

**Vấn đề**: `get_latest_snapshot` hiện tại lấy snapshot có `chapter_start` cao nhất tuyệt đối.
Khi v2 rerun xử lý Ch 1-5, nó trả về snapshot Ch 880+ (v1) → LLM nhận context nhân vật
level cao từ Ch 1 → extraction sai hoàn toàn.

**Fix `db/database.py`**:
```python
def get_latest_snapshot(self, character_id: str, before_chapter: int = None) -> Optional[dict]:
    if before_chapter is not None:
        # Chỉ lấy snapshot đã xảy ra TRƯỚC batch hiện tại
        row = self._conn.execute(
            "SELECT * FROM wiki_snapshots WHERE character_id=? AND chapter_start < ? ORDER BY chapter_start DESC LIMIT 1",
            (character_id, before_chapter),
        ).fetchone()
    else:
        row = self._conn.execute(
            "SELECT * FROM wiki_snapshots WHERE character_id=? ORDER BY chapter_start DESC LIMIT 1",
            (character_id,),
        ).fetchone()
    return dict(row) if row else None
```

**Fix `wiki/orchestrator.py`** — truyền `before_chapter` vào lambda:
```python
get_latest_snapshot_fn=lambda cid: db.get_latest_snapshot(cid, before_chapter=chapter_start),
```

Kết quả:
- Ch 1-5 v2: `before_chapter=1` → NULL → LLM treat như nhân vật hoàn toàn mới ✅
- Ch 100-105 v2: `before_chapter=100` → trả snapshot Ch 96-99 (v2 nếu có, không thì v1) ✅

### 4.1 Thêm method vào `db/database.py`
```python
def get_all_artifacts(self) -> list[dict]: ...
def get_artifact_snapshot_at(self, artifact_id: str, chapter_num: int) -> Optional[dict]: ...
```

### 4.2 Inject artifact context vào Pass 2 prompt — `wiki/extractor.py`

Thêm 1 section vào `_PASS2_USER_TMPL`:
```
Danh sách pháp khí đã biết (context):
{artifact_context}

Với mỗi pháp khí xuất hiện trong đoạn này, trả thêm trong JSON:
"artifact_updates": [
  {
    "artifact_id": "...",
    "owner_id": "...",   // character_id đang cầm, null nếu không rõ
    "condition": "...",  // intact/active/damaged/evolved
    "note": "..."        // mô tả thay đổi nếu có
  }
]
```

### 4.3 Merge artifact updates — `wiki/merger.py`
Xử lý `extraction_result.artifact_updates` tương tự `updated_characters`:
- Lookup artifact_id tồn tại
- Insert `wiki_artifact_snapshots` nếu có thay đổi về owner hoặc condition
- **Key Event detection**: so sánh với snapshot trước nhất của artifact — nếu `owner_id` hoặc `condition` thay đổi, set `is_key_event=1`. Video pipeline dùng cột này để trigger animation đổi tư thế cầm kiếm / hiệu ứng chuyển giao pháp khí.

> **Scope**: Phase 6 **sửa** `extractor.py`, `merger.py`, `db/database.py`.
> Backward-compatible — artifact_context rỗng khi không có data → `main_wiki.py` chạy bình thường.

---

## Phase 7 — Remaster Extraction Loop

`wiki_remaster.py` tự chạy vòng lặp riêng, đọc `wiki_remaster_batches`:

```python
for batch in db.get_remaster_pending_batches():
    # 1. Load text từ data/chapters/ (đã có sẵn)
    batch_text = _load_batch_text(batch.chapter_start, batch.chapter_end)

    # 2. Extract — dùng before_chapter để cutoff context
    result = extract_batch(
        ...,
        get_latest_snapshot_fn=lambda cid: db.get_latest_snapshot(
            cid, before_chapter=batch.chapter_start
        ),
    )
    db.set_remaster_batch_status(batch.batch_id, "EXTRACTED")

    # 3. Merge — append-only, snapshots v2 coexist với v1
    merge_extraction_result(result, db, extraction_version=2)
    db.set_remaster_batch_status(batch.batch_id, "MERGED")
```

**Resume-safe**: restart bất kỳ lúc nào, loop tiếp tục từ batch chưa MERGED trong `wiki_remaster_batches`.

> **Rủi ro LLM**: nếu `gemma4-32k` vẫn parse JSON kém, 707 batch sẽ tốn thời gian retry. Cân nhắc dùng **Gemini 1.5 Flash** hoặc **GPT-4o-mini** cho Phase 7 — chi phí ~$1-2 cho 707 requests nhưng độ ổn định JSON và nhận diện pháp khí vượt trội. Cấu hình qua `wiki_remaster_model` trong `settings.yaml` (tách khỏi `wiki_extract_model` của main pipeline).

**Kết quả**: `wiki_snapshots` có cả v1 và v2 snapshots cùng tồn tại.
Query theo version: `WHERE extraction_version=2 AND chapter_start <= ?`

---

## File output

```
data/rerun/
  character_input/
    diep_thieu_duong.md
    thanh_van_tu.md
    ...  (top 20 files)
  wiki_seed.json            ← artifact seed data (resumable anchor)
db/
  pipeline.db.bak           ← backup tự động trước phase 3
```

---

## Checklist Implementation

- [x] `wiki_remaster.py` — CLI với `--from-phase N`, `--dry-run`
- [x] Phase 1: tạo `wiki_remaster_batches`, init từ `wiki_batches` (idempotent)
- [x] Phase 2: query top 20, render markdown per character → `data/rerun/character_input/`
- [x] Phase 3: LLM loop (1 request/char), parse JSON, ghi `wiki_seed.json` incremental (resumable)
- [x] Phase 4: LLM remaster top 20 chars → UPDATE `wiki_characters` (identity) + INSERT `wiki_snapshots` v2 peak snapshot (resumable)
- [x] Phase 5: backup DB, tạo `wiki_artifacts` + `wiki_artifact_snapshots`, insert từ seed
- [x] Phase 6: `get_latest_snapshot(before_chapter)` trong `db/database.py`; verify schema
- [x] Phase 7: extraction loop dùng `wiki_remaster_batches`, resume-safe
- [x] Test `--dry-run` trên DB hiện tại
