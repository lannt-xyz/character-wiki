# wiki_remaster.py — Artifact Seed & Character Remaster Plan

## Status: Done (5-phase design: 0-4)

## Mục tiêu
1. **Re-read raw chapters** để trích xuất data bị bỏ sót ở lần 1 → v2 snapshots
2. **Tạo wiki_artifacts** — pháp khí quan trọng với visual description đầy đủ
3. **Cập nhật wiki_characters** từ v2 data (phase 4 — chính xác nhất)

## Tổng quan flow

```
[main_wiki.py chạy hết 707 batch — wiki_batches đầy MERGED]
        ↓
[wiki_remaster.py]
  Phase 0: Backup DB → db/pipeline.db.bak
  Phase 1: Init wiki_remaster_batches (PENDING toàn bộ)
  Phase 2: Build top-20 character markdown (v1 snapshots + chapter excerpts)
           + seed artifact stubs vào wiki_artifacts từ weapon field v1 (≥3 mentions)
  Phase 3: Extraction loop — sequential qua 707 batch (resume-safe)
     (LLM extract → merge character deltas v2 → upsert artifacts inline)
  Phase 4: Final synthesis — LLM re-synthesize wiki_characters từ v2 snapshots (top 20 × 1 call)
        ↓
[wiki_batches không bị đụng — main_wiki.py vẫn thấy MERGED hết]
```

**Nguyên tắc thiết kế**:
- `main_wiki.py`, `wiki_batches` **không bị sửa**, không bị reset
- `wiki_remaster.py` có state machine riêng qua `wiki_remaster_batches`
- Phase 3 là loop duy nhất — không có bước LLM per-character riêng lẻ (no wiki_seed.json)
- Artifact discovery diễn ra **inline trong loop** — tự nhiên tích lũy batch-by-batch
- Phase 2 seeds artifact stubs vào DB → `_build_artifact_context` có names từ batch 1

---

## Usage

```bash
# Full run (phase 0 → 4)
uv run python3 wiki_remaster.py

# Resume từ phase cụ thể
uv run python3 wiki_remaster.py --from-phase 3   # resume extraction loop
uv run python3 wiki_remaster.py --from-phase 4   # chỉ chạy final synthesis
uv run python3 wiki_remaster.py --from-phase 1   # skip backup

# Dry-run: skip ghi DB
uv run python3 wiki_remaster.py --dry-run
```

Valid `--from-phase` values: `0, 1, 2, 3, 4`

---

## Phase 0 — Backup DB

Backup `db/pipeline.db` → `db/pipeline.db.bak` unconditionally trước bất kỳ write nào.
Safe to re-run (overwrite backup).

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

## Phase 2 — Build Character Input + Seed Artifact Stubs

### 2a. Build top-20 character markdown

Top 20 by snapshot count:
```sql
SELECT character_id, name, COUNT(*) as snap_count
FROM wiki_snapshots
GROUP BY character_id ORDER BY snap_count DESC LIMIT 20
```

**Format file** (`data/rerun/character_input/{cid}.md`):
```markdown
## Diệp Thiếu Dương [diep_thieu_duong]
visual_anchor: <đặc điểm gốc nếu có>
aliases: Tiểu Dương, ...

| Ch start | Level | Outfit | Weapon | VFX vibes | Physical state |
|---|---|---|---|---|---|
| 26 | — | — | Thất Tinh Long Tuyền Kiếm | — | — |
...

=== Trích đoạn chương nguồn ===
[Chương 26]: <500 ký tự đầu>...
...

=== Pháp khí cần theo dõi ===
- Thất Tinh Long Tuyền Kiếm
- Câu Hồn Tác
...
```

> **Dedup snapshots**: chỉ giữ milestone rows — field thay đổi so với row trước, hoặc cách nhau ≥ 20 chương.
> **Chapter excerpts**: tối đa 8 chapters đại diện (trải đều milestone list), 500 ký tự đầu.
> **Artifact list**: global seed list (từ 2b) thêm vào cuối mỗi file — vocabulary cho LLM.

### 2b. Seed artifact stubs từ v1 weapon field

```python
# Normalize: split weapon by comma, strip, count ≥3 mentions
# Upsert stubs vào wiki_artifacts (name + name_normalized only)
```

Mục đích: `_build_artifact_context(db, chapter_start)` trả về tên pháp khí ngay từ batch 1 —
LLM có vocabulary để nhận diện pháp khí trong Phase 3 loop.

---

## Phase 3 — Main Extraction Loop

Sequential loop qua từng PENDING remaster batch. **Thứ tự là bắt buộc** để story context
tích lũy tự nhiên từ chapter 1 đến cuối.

```python
for batch in db.get_remaster_pending_batches():  # sorted by chapter_start
    text = _load_batch_text(chapter_start, chapter_end)

    # Pass 1: name scan (skip nếu wiki nhỏ)
    # Pass 2: LLM extract character deltas + artifact updates
    extraction_result, artifact_updates = _remaster_pass2(
        text, chapter_start, chapter_end,
        candidate_chars,                               # with before_chapter cutoff
        _build_artifact_context(db, chapter_start),   # artifact context grows each batch
    )

    # Merge character deltas → wiki_snapshots v2 (append-only)
    merge_extraction_result(extraction_result, db, extraction_version=2)

    # Upsert artifact metadata + artifact snapshots
    _merge_artifact_updates(db, artifact_updates, chapter_start)

    db.set_remaster_batch_status(batch_id, "MERGED")
```

**`_merge_artifact_updates` strategy**:
1. `upsert_artifact(artifact_id, name, material, visual_anchor, rarity)` — enriches stubs
2. `add_artifact_snapshot(...)` — only when `owner/condition/state` có thông tin thực

**Resume-safe**: restart bất kỳ lúc nào — loop tiếp tục từ PENDING batch.
**Consecutive fail guard**: dừng sau `wiki_max_consecutive_fail` lỗi liên tiếp.

---

## Phase 4 — Final Synthesis từ v2 Snapshots

Sau khi 707 batches MERGED, top-20 chars có v2 snapshots đầy đủ.
1 LLM call / nhân vật → UPDATE `wiki_characters` (canonical profile).

```python
for char in top_20:
    v2_snaps = [s for s in db.get_all_snapshots(cid) if s["extraction_version"] == 2]
    if not v2_snaps: continue
    md = _build_character_markdown(char, v2_snaps)
    data = llm_synthesize(md)
    db.update_character_identity(cid, ...)
```

Sau phase này, `wiki_characters.visual_anchor / faction / personality` phản ánh
những gì LLM thực sự tìm thấy khi re-read chapters (v2 data), không còn dựa vào v1.

---

## Schema (artifact tables)

```sql
CREATE TABLE IF NOT EXISTS wiki_artifacts (
    artifact_id      TEXT PRIMARY KEY,
    name             TEXT NOT NULL,
    name_normalized  TEXT NOT NULL UNIQUE,
    rarity           TEXT,
    material         TEXT,
    visual_anchor    TEXT,
    description      TEXT,
    created_at       TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS wiki_artifact_snapshots (
    id                  INTEGER PRIMARY KEY AUTOINCREMENT,
    artifact_id         TEXT NOT NULL REFERENCES wiki_artifacts(artifact_id),
    chapter_start       INTEGER NOT NULL,
    owner_id            TEXT,
    normal_state        TEXT,
    active_state        TEXT,
    condition           TEXT NOT NULL DEFAULT 'intact',  -- intact|active|damaged|evolved
    vfx_color           TEXT,
    is_key_event        INTEGER NOT NULL DEFAULT 0,
    extraction_version  INTEGER NOT NULL DEFAULT 2,
    created_at          TEXT NOT NULL
);
```

---

## File Output

```
data/rerun/
  character_input/
    diep_thieu_duong.md  ← v1 snapshots + chapter excerpts + artifact list
    ...  (top 20 files)
db/
  pipeline.db.bak        ← backup tự động tại Phase 0
```

> `wiki_seed.json` đã bị loại bỏ. Artifact discovery xảy ra inline trong Phase 3.

---

## Checklist Implementation

- [x] `wiki_remaster.py` — CLI với `--from-phase N` (0-4), `--dry-run`
- [x] Phase 0: backup DB → `db/pipeline.db.bak`
- [x] Phase 1: init `wiki_remaster_batches` từ `wiki_batches` (idempotent)
- [x] Phase 2: build markdown (v1 snaps + excerpts + artifact seed list) + seed stubs vào `wiki_artifacts`
- [x] Phase 3: extraction loop sequential, resume-safe
- [x] Phase 4: `phase4_final_synthesis` — LLM re-synthesize wiki_characters từ v2 snapshots (resume-safe)
- [x] `--dry-run` tested OK

