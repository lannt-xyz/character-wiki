# Story with Wiki

Tự động crawl truyện chữ và xây dựng **character wiki + artifact wiki** bằng LLM (Ollama) — hai pipeline hoạt động hoàn toàn offline, resume-safe, append-only.

---

## Stack

| Layer | Thư viện |
|---|---|
| Config | `pydantic-settings[yaml]` |
| Logging | `loguru` |
| HTTP | `httpx` |
| Retry | `tenacity` |
| HTML parse | `beautifulsoup4` + `lxml` |
| LLM | Ollama REST API |
| DB | SQLite (stdlib) |
| Test | `pytest` |
| Package mgr | `uv` |

---

## Yêu cầu

- Python ≥ 3.11
- [uv](https://docs.astral.sh/uv/) (`curl -LsSf https://astral.sh/uv/install.sh | sh`)
- [Ollama](https://ollama.com/) đang chạy tại `http://localhost:11434`
- Model LLM đã pull: `ollama pull gemma4-32k:latest`

---

## Cài đặt

```bash
git clone <repo>
cd story-with-wiki
uv sync
```

---

## Cấu hình

Chỉnh `config/settings.yaml`:

```yaml
# Đổi truyện — chỉ cần thay 3 dòng này
story_slug:     "mao-son-troc-quy-nhan"
base_url:       "https://truyencv.io/truyen/{story_slug}/chuong-{n}/"
total_chapters: 3682

ollama_url:         "http://localhost:11434"
wiki_extract_model: "gemma4-32k:latest"
wiki_batch_size:    10    # số chương mỗi batch (main pipeline)
char_top_limit:     50    # top-N nhân vật cho remaster (mặc định 50)
```

Xem đầy đủ các option tại [config/settings.yaml](config/settings.yaml).

Override bằng env var với prefix `PIPELINE_`:
```bash
PIPELINE_CHAR_TOP_LIMIT=100 uv run python3 wiki_remaster.py
```

---

## Hai pipeline

### Pipeline 1 — `main_wiki.py` (lần đọc đầu)

Crawl HTML → parse chương → 2-pass LLM extraction → ghi snapshots v1 vào SQLite.

```bash
# Chạy toàn bộ (resume từ batch chưa xong)
uv run python3 main_wiki.py

# Resume từ batch cụ thể (chapter_start)
uv run python3 main_wiki.py --from-batch 100

# Chỉ chạy N batch (test)
uv run python3 main_wiki.py --max-batches 5

# Dry-run: không ghi DB
uv run python3 main_wiki.py --dry-run

# Xem tiến độ
uv run python3 main_wiki.py --stats

# Export wiki ra JSON
uv run python3 main_wiki.py --export
```

### Pipeline 2 — `wiki_remaster.py` (remaster chất lượng cao)

Re-read raw chapters theo từng nhân vật → per-character salient-span extraction → v2 snapshots → final synthesis. Chạy **sau khi** `main_wiki.py` hoàn thành.

```bash
# Full run phase 0 → 4
uv run python3 wiki_remaster.py

# Resume từ phase cụ thể
uv run python3 wiki_remaster.py --from-phase 3     # resume extraction loop
uv run python3 wiki_remaster.py --from-phase 4     # chỉ final synthesis
uv run python3 wiki_remaster.py --from-phase 1     # skip backup

# Dry-run
uv run python3 wiki_remaster.py --dry-run

# Limit phase 3 (test)
uv run python3 wiki_remaster.py --from-phase 3 --max-batches 10

# Xem tiến độ
uv run python3 wiki_remaster.py --stats
```

| Phase | Tên | Mô tả |
|---|---|---|
| 0 | Backup DB | Copy `db/pipeline.db` → timestamp backup |
| 1 | Init batches | Build mention index + char_batches cho top-N nhân vật |
| 2 | Build input | Build character markdown + seed artifact stubs từ v1 data |
| 3 | Extraction loop | Per-character LLM extraction — salient spans → v2 snapshots |
| 4 | Final synthesis | LLM re-synthesize `wiki_characters` từ v2 snapshots |

---

## Cấu trúc output

```
data/
  chapters/
    chuong-0001.txt              # nội dung chương thô (crawled)
  rerun/
    character_input/
      {character_id}.md          # character seed context (Phase 2 output)
    trace/
      {character_id}/            # LLM request/response per char batch (Phase 3)
  llm_requests/                  # batch-level LLM trace files
  wiki/
    {tên_nhân_vật}.json          # wiki từng nhân vật (sau main_wiki --export)

db/
  pipeline.db                    # SQLite — nguồn chân lý
  *.db.bak                       # DB backups (Phase 0)

logs/
  wiki.log                       # log DEBUG — main pipeline
  wiki_remaster.log              # log DEBUG — remaster pipeline
```

### Wiki JSON (mỗi nhân vật)

```json
{
  "id": "nhan_vat_a",
  "name": "Nhân Vật A",
  "aliases": ["Alias 1"],
  "gender": "male",
  "faction": "Môn phái X",
  "visual_anchor": "tall figure in black robe, golden eyes",
  "personality": "lạnh lùng, kiên quyết",
  "snapshots": [
    {
      "chapter_start": 1,
      "level": "Luyện Khí Tầng 1",
      "outfit": "áo trắng",
      "weapon": "trường kiếm",
      "visual_importance": 7
    }
  ]
}
```

---

## Kiến trúc

```
┌─────────────────────── main_wiki.py ──────────────────────────┐
│  orchestrator.py — batch loop PENDING→CRAWLED→EXTRACTED→MERGED │
│    ├── scraper.py     HTTP + HTML parse                        │
│    ├── storage.py     ghi .txt files                           │
│    ├── extractor.py   2-pass LLM (Pass1: names, Pass2: deltas) │
│    ├── merger.py      append-only snapshots v1 → SQLite        │
│    └── validator.py   sanity check + JSON export               │
└───────────────────────────────────────────────────────────────┘
                              ↓ (sau khi hoàn thành)
┌─────────────────────── wiki_remaster.py ──────────────────────┐
│  Phase 0: Backup DB                                            │
│  Phase 1: Build mention index + char_batches (top-N chars)     │
│  Phase 2: Character markdown + artifact stubs (v1 → seed)      │
│  Phase 3: Per-char extraction loop → v2 snapshots + artifacts  │
│  Phase 4: Final LLM synthesis → wiki_characters remaster_v=2   │
└───────────────────────────────────────────────────────────────┘
```

---

## SQLite Schema

```sql
-- Pipeline 1
chapters(chapter_num PK, title, url, status, crawled_at)
wiki_batches(batch_id PK, chapter_start, chapter_end, status)
wiki_characters(character_id PK, name, aliases_json, gender, faction,
                visual_anchor, personality, remaster_version)
wiki_snapshots(id PK, character_id FK, chapter_start, level, outfit.
               weapon, vfx_vibes, visual_importance, extraction_version)
wiki_relations(id PK, character_id FK, related_name, description, chapter_start)

-- Pipeline 2 (wiki_remaster)
wiki_mention_index(id PK, character_id, chapter_num)      -- UNIQUE(char, ch)
wiki_char_batches(batch_id PK, character_id, segment_start, segment_end, status)
wiki_artifacts(artifact_id PK, name, rarity, material, visual_anchor)
wiki_artifact_snapshots(id PK, artifact_id FK, chapter_start,
                        owner_id, condition, extraction_version=2)
```

---

## Chạy tests

```bash
uv run pytest tests/ -v
```

---

## Đổi truyện

1. Sửa `config/settings.yaml`: `story_slug`, `base_url`, `total_chapters`
2. Nếu cấu trúc HTML nguồn thay đổi: sửa selector tại `crawler/scraper.py`
3. Xóa `db/pipeline.db` và `data/` để bắt đầu lại từ đầu

---

## Workflow tổng quan

Xem [WORKFLOW.md](WORKFLOW.md) để hiểu toàn bộ flow kèm sơ đồ Mermaid.
