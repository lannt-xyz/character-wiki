---
description: "Project usage guide: CLI commands, config, architecture, and conventions for the story-with-wiki pipeline."
applyTo: "**"
---

# Story-with-Wiki — Usage Instructions

## Hai pipeline

| Script | Mục đích | Chạy khi |
|---|---|---|
| `main_wiki.py` | Crawl + v1 extraction toàn bộ chương | Lần đầu / thêm chương mới |
| `wiki_remaster.py` | Per-char salient-span extraction → v2 snapshots | Sau khi main_wiki hoàn thành |

---

## main_wiki.py — Commands

```bash
uv run python3 main_wiki.py                      # full pipeline (resume-safe)
uv run python3 main_wiki.py --from-batch <N>     # resume từ chapter_start N
uv run python3 main_wiki.py --max-batches <N>    # giới hạn N batch (test)
uv run python3 main_wiki.py --dry-run            # crawl + extract, không ghi DB
uv run python3 main_wiki.py --stats              # in tiến độ
uv run python3 main_wiki.py --export             # export wiki → data/wiki/*.json
uv run pytest tests/ -v                          # run tests
```

## wiki_remaster.py — Commands

```bash
uv run python3 wiki_remaster.py                                   # full run phase 0→4
uv run python3 wiki_remaster.py --from-phase 1                    # skip backup
uv run python3 wiki_remaster.py --from-phase 3                    # resume extraction loop
uv run python3 wiki_remaster.py --from-phase 4                    # chỉ final synthesis
uv run python3 wiki_remaster.py --dry-run                         # skip mọi DB write
uv run python3 wiki_remaster.py --from-phase 3 --max-batches <N>  # limit Phase 3
uv run python3 wiki_remaster.py --stats                           # char_batches progress
```

---

## Config

File: `config/settings.yaml` — mọi tuning đều ở đây. Override bằng env var `PIPELINE_<KEY>=value`.

### main_wiki.py settings

| Key | Default | Ý nghĩa |
|---|---|---|
| `story_slug` | `mao-son-troc-quy-nhan` | Slug truyện trên nguồn |
| `base_url` | `https://truyencv.io/...` | URL pattern chương |
| `total_chapters` | `3682` | Tổng số chương |
| `ollama_url` | `http://localhost:11434` | Địa chỉ Ollama |
| `wiki_extract_model` | `gemma4-32k:latest` | Model LLM |
| `wiki_batch_size` | `10` | Số chương mỗi batch |
| `wiki_context_threshold` | `50` | Min wiki chars trước khi bật Pass 2 |
| `wiki_max_consecutive_fail` | `5` | Fail liên tiếp tối đa trước khi dừng |
| `wiki_snapshot_min_change` | `1` | Min fields thay đổi để tạo snapshot mới |
| `llm_timeout` | `120` | Timeout (giây) mỗi request Ollama |
| `llm_max_retries` | `3` | Số lần retry khi LLM lỗi |
| `crawler_rate_limit` | `1.0` | Giây giữa các request crawl |

### wiki_remaster.py settings

| Key | Default | Ý nghĩa |
|---|---|---|
| `char_top_limit` | `50` | Top-N nhân vật xử lý trong Phase 1-4 (0 = all) |
| `char_segment_size` | `20` | Số chương tối đa mỗi char_batch segment |
| `char_gap_threshold` | `50` | Chapter gap để tách segment mới |
| `char_span_budget` | `20000` | Max ký tự salient spans gửi LLM mỗi call |

---

## Data Paths

| Path | Nội dung |
|---|---|
| `data/chapters/chuong-XXXX.txt` | Raw text mỗi chương (crawled) |
| `data/rerun/character_input/{id}.md` | Character seed context (Phase 2 output) |
| `data/rerun/trace/{id}/` | LLM request/response per char batch (Phase 3) |
| `data/llm_requests/` | Batch-level LLM trace files |
| `data/wiki/{name}.json` | Wiki export (sau `main_wiki --export`) |
| `db/pipeline.db` | SQLite — nguồn chân lý |
| `db/*.db.bak` | DB backups (Phase 0) |
| `logs/wiki.log` | Full DEBUG log — main pipeline |
| `logs/wiki_remaster.log` | Full DEBUG log — remaster pipeline |

---

## Architecture

```
main_wiki.py (CLI)
    └── wiki/orchestrator.py  — batch loop PENDING→CRAWLED→EXTRACTED→MERGED
            ├── crawler/scraper.py    — HTTP + HTML parse
            ├── crawler/storage.py   — ghi .txt files
            ├── wiki/extractor.py    — 2-pass LLM qua Ollama REST
            ├── wiki/merger.py       — append-only snapshot v1 → SQLite
            └── wiki/validator.py    — sanity check + export JSON

wiki_remaster.py (CLI)
    ├── Phase 0  — Backup DB
    ├── Phase 1  — Build mention index + char_batches (reset + rebuild)
    ├── Phase 2  — Character markdown + artifact stubs từ v1 data
    ├── Phase 3  — Per-character extraction loop
    │              (salient spans → LLM → v2 snapshots + artifacts)
    └── Phase 4  — Final synthesis (wiki_characters ← v2 snapshots, top-N chars)
```

## Pipeline States

**main_wiki.py** (per batch):

`PENDING` → `CRAWLED` → `EXTRACTED` → `MERGED`

**wiki_remaster.py** (per char_batch):

`PENDING` → `MERGED` (extracted inline, no intermediate state)

- Resume-safe: restart bất kỳ lúc nào, pipeline tiếp tục từ batch chưa MERGED.
- Sau mỗi batch extraction: Ollama tự offload VRAM (`keep_alive=0`).
- Snapshots là **append-only** — không bao giờ UPDATE, chỉ INSERT.
- Safety guard: nếu có MERGED char_batches và chạy `--from-phase 0`, exit với lỗi.

---

## Đổi truyện

1. Sửa `config/settings.yaml`: `story_slug`, `base_url`, `total_chapters`
2. Nếu HTML nguồn khác cấu trúc: sửa selector trong `crawler/scraper.py`
3. Xóa `db/pipeline.db` và `data/` nếu muốn bắt đầu lại từ đầu

---

## SQLite Schema (đầy đủ)

### Pipeline 1 — main_wiki.py

```sql
chapters(chapter_num PK, title, url, status, crawled_at, error_msg)
wiki_batches(batch_id PK, chapter_start, chapter_end, status, extraction_version)
wiki_characters(character_id PK, name, name_normalized UNIQUE, aliases_json,
                gender, faction, visual_anchor, personality, remaster_version,
                created_at, updated_at)
wiki_snapshots(id PK, character_id FK, chapter_start, is_active, level, outfit,
               weapon, vfx_vibes, physical_description, visual_importance,
               extraction_version, created_at)
wiki_relations(id PK, character_id FK, related_name, description, chapter_start)
```

### Pipeline 2 — wiki_remaster.py

```sql
wiki_mention_index(id PK, character_id, chapter_num)         -- UNIQUE(char, ch)
wiki_char_batches(batch_id PK, character_id, segment_start, segment_end,
                  remaster_version, status, extracted_at, merged_at)
wiki_artifacts(artifact_id PK, name, name_normalized UNIQUE, rarity, material,
               visual_anchor, description, created_at)
wiki_artifact_snapshots(id PK, artifact_id FK, chapter_start, owner_id,
                        normal_state, active_state, condition, vfx_color,
                        is_key_event, extraction_version, created_at)
wiki_remaster_batches(batch_id PK, chapter_start, chapter_end,
                      remaster_version, status)               -- legacy
```
