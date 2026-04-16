---
description: "Project usage guide: CLI commands, config, architecture, and conventions for the story-with-wiki pipeline."
applyTo: "**"
---

# Story-with-Wiki — Usage Instructions

## Commands (always via `uv run`)

```bash
# Full pipeline run (resume-safe)
uv run python3 main_wiki.py

# Resume từ batch cụ thể (chapter_start)
uv run python3 main_wiki.py --from-batch <N>

# Giới hạn số batch (test/debug)
uv run python3 main_wiki.py --max-batches <N>

# Dry-run: crawl + extract, không ghi DB
uv run python3 main_wiki.py --dry-run

# In tiến độ (merged/total batches, characters, snapshots)
uv run python3 main_wiki.py --stats

# Export wiki ra data/wiki/*.json
uv run python3 main_wiki.py --export

# Run tests
uv run pytest tests/ -v
```

## Config

File: `config/settings.yaml` — mọi tuning đều ở đây.

| Key | Default | Ý nghĩa |
|---|---|---|
| `story_slug` | `mao-son-troc-quy-nhan` | Slug truyện trên nguồn |
| `base_url` | `https://truyencv.io/...` | URL pattern chương (`{story_slug}`, `{n}`) |
| `total_chapters` | `3534` | Tổng số chương |
| `ollama_url` | `http://localhost:11434` | Địa chỉ Ollama |
| `wiki_extract_model` | `gemma4-32k:latest` | Model LLM dùng cho extraction |
| `wiki_batch_size` | `5` | Số chương mỗi batch |
| `wiki_context_threshold` | `50` | Min wiki chars trước khi bật Pass 2 |
| `wiki_max_consecutive_fail` | `5` | Số lần fail liên tiếp trước khi dừng |
| `wiki_snapshot_min_change` | `1` | Min fields thay đổi để tạo snapshot mới |
| `llm_timeout` | `120` | Timeout (giây) mỗi request Ollama |
| `llm_max_retries` | `3` | Số lần retry khi LLM lỗi |
| `crawler_rate_limit` | `1.0` | Giây giữa các request crawl |

Override bằng env var: `PIPELINE_<KEY>=value`.

## Data Paths

| Path | Nội dung |
|---|---|
| `data/chapters/chuong-XXXX.txt` | Raw text mỗi chương |
| `data/wiki/{name}.json` | Wiki export (sau `--export`) |
| `db/pipeline.db` | SQLite — nguồn chân lý |
| `logs/wiki.log` | Full DEBUG log |

## Architecture

```
main_wiki.py (CLI)
    └── wiki/orchestrator.py  — batch loop, trạng thái PENDING→CRAWLED→EXTRACTED→MERGED
            ├── crawler/scraper.py    — HTTP + HTML parse
            ├── crawler/storage.py   — ghi .txt files
            ├── wiki/extractor.py    — 2-pass LLM qua Ollama REST
            ├── wiki/merger.py       — append-only snapshot vào SQLite
            └── wiki/validator.py    — sanity check + export JSON
```

## Pipeline States (mỗi batch)

`PENDING` → `CRAWLED` → `EXTRACTED` → `MERGED`

- Resume-safe: restart bất kỳ lúc nào, pipeline tiếp tục từ batch chưa MERGED.
- Sau mỗi batch extraction: Ollama tự offload VRAM (`keep_alive=0`).
- Snapshots là **append-only** — không bao giờ UPDATE, chỉ INSERT.

## Đổi truyện

1. Sửa `config/settings.yaml`: `story_slug`, `base_url`, `total_chapters`
2. Nếu HTML nguồn khác cấu trúc: sửa selector trong `crawler/scraper.py`
3. Xóa `db/pipeline.db` và `data/` nếu muốn bắt đầu lại từ đầu

## SQLite Schema (tóm tắt)

```sql
chapters(id, story_slug, chapter_num, title, content, crawled_at)
wiki_characters(id TEXT PK, name, aliases_json, gender, faction, ...)
wiki_snapshots(id, character_id, chapter_start, level, physical_description, outfit, ...)
wiki_batches(batch_id, chapter_start, chapter_end, status, ...)
```
