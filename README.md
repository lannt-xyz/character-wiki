# Story with Wiki

Tự động crawl truyện chữ và xây dựng **character wiki** bằng LLM (Ollama) — pipeline hoạt động hoàn toàn offline, resume-safe, append-only.

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
- Model LLM đã pull, ví dụ: `ollama pull gemma4-32k:latest`

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
story_slug: "mao-son-troc-quy-nhan"
base_url: "https://truyencv.io/truyen/{story_slug}/chuong-{n}/"
total_chapters: 3534

ollama_url: "http://localhost:11434"
wiki_extract_model: "gemma4-32k:latest"
wiki_batch_size: 5        # số chương mỗi batch
```

Xem đầy đủ các option tại [config/settings.yaml](config/settings.yaml).

Override bằng env var với prefix `PIPELINE_`, ví dụ:
```bash
PIPELINE_WIKI_BATCH_SIZE=10 uv run python3 main_wiki.py
```

---

## Sử dụng

```bash
# Chạy toàn bộ pipeline (resume từ batch chưa xong)
uv run python3 main_wiki.py

# Resume từ batch cụ thể (chapter_start)
uv run python3 main_wiki.py --from-batch 100

# Chỉ chạy N batch (test)
uv run python3 main_wiki.py --max-batches 5

# Crawl + extract nhưng không ghi DB
uv run python3 main_wiki.py --dry-run

# Xem tiến độ
uv run python3 main_wiki.py --stats

# Export wiki ra JSON
uv run python3 main_wiki.py --export
```

---

## Cấu trúc output

```
data/
  chapters/
    chuong-0001.txt        # nội dung chương thô
    chuong-0002.txt
    ...
  wiki/
    {tên_nhân_vật}.json    # wiki từng nhân vật (sau --export)

db/
  pipeline.db              # SQLite — nguồn chân lý

logs/
  wiki.log                 # log DEBUG đầy đủ
```

### Wiki JSON (mỗi nhân vật)

```json
{
  "id": "lý_tiểu_long",
  "name": "Lý Tiểu Long",
  "aliases": ["Tiểu Long"],
  "gender": "male",
  "faction": "...",
  "snapshots": [
    {
      "chapter_start": 1,
      "chapter_end": 5,
      "level": "...",
      "physical_description": "...",
      "outfit": "...",
      "personality": "...",
      "relationships": {...}
    }
  ]
}
```

---

## Kiến trúc pipeline

```
[Crawler] → raw text → [Extractor (LLM)] → delta patches → [Merger] → SQLite
                                                                          ↓
                                                               [Validator + Export]
```

1. **Crawler** — tải HTML, parse nội dung chương, lưu `.txt`
2. **Extractor** — 2-pass LLM:
   - Pass 1: scan tên nhân vật trong batch
   - Pass 2: trích delta thay đổi mỗi nhân vật
3. **Merger** — append-only snapshot vào SQLite; persistent fields kế thừa, transient reset
4. **Validator** — sanity check + export JSON

Sau mỗi batch, Ollama tự **offload VRAM** (`keep_alive=0`).

---

## Chạy tests

```bash
uv run pytest tests/ -v
```

---

## Đổi truyện

Chỉ cần sửa `config/settings.yaml`:
```yaml
story_slug: "ten-truyen-moi"
base_url: "https://domain.com/truyen/{story_slug}/chuong-{n}/"
total_chapters: 1000
```

Nếu cấu trúc HTML nguồn thay đổi, sửa selector tại `crawler/scraper.py`.
# character-wiki
