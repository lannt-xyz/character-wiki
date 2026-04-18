# Story-with-Wiki — Workflow Tổng Quan

Tài liệu mô tả toàn bộ luồng làm việc của project, từ crawl raw text đến wiki nhân vật & pháp khí chất lượng cao.

---

## Tổng quan hai pipeline

```mermaid
flowchart TD
    A([Bắt đầu]) --> B["config/settings.yaml\nstory_slug, base_url, total_chapters"]
    B --> P1

    subgraph P1["Pipeline 1 — main_wiki.py"]
        direction TB
        P1A[Crawler\nHTTP + HTML parse] --> P1B[Storage\nghi .txt files]
        P1B --> P1C[Extractor\n2-pass LLM extraction]
        P1C --> P1D[Merger\nappend-only v1 snapshots]
        P1D --> P1E{Còn batch\nPENDING?}
        P1E -- Có --> P1A
        P1E -- Hết --> P1F[Validator\nsanity check + JSON export]
    end

    P1F --> DONE1([wiki_batches: tất cả MERGED\nv1 snapshots đầy đủ])
    DONE1 --> P2

    subgraph P2["Pipeline 2 — wiki_remaster.py"]
        direction TB
        PH0[Phase 0\nBackup DB] --> PH1
        PH1[Phase 1\nMention Index + Char Batches] --> PH2
        PH2[Phase 2\nCharacter Markdown + Artifact Stubs] --> PH3
        PH3[Phase 3\nPer-Character Extraction Loop] --> PH4
        PH4[Phase 4\nFinal Synthesis]
    end

    PH4 --> DONE2([wiki_characters: remaster_version=2\nwiki_artifacts: visual_anchor đầy đủ])
```

---

## Pipeline 1 — main_wiki.py

### Batch State Machine

```mermaid
stateDiagram-v2
    [*] --> PENDING : chapter range registered
    PENDING --> CRAWLED : scraper.py — HTTP fetch + HTML parse
    CRAWLED --> EXTRACTED : extractor.py — Pass1 + Pass2 LLM
    EXTRACTED --> MERGED : merger.py — append snapshots → SQLite
    MERGED --> [*]

    note right of EXTRACTED
        Sau mỗi batch LLM call:
        keep_alive=0 → VRAM freed
    end note
```

### 2-pass LLM Extraction (per batch)

```mermaid
sequenceDiagram
    participant O as orchestrator.py
    participant E as extractor.py
    participant L as Ollama REST
    participant DB as SQLite

    O->>E: batch_text (10 chapters)
    E->>L: Pass 1 — Tìm tên nhân vật trong batch
    L-->>E: [NameEntry list]
    E->>DB: lookup existing wiki_characters
    alt context đủ lớn (> wiki_context_threshold)
        E->>L: Pass 2 — Extract character deltas\n(character context + batch text)
        L-->>E: ExtractionResult\n(new_chars + updated_chars)
    end
    E-->>O: ExtractionResult
    O->>DB: merger.py\nINSERT snapshots v1\nUPSERT characters
    O->>DB: set_batch_status MERGED
```

---

## Pipeline 2 — wiki_remaster.py

### Phase Flow Tổng Quan

```mermaid
flowchart LR
    subgraph PH0["Phase 0 — Backup"]
        B0["pipeline.db → *.db.bak\n(timestamp)"]
    end

    subgraph PH1["Phase 1 — Init"]
        B1A["get_top_chars\n(char_top_limit)"] --> B1B["Scan all chapters\nnormalized text match"]
        B1B --> B1C["wiki_mention_index\nfilled"]
        B1C --> B1D["Group mentions → segments\n(char_segment_size / char_gap_threshold)"]
        B1D --> B1E["wiki_char_batches\nall PENDING"]
    end

    subgraph PH2["Phase 2 — Build Input"]
        B2A["Top-20 chars → .md files\n(v1 snapshots + excerpts)"] --> B2B["weapon field v1\n≥3 mentions → artifact names"]
        B2B --> B2C["wiki_artifacts\nstub records seeded"]
    end

    subgraph PH3["Phase 3 — Extraction Loop"]
        B3A{Pending\nchar_batch?}
        B3A -- Có --> B3B["Load chapters\nfor segment"]
        B3B --> B3C["Extract salient spans\n(paragraph filter + context window)"]
        B3C --> B3D{Spans\nfound?}
        B3D -- Có --> B3E["LLM CharPass\n1 call / char_batch"]
        B3E --> B3F["Merge v2 snapshots\n+ artifact updates"]
        B3F --> B3G["set MERGED"]
        B3G --> B3A
        B3D -- Không --> B3G
        B3A -- Hết --> B3H([Phase 3 done])
    end

    subgraph PH4["Phase 4 — Synthesis"]
        B4A["get_top_chars\n(char_top_limit)"] --> B4B{remaster_version\n== 2?}
        B4B -- Không --> B4C["Build char markdown\nfrom v2 snapshots"]
        B4C --> B4D["LLM synthesis\n1 call / char"]
        B4D --> B4E["UPDATE wiki_characters\nvisual_anchor, faction,\nrelations, remaster_version=2"]
        B4E --> B4B
        B4B -- Đã xong\nskip --> B4F([Phase 4 done])
    end

    PH0 --> PH1 --> PH2 --> PH3 --> PH4
```

### Phase 3 — Salient Span Extraction

```mermaid
flowchart TD
    A["char_batch\nchar_id + segment_start + segment_end"] --> B

    B["Load chapters từ DB\n(range query)"] --> C

    C["Split mỗi chương\nthành paragraphs"] --> D

    D{"Para chứa\ntên/alias\nnhân vật?"}
    D -- Có --> E["Include paragraph\n+ predecessor para\n(context window)"]
    D -- Không --> F[Skip]
    E --> G
    F --> G

    G{"Total chars\n> char_span_budget?"}
    G -- Có --> H[Truncate tại budget]
    G -- Không --> I[Continue]
    H --> J[spans_text ready]
    I --> J

    J --> K["Load seed_context\n.md file hoặc v1 snapshots fallback"]
    J --> L["Build artifact_context\ncandidate artifacts trong segment"]

    K --> M["LLM CharPass call\nchar-focused extraction"]
    L --> M

    M --> N["CharPassResult\nsnapshots + artifact_updates + new_aliases"]
    N --> O["INSERT v2 snapshots\n(skip nếu đã tồn tại)"]
    N --> P["Upsert wiki_artifacts\n+ INSERT artifact snapshots"]
    N --> Q["Append new_aliases\nvào wiki_characters"]
```

---

## Data Flow — DB Write Pattern

```mermaid
flowchart LR
    subgraph V1["v1 — main_wiki.py viết"]
        direction TB
        WB["wiki_batches\nstatus=MERGED"]
        WC1["wiki_characters\nremaster_version=1"]
        WS1["wiki_snapshots\nextraction_version=1"]
        WR["wiki_relations"]
    end

    subgraph V2["v2 — wiki_remaster.py viết"]
        direction TB
        MI["wiki_mention_index"]
        CB["wiki_char_batches\nstatus=MERGED"]
        WS2["wiki_snapshots\nextraction_version=2"]
        WA["wiki_artifacts"]
        WAS["wiki_artifact_snapshots"]
        WC2["wiki_characters\nremaster_version=2\nvisual_anchor updated"]
    end

    WC1 -->|Phase 1 đọc| MI
    WC1 -->|Phase 2 đọc| MI
    WS1 -->|Phase 2 đọc| MI
    WS2 -->|Phase 4 đọc| WC2
    WA  -->|Phase 3 enriches| WAS
```

---

## SQLite Schema (ER Diagram)

```mermaid
erDiagram
    chapters {
        int chapter_num PK
        text title
        text url
        text status
        text crawled_at
    }

    wiki_characters {
        text character_id PK
        text name
        text name_normalized
        text aliases_json
        text gender
        text faction
        text visual_anchor
        text personality
        int remaster_version
    }

    wiki_snapshots {
        int id PK
        text character_id FK
        int chapter_start
        text level
        text outfit
        text weapon
        text vfx_vibes
        int visual_importance
        int extraction_version
    }

    wiki_relations {
        int id PK
        text character_id FK
        text related_name
        text description
        int chapter_start
    }

    wiki_batches {
        int batch_id PK
        int chapter_start
        int chapter_end
        text status
    }

    wiki_char_batches {
        int batch_id PK
        text character_id
        int segment_start
        int segment_end
        text status
    }

    wiki_mention_index {
        int id PK
        text character_id
        int chapter_num
    }

    wiki_artifacts {
        text artifact_id PK
        text name
        text rarity
        text material
        text visual_anchor
    }

    wiki_artifact_snapshots {
        int id PK
        text artifact_id FK
        int chapter_start
        text owner_id
        text condition
        int extraction_version
    }

    wiki_characters ||--o{ wiki_snapshots : "has snapshots"
    wiki_characters ||--o{ wiki_relations : "has relations"
    wiki_characters ||--o{ wiki_char_batches : "processed in"
    wiki_characters ||--o{ wiki_mention_index : "mentioned in"
    wiki_artifacts ||--o{ wiki_artifact_snapshots : "has snapshots"
```

---

## Safety Guards

| Guard | Vị trí | Hành vi |
|---|---|---|
| Phase 1 reset guard | `main()` — `wiki_remaster.py` | Nếu có char_batches MERGED và `--from-phase 0`, exit lỗi. Dùng `--from-phase 3` để resume hoặc `--from-phase 1` để force reinit |
| Phase 3 resume | `phase3_char_extraction_loop()` | Chỉ xử lý PENDING batches — tự động bỏ qua MERGED |
| Phase 4 resume | `phase4_final_synthesis()` | Skip chars đã có `remaster_version=2` |
| Ollama VRAM | `wiki/extractor.py` | `keep_alive=0` sau mỗi LLM call |

---

## Luồng làm việc điển hình

```bash
# Bước 1: Crawl và extract lần đầu
uv run python3 main_wiki.py

# Theo dõi tiến độ (cửa sổ khác)
watch -n 30 "uv run python3 main_wiki.py --stats"

# Bước 2: Sau khi main_wiki.py xong — chạy remaster
uv run python3 wiki_remaster.py

# Theo dõi Phase 3
watch -n 30 "uv run python3 wiki_remaster.py --stats"

# Nếu bị interrupt — resume an toàn từ Phase 3
uv run python3 wiki_remaster.py --from-phase 3

# Chỉ chạy lại Phase 4 (sau khi Phase 3 xong)
uv run python3 wiki_remaster.py --from-phase 4

# Export kết quả cuối
uv run python3 main_wiki.py --export
```
