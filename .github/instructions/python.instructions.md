---
description: "Use when writing Python code for this project. Enforces tech stack conventions: pydantic for all JSON schemas, loguru for logging, tenacity for retry, asyncio only for I/O-bound tasks, FFmpeg NVENC encoding, uv for packages."
applyTo: "**/*.py"
---

# Python Coding Conventions

## Imports & Package Management

- Package manager: `uv` — never `pip install` directly in docs/scripts
- Never use `ollama` Python SDK; call Ollama via `httpx` REST directly
- Never use raw `ffmpeg` subprocess string building — use `ffmpeg-python` wrapper

## Config — Always Type-Safe

Use `pydantic-settings` + YAML. Never access config via plain dicts or `os.environ` directly.

```python
# config/settings.py
from pydantic_settings import BaseSettings, SettingsConfigDict
from pydantic import field_validator

class Settings(BaseSettings):
    model_config = SettingsConfigDict(yaml_file="config/settings.yaml")
    story_slug: str
    base_url: str
    total_chapters: int
    # ...

settings = Settings()  # Fails fast at startup if config invalid
```

## JSON Schema — Always Pydantic Models

Never parse LLM JSON output into raw dicts. Always validate through a Pydantic model.

```python
# models/schemas.py
from pydantic import BaseModel

class ShotScript(BaseModel):
    scene_prompt: str          # English, for ComfyUI
    narration_text: str        # Vietnamese, for TTS
    duration_sec: int = 6
    is_key_shot: bool = False
    characters: list[str] = [] # [] = scene-only shot, no IPAdapter needed
```

## Logging — Always Loguru

Never use `print()` or `logging` stdlib in pipeline code. Always `loguru`.

```python
from loguru import logger

# In orchestrator setup:
logger.add("logs/pipeline.log", rotation="100MB")
logger.add("logs/episode-{episode_num:03d}.log", ...)

# Usage:
logger.info("Phase 3 started | episode={}", episode_num)
logger.error("ComfyUI OOM | episode={} shot={}", episode_num, shot_idx)
```

## Retry — Always Tenacity

All external calls (Ollama, ComfyUI, edge-tts, httpx crawler) must have tenacity retry.

```python
from tenacity import retry, stop_after_attempt, wait_exponential, retry_if_exception_type

@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential(multiplier=1, min=2, max=30),
    retry=retry_if_exception_type((httpx.HTTPError, httpx.TimeoutException)),
)
async def fetch_chapter(url: str) -> str: ...

# LLM JSON parse: max 3 retries
@retry(stop=stop_after_attempt(3), wait=wait_exponential(min=1, max=5))
def call_llm_structured(prompt: str) -> ShotScript: ...
```

## Async — Only When I/O Bound

| Context | Use | Reason |
|---------|-----|--------|
| Crawler | `asyncio + httpx` | Network I/O bound; semaphore(1) for rate-limit |
| LLM calls | **Sync** | Serial GPU; async adds no throughput |
| ComfyUI API | **Sync + polling** | Serial GPU; batch latency insensitive |
| TTS generation | `asyncio + edge-tts` | Network I/O; ~900 shots → parallel saves 25min |
| FFmpeg zoompan clips | `ThreadPoolExecutor(4)` | CPU parallel; clips are independent |
| FFmpeg NVENC encode | **Sync, serial** | Single GPU encoder |

Never use asyncio for GPU-bound tasks. It does not improve throughput and complicates code.

## FFmpeg — NVENC Only

```python
# CORRECT
stream = ffmpeg.output(video, audio, output_path, vcodec="h264_nvenc", acodec="aac")

# WRONG — never on this machine
stream = ffmpeg.output(video, audio, output_path, vcodec="libx264")
```

## FFmpeg Security — Sanitize LLM Output

`narration_text` from LLM is untrusted. Before using in FFmpeg `drawtext` or SRT:

```python
def sanitize_for_srt(text: str) -> str:
    # SRT special chars: angle brackets, ampersand
    return text.replace("&", "&amp;").replace("<", "").replace(">", "")

def sanitize_for_drawtext(text: str) -> str:
    # FFmpeg drawtext special chars: colon, single quote, backslash
    return text.replace("\\", "\\\\").replace("'", "\\'").replace(":", "\\:")
```

## Error Handling

- Validate at system boundaries only (HTTP response, LLM JSON output, FFmpeg output)
- Inside pipeline modules, raise specific exceptions; catch at orchestrator level
- Never silently swallow exceptions in pipeline code — always log then raise

## State — Explicit, Never In-Memory

Pipeline state lives in SQLite (`db/pipeline.db`). Never track progress in Python variables across restarts.

```python
# CORRECT
def mark_episode_done(episode_num: int, phase: str): ...  # writes to DB

# WRONG
completed_episodes: list[int] = []  # lost on crash
```
