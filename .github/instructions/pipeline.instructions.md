---
description: "Use when designing or implementing pipeline phases, orchestrator, VRAM management, state machine, or ComfyUI/Ollama integration. Enforces sequential GPU phases, health checks, SQLite state, cleanup strategy."
applyTo: "pipeline/**/*.py, image_gen/**/*.py, llm/**/*.py, video/**/*.py"
---

# Pipeline Architecture Rules

## VRAM Management — Sequential, Never Concurrent

RTX 5060 Ti has 8GB VRAM. Ollama and ComfyUI must **never run at the same time**.

```
[Phase 2 LLM]     → qwen2.5:7b Q8_0 = ~7.7GB  ← full card
[Phase 3 ComfyUI] → SDXL + IPAdapter = ~6-7GB  ← only after ollama stop
[Phase 3 SVD]     → ~7-8GB                     ← OOM risk, must fallback
[Phase 5 NVENC]   → ~1GB                        ← safe concurrent with CPU
```

Always use `vram_manager.py` to transition between LLM and ComfyUI phases:

```python
# pipeline/vram_manager.py pattern
def unload_ollama():
    subprocess.run(["ollama", "stop"], check=True)
    time.sleep(3)  # wait for VRAM release
    assert_gpu_memory_free(threshold_gb=7.0)

def load_ollama():
    subprocess.run(["ollama", "serve"], ...)
```

## Health Check Before Every Phase

Before starting any phase that calls an external service (Ollama, ComfyUI), run a health check. Never assume the service is up.

```python
# CORRECT
def run_phase_3(episode_num: int):
    comfyui_client.health_check()  # raises if down
    vram_manager.unload_ollama()
    generate_images(episode_num)

# WRONG — no health check
def run_phase_3(episode_num: int):
    generate_images(episode_num)  # hangs silently if ComfyUI is down
```

## State Machine — VALIDATED is the Final State

Episode state transitions are strictly ordered:

```
PENDING → CRAWLED → SUMMARIZED → SCRIPTED → IMAGES_DONE → AUDIO_DONE → VIDEO_DONE → VALIDATED
```

- Never skip states
- Re-running `--from-phase X` re-sets state to the state before X and replays forward
- VALIDATED is the only state after which cleanup (delete images/audio) is allowed

## SVD Fallback — Always Have One

If ComfyUI returns OOM error during SVD generation, fall back to zoompan for that shot. Never abort the episode.

```python
def generate_key_shot_clip(shot: ShotScript) -> Path:
    if settings.enable_svd:
        try:
            return comfyui_client.generate_svd(shot)
        except ComfyUIOutOfMemoryError:
            logger.warning("SVD OOM, falling back to zoompan | shot={}", shot.index)
    return ffmpeg_zoompan(shot)  # fallback always available
```

## Cleanup — Only After VALIDATED

```python
# CORRECT: cleanup gated on validator pass
def finalize_episode(episode_num: int):
    validator.assert_episode(episode_num)          # raises if invalid
    state.set_status(episode_num, "VALIDATED")
    shutil.rmtree(f"data/images/episode-{episode_num:03d}")
    shutil.rmtree(f"data/audio/episode-{episode_num:03d}")
    # Keep: data/scripts/, data/summaries/, data/thumbnails/, data/output/
```

## ComfyUI Workflow — Dispatch by Shot Type

```python
def select_workflow(shot: ShotScript) -> str:
    if shot.is_key_shot and settings.enable_svd:
        return "image_gen/workflows/svd_keyshot.json"
    elif shot.characters:  # non-empty list
        return "image_gen/workflows/txt2img_ipadapter.json"
    else:
        return "image_gen/workflows/txt2img_scene.json"
```

## ETA Estimation — Use episode_timings Table

After Episode 1 completes, calculate and log ETA for remaining episodes:

```python
def log_eta(completed_episode: int):
    avg_duration = state.get_average_phase_duration()  # from episode_timings
    remaining = 100 - completed_episode
    eta_hours = (avg_duration * remaining) / 3600
    logger.info("ETA for {} remaining episodes: {:.1f}h", remaining, eta_hours)
```

## Orchestrator CLI Interface

`main.py` must support these args:

```
python main.py                    # run full pipeline from current state
python main.py --episode 5        # re-run episode 5 from scratch
python main.py --from-phase IMAGE # resume all episodes from IMAGE phase
python main.py --dry-run          # validate config + health checks, no execution
```
