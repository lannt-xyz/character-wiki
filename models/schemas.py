from typing import Optional

from pydantic import BaseModel, Field


# ---------------------------------------------------------------------------
# Crawler schemas
# ---------------------------------------------------------------------------


class ChapterMeta(BaseModel):
    chapter_num: int
    title: str
    url: str
    content: Optional[str] = None
    status: str = "PENDING"  # PENDING | CRAWLED | ERROR
    error_msg: Optional[str] = None


# ---------------------------------------------------------------------------
# Wiki / Character schemas
# ---------------------------------------------------------------------------


class CharacterSnapshot(BaseModel):
    """Append-only snapshot of a character's temporal state at a chapter start.

    chapter_end is NOT stored here — it is derived as the chapter_start of the
    next snapshot for the same character (truly append-only).
    """

    chapter_start: int
    extraction_version: int = 1
    is_active: bool = True

    # Persistent fields — inherited from latest snapshot when LLM returns None
    level: Optional[str] = None
    outfit: Optional[str] = None
    weapon: Optional[str] = None
    vfx_vibes: Optional[str] = None

    # Transient field — reset to NULL each batch if not mentioned
    physical_description: Optional[str] = None

    # Priority for Video Pipeline Cref ordering
    visual_importance: int = Field(default=5, ge=1, le=10)


class CharacterPatch(BaseModel):
    """Delta update from LLM — only changed fields are set.

    All fields Optional. physical_description=None = transient ended (no injury etc.),
    not "no change".
    """

    character_id: str
    level: Optional[str] = None
    outfit: Optional[str] = None
    weapon: Optional[str] = None
    vfx_vibes: Optional[str] = None
    physical_description: Optional[str] = None
    visual_importance: Optional[int] = Field(default=None, ge=1, le=10)
    is_active: Optional[bool] = None
    aliases: Optional[list[str]] = None


class Character(BaseModel):
    """Static identity of a character — does not change across chapters."""

    character_id: str
    name: str
    name_normalized: str
    aliases: list[str] = Field(default_factory=list)
    traits: list[str] = Field(default_factory=list)
    relations: list[dict] = Field(default_factory=list)
    # Fixed physical anchor (scars, birthmarks, build) — sent in every Pass 2 context
    visual_anchor: Optional[str] = None


class ExtractionResult(BaseModel):
    """Output from wiki/extractor.py for one batch."""

    batch_chapter_start: int
    batch_chapter_end: int
    new_characters: list[dict] = Field(
        default_factory=list,
        description="Each item: {character: Character-dict, snapshot: CharacterSnapshot-dict}",
    )
    updated_characters: list[CharacterPatch] = Field(default_factory=list)


class WikiBatchState(BaseModel):
    """State of a single extraction batch in DB."""

    batch_id: int  # = chapter_start, deterministic
    chapter_start: int
    chapter_end: int
    status: str = "PENDING"  # PENDING | CRAWLED | EXTRACTED | MERGED
    extraction_version: int = 1


# ---------------------------------------------------------------------------
# Pass-1 helper (name scan output)
# ---------------------------------------------------------------------------


class NameEntry(BaseModel):
    name: str
    aliases: list[str] = Field(default_factory=list)


# ---------------------------------------------------------------------------
# Remaster schemas
# ---------------------------------------------------------------------------


class RemasterBatch(BaseModel):
    batch_id: int
    chapter_start: int
    chapter_end: int
    remaster_version: int = 1
    status: str = "PENDING"
    extracted_at: Optional[str] = None
    merged_at: Optional[str] = None


class ArtifactSeedSnapshot(BaseModel):
    chapter_start: int
    owner_id: Optional[str] = None
    normal_state: Optional[str] = None
    active_state: Optional[str] = None
    condition: str = "intact"


class ArtifactSeedEntry(BaseModel):
    artifact_id: str
    name: str
    rarity: Optional[str] = None
    material: Optional[str] = None
    visual_anchor: Optional[str] = None
    snapshots: list[ArtifactSeedSnapshot] = Field(default_factory=list)


class WikiSeedCharacter(BaseModel):
    character_id: str
    artifacts: list[ArtifactSeedEntry] = Field(default_factory=list)
