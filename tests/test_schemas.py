"""tests/test_schemas.py — Validate Pydantic schema models."""

import pytest
from pydantic import ValidationError

from models.schemas import (
    CharacterPatch,
    CharacterSnapshot,
    ChapterMeta,
    ExtractionResult,
    NameEntry,
)


class TestChapterMeta:
    def test_basic(self):
        ch = ChapterMeta(chapter_num=1, title="Chương 1", url="http://x.com/1")
        assert ch.status == "PENDING"
        assert ch.content is None

    def test_with_error(self):
        ch = ChapterMeta(
            chapter_num=2, title="Ch2", url="http://x.com/2",
            status="ERROR", error_msg="Timeout"
        )
        assert ch.status == "ERROR"
        assert ch.error_msg == "Timeout"


class TestCharacterSnapshot:
    def test_defaults(self):
        snap = CharacterSnapshot(chapter_start=1)
        assert snap.is_active is True
        assert snap.extraction_version == 1
        assert snap.physical_description is None
        assert 1 <= snap.visual_importance <= 10

    def test_visual_importance_bounds(self):
        with pytest.raises(ValidationError):
            CharacterSnapshot(chapter_start=1, visual_importance=0)
        with pytest.raises(ValidationError):
            CharacterSnapshot(chapter_start=1, visual_importance=11)


class TestCharacterPatch:
    def test_all_none(self):
        patch = CharacterPatch(character_id="hero_001")
        assert patch.level is None
        assert patch.outfit is None
        assert patch.is_active is None

    def test_partial_patch(self):
        patch = CharacterPatch(character_id="hero_001", level="Trúc Cơ", outfit="Trường bào")
        assert patch.level == "Trúc Cơ"
        assert patch.weapon is None

    def test_visual_importance_bounds(self):
        with pytest.raises(ValidationError):
            CharacterPatch(character_id="hero_001", visual_importance=0)
        with pytest.raises(ValidationError):
            CharacterPatch(character_id="hero_001", visual_importance=11)


class TestExtractionResult:
    def test_empty(self):
        result = ExtractionResult(batch_chapter_start=1, batch_chapter_end=5)
        assert result.new_characters == []
        assert result.updated_characters == []

    def test_with_updated(self):
        patch = CharacterPatch(character_id="hero_001", level="Trúc Cơ")
        result = ExtractionResult(
            batch_chapter_start=11,
            batch_chapter_end=15,
            updated_characters=[patch],
        )
        assert len(result.updated_characters) == 1
        assert result.updated_characters[0].level == "Trúc Cơ"


class TestNameEntry:
    def test_basic(self):
        entry = NameEntry(name="Lâm Phong", aliases=["Lâm sư huynh"])
        assert entry.name == "Lâm Phong"
        assert "Lâm sư huynh" in entry.aliases

    def test_no_aliases(self):
        entry = NameEntry(name="Vân Lam")
        assert entry.aliases == []
